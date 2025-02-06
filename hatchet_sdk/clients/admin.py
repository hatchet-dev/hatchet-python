import json
from datetime import datetime
from typing import Any, Callable, TypeVar, Union, cast

import grpc
from google.protobuf import timestamp_pb2
from pydantic import BaseModel, Field

from hatchet_sdk.clients.rest.models.workflow_run import WorkflowRun
from hatchet_sdk.clients.rest.tenacity_utils import tenacity_retry
from hatchet_sdk.clients.run_event_listener import new_listener
from hatchet_sdk.clients.workflow_listener import PooledWorkflowRunListener
from hatchet_sdk.connection import new_conn
from hatchet_sdk.contracts.workflows_pb2 import (
    BulkTriggerWorkflowRequest,
    BulkTriggerWorkflowResponse,
    CreateWorkflowVersionOpts,
    PutRateLimitRequest,
    PutWorkflowRequest,
    RateLimitDuration,
    ScheduleWorkflowRequest,
    TriggerWorkflowRequest,
    TriggerWorkflowResponse,
    WorkflowVersion,
)
from hatchet_sdk.contracts.workflows_pb2_grpc import WorkflowServiceStub
from hatchet_sdk.utils.serialization import flatten
from hatchet_sdk.utils.tracing import (
    create_carrier,
    create_tracer,
    inject_carrier_into_metadata,
    parse_carrier_from_metadata,
)
from hatchet_sdk.utils.types import JSONSerializableDict
from hatchet_sdk.workflow_run import RunRef, WorkflowRunRef

from ..loader import ClientConfig
from ..metadata import get_metadata


def new_admin(config: ClientConfig) -> "AdminClient":
    return AdminClient(config)


class ScheduleTriggerWorkflowOptions(BaseModel):
    parent_id: str | None = None
    parent_step_run_id: str | None = None
    child_index: int | None = None
    child_key: str | None = None
    namespace: str | None = None


class ChildTriggerWorkflowOptions(BaseModel):
    additional_metadata: JSONSerializableDict = Field(default_factory=dict)
    sticky: bool | None = None


class ChildWorkflowRunDict(BaseModel):
    workflow_name: str
    input: JSONSerializableDict
    options: ChildTriggerWorkflowOptions
    key: str | None = None


class TriggerWorkflowOptions(ScheduleTriggerWorkflowOptions):
    additional_metadata: JSONSerializableDict = Field(default_factory=dict)
    desired_worker_id: str | None = None
    namespace: str | None = None


class WorkflowRunDict(BaseModel):
    workflow_name: str
    input: JSONSerializableDict
    options: TriggerWorkflowOptions


class DedupeViolationErr(Exception):
    """Raised by the Hatchet library to indicate that a workflow has already been run with this deduplication value."""

    pass


class AdminClientBase:
    pooled_workflow_listener: PooledWorkflowRunListener | None = None

    def _prepare_workflow_request(
        self, workflow_name: str, input: dict[str, Any], options: TriggerWorkflowOptions
    ) -> TriggerWorkflowRequest:
        try:
            payload_data = json.dumps(input)
            _options = options.model_dump()

            _options.pop("namespace")

            try:
                _options = {
                    **_options,
                    "additional_metadata": json.dumps(
                        options.additional_metadata
                    ).encode("utf-8"),
                }
            except json.JSONDecodeError as e:
                raise ValueError(f"Error encoding payload: {e}")

            return TriggerWorkflowRequest(
                name=workflow_name, input=payload_data, **_options
            )
        except json.JSONDecodeError as e:
            raise ValueError(f"Error encoding payload: {e}")

    def _prepare_put_workflow_request(
        self,
        name: str,
        workflow: CreateWorkflowVersionOpts,
        overrides: CreateWorkflowVersionOpts | None = None,
    ) -> PutWorkflowRequest:
        try:
            opts: CreateWorkflowVersionOpts

            if isinstance(workflow, CreateWorkflowVersionOpts):
                opts = workflow
            else:
                opts = workflow.get_create_opts(self.client.config.namespace)

            if overrides is not None:
                opts.MergeFrom(overrides)

            opts.name = name

            return PutWorkflowRequest(
                opts=opts,
            )
        except grpc.RpcError as e:
            raise ValueError(f"Could not put workflow: {e}")

    def _prepare_schedule_workflow_request(
        self,
        name: str,
        schedules: list[Union[datetime, timestamp_pb2.Timestamp]],
        input: JSONSerializableDict = {},
        options: ScheduleTriggerWorkflowOptions = ScheduleTriggerWorkflowOptions(),
    ) -> ScheduleWorkflowRequest:
        timestamp_schedules = []
        for schedule in schedules:
            if isinstance(schedule, datetime):
                t = schedule.timestamp()
                seconds = int(t)
                nanos = int(t % 1 * 1e9)
                timestamp = timestamp_pb2.Timestamp(seconds=seconds, nanos=nanos)
                timestamp_schedules.append(timestamp)
            elif isinstance(schedule, timestamp_pb2.Timestamp):
                timestamp_schedules.append(schedule)
            else:
                raise ValueError(
                    "Invalid schedule type. Must be datetime or timestamp_pb2.Timestamp."
                )

        return ScheduleWorkflowRequest(
            name=name,
            schedules=timestamp_schedules,
            input=json.dumps(input),
            **options.model_dump(),
        )


T = TypeVar("T")


class AdminClientAioImpl(AdminClientBase):
    def __init__(self, config: ClientConfig):
        aio_conn = new_conn(config, True)
        self.config = config
        self.aio_client = WorkflowServiceStub(aio_conn)  # type: ignore[no-untyped-call]
        self.token = config.token
        self.listener_client = new_listener(config)
        self.namespace = config.namespace
        self.otel_tracer = create_tracer(config=config)

    async def run(
        self,
        function: Union[str, Callable[[Any], T]],
        input: JSONSerializableDict,
        options: TriggerWorkflowOptions = TriggerWorkflowOptions(),
    ) -> "RunRef[T]":
        workflow_name = cast(
            str,
            (
                function
                if isinstance(function, str)
                else getattr(function, "function_name")
            ),
        )

        wrr = await self.run_workflow(workflow_name, input, options)

        return RunRef[T](
            wrr.workflow_run_id, wrr.workflow_listener, wrr.workflow_run_event_listener
        )

    @tenacity_retry
    async def run_workflow(
        self,
        workflow_name: str,
        input: JSONSerializableDict,
        options: TriggerWorkflowOptions = TriggerWorkflowOptions(),
    ) -> WorkflowRunRef:
        ctx = parse_carrier_from_metadata(options.additional_metadata)

        with self.otel_tracer.start_as_current_span(
            f"hatchet.async_run_workflow.{workflow_name}", context=ctx
        ) as span:
            carrier = create_carrier()

            try:
                if not self.pooled_workflow_listener:
                    self.pooled_workflow_listener = PooledWorkflowRunListener(
                        self.config
                    )

                namespace = options.namespace or self.namespace

                if namespace != "" and not workflow_name.startswith(self.namespace):
                    workflow_name = f"{namespace}{workflow_name}"

                options.additional_metadata = inject_carrier_into_metadata(
                    options.additional_metadata, carrier
                )

                span.set_attributes(
                    flatten(options.additional_metadata, parent_key="", separator=".")
                )

                request = self._prepare_workflow_request(workflow_name, input, options)

                span.add_event(
                    "Triggering workflow", attributes={"workflow_name": workflow_name}
                )

                resp: TriggerWorkflowResponse = await self.aio_client.TriggerWorkflow(
                    request,
                    metadata=get_metadata(self.token),
                )

                span.add_event(
                    "Received workflow response",
                    attributes={"workflow_name": workflow_name},
                )

                return WorkflowRunRef(
                    workflow_run_id=resp.workflow_run_id,
                    workflow_listener=self.pooled_workflow_listener,
                    workflow_run_event_listener=self.listener_client,
                )
            except (grpc.RpcError, grpc.aio.AioRpcError) as e:
                if e.code() == grpc.StatusCode.ALREADY_EXISTS:
                    raise DedupeViolationErr(e.details())

                raise e

    @tenacity_retry
    async def run_workflows(
        self,
        workflows: list[WorkflowRunDict],
        options: TriggerWorkflowOptions = TriggerWorkflowOptions(),
    ) -> list[WorkflowRunRef]:
        if len(workflows) == 0:
            raise ValueError("No workflows to run")

        if not self.pooled_workflow_listener:
            self.pooled_workflow_listener = PooledWorkflowRunListener(self.config)

        namespace = options.namespace or self.namespace

        workflow_run_requests: list[TriggerWorkflowRequest] = []

        for workflow in workflows:
            workflow_name = workflow.workflow_name
            input_data = workflow.input
            options = workflow.options

            if namespace != "" and not workflow_name.startswith(self.namespace):
                workflow_name = f"{namespace}{workflow_name}"

            # Prepare and trigger workflow for each workflow name and input
            request = self._prepare_workflow_request(workflow_name, input_data, options)
            workflow_run_requests.append(request)

        resp: BulkTriggerWorkflowResponse = await self.aio_client.BulkTriggerWorkflow(
            BulkTriggerWorkflowRequest(workflows=workflow_run_requests),
            metadata=get_metadata(self.token),
        )

        return [
            WorkflowRunRef(
                workflow_run_id=workflow_run_id,
                workflow_listener=self.pooled_workflow_listener,
                workflow_run_event_listener=self.listener_client,
            )
            for workflow_run_id in resp.workflow_run_ids
        ]

    @tenacity_retry
    async def put_workflow(
        self,
        name: str,
        workflow: CreateWorkflowVersionOpts,
        overrides: CreateWorkflowVersionOpts | None = None,
    ) -> WorkflowVersion:
        opts = self._prepare_put_workflow_request(name, workflow, overrides)

        return cast(
            WorkflowVersion,
            await self.aio_client.PutWorkflow(
                opts,
                metadata=get_metadata(self.token),
            ),
        )

    @tenacity_retry
    async def put_rate_limit(
        self,
        key: str,
        limit: int,
        duration: RateLimitDuration = RateLimitDuration.SECOND,
    ) -> None:
        await self.aio_client.PutRateLimit(
            PutRateLimitRequest(
                key=key,
                limit=limit,
                duration=duration,
            ),
            metadata=get_metadata(self.token),
        )

    @tenacity_retry
    async def schedule_workflow(
        self,
        name: str,
        schedules: list[Union[datetime, timestamp_pb2.Timestamp]],
        input: JSONSerializableDict = {},
        options: ScheduleTriggerWorkflowOptions = ScheduleTriggerWorkflowOptions(),
    ) -> WorkflowVersion:
        try:
            namespace = options.namespace or self.namespace

            if namespace != "" and not name.startswith(self.namespace):
                name = f"{namespace}{name}"

            request = self._prepare_schedule_workflow_request(
                name, schedules, input, options
            )

            return cast(
                WorkflowVersion,
                await self.aio_client.ScheduleWorkflow(
                    request,
                    metadata=get_metadata(self.token),
                ),
            )
        except (grpc.aio.AioRpcError, grpc.RpcError) as e:
            if e.code() == grpc.StatusCode.ALREADY_EXISTS:
                raise DedupeViolationErr(e.details())

            raise e


class AdminClient(AdminClientBase):
    def __init__(self, config: ClientConfig):
        conn = new_conn(config, False)
        self.config = config
        self.client = WorkflowServiceStub(conn)  # type: ignore[no-untyped-call]
        self.aio = AdminClientAioImpl(config)
        self.token = config.token
        self.listener_client = new_listener(config)
        self.namespace = config.namespace
        self.otel_tracer = create_tracer(config=config)

    @tenacity_retry
    def put_workflow(
        self,
        name: str,
        workflow: CreateWorkflowVersionOpts,
        overrides: CreateWorkflowVersionOpts | None = None,
    ) -> WorkflowVersion:
        opts = self._prepare_put_workflow_request(name, workflow, overrides)

        resp: WorkflowVersion = self.client.PutWorkflow(
            opts,
            metadata=get_metadata(self.token),
        )

        return resp

    @tenacity_retry
    def put_rate_limit(
        self,
        key: str,
        limit: int,
        duration: Union[RateLimitDuration, str] = RateLimitDuration.SECOND,
    ) -> None:
        self.client.PutRateLimit(
            PutRateLimitRequest(
                key=key,
                limit=limit,
                duration=duration,
            ),
            metadata=get_metadata(self.token),
        )

    @tenacity_retry
    def schedule_workflow(
        self,
        name: str,
        schedules: list[Union[datetime, timestamp_pb2.Timestamp]],
        input: JSONSerializableDict = {},
        options: ScheduleTriggerWorkflowOptions = ScheduleTriggerWorkflowOptions(),
    ) -> WorkflowVersion:
        try:
            namespace = options.namespace or self.namespace

            if namespace != "" and not name.startswith(self.namespace):
                name = f"{namespace}{name}"

            request = self._prepare_schedule_workflow_request(
                name, schedules, input, options
            )

            return cast(
                WorkflowVersion,
                self.client.ScheduleWorkflow(
                    request,
                    metadata=get_metadata(self.token),
                ),
            )
        except (grpc.RpcError, grpc.aio.AioRpcError) as e:
            if e.code() == grpc.StatusCode.ALREADY_EXISTS:
                raise DedupeViolationErr(e.details())

            raise e

    ## TODO: `options` is treated as a dict (wrong type hint)
    ## TODO: `any` type hint should come from `typing`
    @tenacity_retry
    def run_workflow(
        self,
        workflow_name: str,
        input: JSONSerializableDict,
        options: TriggerWorkflowOptions = TriggerWorkflowOptions(),
    ) -> WorkflowRunRef:
        ctx = parse_carrier_from_metadata(options.additional_metadata)

        with self.otel_tracer.start_as_current_span(
            f"hatchet.run_workflow.{workflow_name}", context=ctx
        ) as span:
            carrier = create_carrier()

            try:
                if not self.pooled_workflow_listener:
                    self.pooled_workflow_listener = PooledWorkflowRunListener(
                        self.config
                    )

                namespace = options.namespace or self.namespace

                options.additional_metadata = inject_carrier_into_metadata(
                    options.additional_metadata, carrier
                )

                span.set_attributes(
                    flatten(options.additional_metadata, parent_key="", separator=".")
                )

                if namespace != "" and not workflow_name.startswith(self.namespace):
                    workflow_name = f"{namespace}{workflow_name}"

                request = self._prepare_workflow_request(workflow_name, input, options)

                span.add_event(
                    "Triggering workflow", attributes={"workflow_name": workflow_name}
                )

                resp: TriggerWorkflowResponse = self.client.TriggerWorkflow(
                    request,
                    metadata=get_metadata(self.token),
                )

                span.add_event(
                    "Received workflow response",
                    attributes={"workflow_name": workflow_name},
                )

                return WorkflowRunRef(
                    workflow_run_id=resp.workflow_run_id,
                    workflow_listener=self.pooled_workflow_listener,
                    workflow_run_event_listener=self.listener_client,
                )
            except (grpc.RpcError, grpc.aio.AioRpcError) as e:
                if e.code() == grpc.StatusCode.ALREADY_EXISTS:
                    raise DedupeViolationErr(e.details())

                raise e

    @tenacity_retry
    def run_workflows(
        self,
        workflows: list[WorkflowRunDict],
        options: TriggerWorkflowOptions = TriggerWorkflowOptions(),
    ) -> list[WorkflowRunRef]:
        workflow_run_requests: list[TriggerWorkflowRequest] = []
        if not self.pooled_workflow_listener:
            self.pooled_workflow_listener = PooledWorkflowRunListener(self.config)

        for workflow in workflows:
            workflow_name = workflow.workflow_name
            input_data = workflow.input
            options = workflow.options

            namespace = options.namespace or self.namespace

        if namespace != "" and not workflow_name.startswith(self.namespace):
            workflow_name = f"{namespace}{workflow_name}"

        # Prepare and trigger workflow for each workflow name and input
        request = self._prepare_workflow_request(workflow_name, input_data, options)

        workflow_run_requests.append(request)

        bulk_request = BulkTriggerWorkflowRequest(workflows=workflow_run_requests)

        resp: BulkTriggerWorkflowResponse = self.client.BulkTriggerWorkflow(
            bulk_request,
            metadata=get_metadata(self.token),
        )

        return [
            WorkflowRunRef(
                workflow_run_id=workflow_run_id,
                workflow_listener=self.pooled_workflow_listener,
                workflow_run_event_listener=self.listener_client,
            )
            for workflow_run_id in resp.workflow_run_ids
        ]

    def run(
        self,
        function: Union[str, Callable[[Any], T]],
        input: JSONSerializableDict,
        options: TriggerWorkflowOptions = TriggerWorkflowOptions(),
    ) -> "RunRef[T]":
        workflow_name = cast(
            str,
            (
                function
                if isinstance(function, str)
                else getattr(function, "function_name")
            ),
        )

        wrr = self.run_workflow(workflow_name, input, options)

        return RunRef[T](
            wrr.workflow_run_id, wrr.workflow_listener, wrr.workflow_run_event_listener
        )

    def get_workflow_run(self, workflow_run_id: str) -> WorkflowRunRef:
        try:
            if not self.pooled_workflow_listener:
                self.pooled_workflow_listener = PooledWorkflowRunListener(self.config)

            return WorkflowRunRef(
                workflow_run_id=workflow_run_id,
                workflow_listener=self.pooled_workflow_listener,
                workflow_run_event_listener=self.listener_client,
            )
        except grpc.RpcError as e:
            raise ValueError(f"Could not get workflow run: {e}")
