import asyncio
import json
from datetime import datetime
from typing import Any, Union, cast

import grpc
from google.protobuf import timestamp_pb2
from pydantic import BaseModel, Field

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
from hatchet_sdk.loader import ClientConfig
from hatchet_sdk.metadata import get_metadata
from hatchet_sdk.utils.serialization import flatten
from hatchet_sdk.utils.tracing import (
    create_carrier,
    create_tracer,
    inject_carrier_into_metadata,
    parse_carrier_from_metadata,
)
from hatchet_sdk.utils.types import JSONSerializableDict
from hatchet_sdk.workflow_run import WorkflowRunRef


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


class AdminClient:
    pooled_workflow_listener: PooledWorkflowRunListener | None = None

    def __init__(self, config: ClientConfig):
        conn = new_conn(config, False)
        self.config = config
        self.client = WorkflowServiceStub(conn)  # type: ignore[no-untyped-call]
        self.token = config.token
        self.listener_client = new_listener(config)
        self.namespace = config.namespace
        self.otel_tracer = create_tracer(config=config)

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

    @tenacity_retry
    async def arun_workflow(
        self,
        workflow_name: str,
        input: JSONSerializableDict,
        options: TriggerWorkflowOptions = TriggerWorkflowOptions(),
    ) -> WorkflowRunRef:
        ## IMPORTANT: The `pooled_workflow_listener` must be created 1) lazily, and not at `init` time, and 2) on the
        ## main thread. If 1) is not followed, you'll get an error about something being attached to the wrong event
        ## loop. If 2) is not followed, you'll get an error about the event loop not being set up.
        if not self.pooled_workflow_listener:
            self.pooled_workflow_listener = PooledWorkflowRunListener(self.config)

        return await asyncio.to_thread(self.run_workflow, workflow_name, input, options)

    @tenacity_retry
    async def arun_workflows(
        self,
        workflows: list[WorkflowRunDict],
        options: TriggerWorkflowOptions = TriggerWorkflowOptions(),
    ) -> list[WorkflowRunRef]:
        ## IMPORTANT: The `pooled_workflow_listener` must be created 1) lazily, and not at `init` time, and 2) on the
        ## main thread. If 1) is not followed, you'll get an error about something being attached to the wrong event
        ## loop. If 2) is not followed, you'll get an error about the event loop not being set up.
        if not self.pooled_workflow_listener:
            self.pooled_workflow_listener = PooledWorkflowRunListener(self.config)

        return await asyncio.to_thread(self.run_workflows, workflows, options)

    @tenacity_retry
    async def aput_workflow(
        self,
        name: str,
        workflow: CreateWorkflowVersionOpts,
        overrides: CreateWorkflowVersionOpts | None = None,
    ) -> WorkflowVersion:
        ## IMPORTANT: The `pooled_workflow_listener` must be created 1) lazily, and not at `init` time, and 2) on the
        ## main thread. If 1) is not followed, you'll get an error about something being attached to the wrong event
        ## loop. If 2) is not followed, you'll get an error about the event loop not being set up.
        if not self.pooled_workflow_listener:
            self.pooled_workflow_listener = PooledWorkflowRunListener(self.config)

        return await asyncio.to_thread(self.put_workflow, name, workflow, overrides)

    @tenacity_retry
    async def aput_rate_limit(
        self,
        key: str,
        limit: int,
        duration: RateLimitDuration = RateLimitDuration.SECOND,
    ) -> None:
        ## IMPORTANT: The `pooled_workflow_listener` must be created 1) lazily, and not at `init` time, and 2) on the
        ## main thread. If 1) is not followed, you'll get an error about something being attached to the wrong event
        ## loop. If 2) is not followed, you'll get an error about the event loop not being set up.
        if not self.pooled_workflow_listener:
            self.pooled_workflow_listener = PooledWorkflowRunListener(self.config)

        return await asyncio.to_thread(self.put_rate_limit, key, limit, duration)

    @tenacity_retry
    async def aschedule_workflow(
        self,
        name: str,
        schedules: list[Union[datetime, timestamp_pb2.Timestamp]],
        input: JSONSerializableDict = {},
        options: ScheduleTriggerWorkflowOptions = ScheduleTriggerWorkflowOptions(),
    ) -> WorkflowVersion:
        ## IMPORTANT: The `pooled_workflow_listener` must be created 1) lazily, and not at `init` time, and 2) on the
        ## main thread. If 1) is not followed, you'll get an error about something being attached to the wrong event
        ## loop. If 2) is not followed, you'll get an error about the event loop not being set up.
        if not self.pooled_workflow_listener:
            self.pooled_workflow_listener = PooledWorkflowRunListener(self.config)

        return await asyncio.to_thread(
            self.schedule_workflow, name, schedules, input, options
        )

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
