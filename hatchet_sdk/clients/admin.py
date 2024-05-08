import json
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, TypedDict, Union

import grpc
from google.protobuf import timestamp_pb2

from ..loader import ClientConfig
from ..metadata import get_metadata
from ..workflow import WorkflowMeta
from ..workflows_pb2 import (
    CreateWorkflowVersionOpts,
    PutRateLimitRequest,
    PutWorkflowRequest,
    RateLimitDuration,
    ScheduleWorkflowRequest,
    TriggerWorkflowRequest,
    TriggerWorkflowResponse,
    WorkflowVersion,
)
from ..workflows_pb2_grpc import WorkflowServiceStub


def new_admin(conn, config: ClientConfig):
    return AdminClientImpl(
        client=WorkflowServiceStub(conn),
        token=config.token,
    )


class ScheduleTriggerWorkflowOptions(TypedDict):
    parent_id: Optional[str]
    parent_step_run_id: Optional[str]
    child_index: Optional[int]
    child_key: Optional[str]


class TriggerWorkflowOptions(ScheduleTriggerWorkflowOptions):
    additional_metadata: Dict[str, str] | None = None


class AdminClientImpl:
    def __init__(self, client: WorkflowServiceStub, token):
        self.client = client
        self.token = token

    def put_workflow(
        self,
        name: str,
        workflow: CreateWorkflowVersionOpts | WorkflowMeta,
        overrides: CreateWorkflowVersionOpts | None = None,
    ) -> WorkflowVersion:
        try:
            opts: CreateWorkflowVersionOpts

            if isinstance(workflow, CreateWorkflowVersionOpts):
                opts = workflow
            else:
                opts = workflow.get_create_opts()

            if overrides is not None:
                opts.MergeFrom(overrides)

            opts.name = name

            return self.client.PutWorkflow(
                PutWorkflowRequest(
                    opts=opts,
                ),
                metadata=get_metadata(self.token),
            )
        except grpc.RpcError as e:
            raise ValueError(f"Could not put workflow: {e}")

    def put_rate_limit(
        self,
        key: str,
        limit: int,
        duration: RateLimitDuration = RateLimitDuration.SECOND,
    ):
        try:
            self.client.PutRateLimit(
                PutRateLimitRequest(
                    key=key,
                    limit=limit,
                    duration=duration,
                ),
                metadata=get_metadata(self.token),
            )
        except grpc.RpcError as e:
            raise ValueError(f"Could not put rate limit: {e}")

    def schedule_workflow(
        self,
        name: str,
        schedules: List[Union[datetime, timestamp_pb2.Timestamp]],
        input={},
        options: ScheduleTriggerWorkflowOptions = None,
    ):
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

        try:
            self.client.ScheduleWorkflow(
                ScheduleWorkflowRequest(
                    name=name,
                    schedules=timestamp_schedules,
                    input=json.dumps(input),
                    **(options or {}),
                ),
                metadata=get_metadata(self.token),
            )

        except grpc.RpcError as e:
            raise ValueError(f"gRPC error: {e}")

    def run_workflow(
        self,
        workflow_name: str,
        input: any,
        options: TriggerWorkflowOptions = None,
    ):
        try:
            payload_data = json.dumps(input)

            try:
                meta = None if options is None else options["additional_metadata"]
                options["additional_metadata"] = (
                    None if meta is None else json.dumps(meta).encode("utf-8")
                )
            except json.JSONDecodeError as e:
                raise ValueError(f"Error encoding payload: {e}")

            resp: TriggerWorkflowResponse = self.client.TriggerWorkflow(
                TriggerWorkflowRequest(
                    name=workflow_name, input=payload_data, **(options or {})
                ),
                metadata=get_metadata(self.token),
            )

            return resp.workflow_run_id
        except grpc.RpcError as e:
            raise ValueError(f"gRPC error: {e}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Error encoding payload: {e}")
