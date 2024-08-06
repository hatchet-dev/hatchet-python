# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.2.7](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.4 
# Pydantic Version: 2.8.2 
from datetime import datetime
from enum import IntEnum
from google.protobuf.message import Message  # type: ignore
from pydantic import BaseModel
from pydantic import Field
import typing

class ActionType(IntEnum):
    START_STEP_RUN = 0
    CANCEL_STEP_RUN = 1
    START_GET_GROUP_KEY = 2


class GroupKeyActionEventType(IntEnum):
    GROUP_KEY_EVENT_TYPE_UNKNOWN = 0
    GROUP_KEY_EVENT_TYPE_STARTED = 1
    GROUP_KEY_EVENT_TYPE_COMPLETED = 2
    GROUP_KEY_EVENT_TYPE_FAILED = 3


class StepActionEventType(IntEnum):
    STEP_EVENT_TYPE_UNKNOWN = 0
    STEP_EVENT_TYPE_STARTED = 1
    STEP_EVENT_TYPE_COMPLETED = 2
    STEP_EVENT_TYPE_FAILED = 3


class ResourceType(IntEnum):
    RESOURCE_TYPE_UNKNOWN = 0
    RESOURCE_TYPE_STEP_RUN = 1
    RESOURCE_TYPE_WORKFLOW_RUN = 2


class ResourceEventType(IntEnum):
    RESOURCE_EVENT_TYPE_UNKNOWN = 0
    RESOURCE_EVENT_TYPE_STARTED = 1
    RESOURCE_EVENT_TYPE_COMPLETED = 2
    RESOURCE_EVENT_TYPE_FAILED = 3
    RESOURCE_EVENT_TYPE_CANCELLED = 4
    RESOURCE_EVENT_TYPE_TIMED_OUT = 5
    RESOURCE_EVENT_TYPE_STREAM = 6


class WorkflowRunEventType(IntEnum):
    WORKFLOW_RUN_EVENT_TYPE_FINISHED = 0

class WorkerLabels(BaseModel):
# value of the label
    strValue: typing.Optional[str] = Field(default="")
    intValue: typing.Optional[int] = Field(default=0)

class WorkerRegisterRequest(BaseModel):
# the name of the worker
    workerName: str = Field(default="")
# a list of actions that this worker can run
    actions: typing.List[str] = Field(default_factory=list)
# (optional) the services for this worker
    services: typing.List[str] = Field(default_factory=list)
# (optional) the max number of runs this worker can handle
    maxRuns: typing.Optional[int] = Field(default=0)
# (optional) worker labels (i.e. state or other metadata)
    labels: typing.Dict[str, WorkerLabels] = Field(default_factory=dict)

class WorkerRegisterResponse(BaseModel):
# the tenant id
    tenantId: str = Field(default="")
# the id of the worker
    workerId: str = Field(default="")
# the name of the worker
    workerName: str = Field(default="")

class UpsertWorkerLabelsRequest(BaseModel):
# the name of the worker
    workerId: str = Field(default="")
# (optional) the worker labels
    labels: typing.Dict[str, WorkerLabels] = Field(default_factory=dict)

class UpsertWorkerLabelsResponse(BaseModel):
# the tenant id
    tenantId: str = Field(default="")
# the id of the worker
    workerId: str = Field(default="")

class AssignedAction(BaseModel):
# the tenant id
    tenantId: str = Field(default="")
# the workflow run id (optional)
    workflowRunId: str = Field(default="")
# the get group key run id (optional)
    getGroupKeyRunId: str = Field(default="")
# the job id
    jobId: str = Field(default="")
# the job name
    jobName: str = Field(default="")
# the job run id
    jobRunId: str = Field(default="")
# the step id
    stepId: str = Field(default="")
# the step run id
    stepRunId: str = Field(default="")
# the action id
    actionId: str = Field(default="")
# the action type
    actionType: ActionType = Field(default=0)
# the action payload
    actionPayload: str = Field(default="")
# the step name
    stepName: str = Field(default="")
# the count number of the retry attempt
    retryCount: int = Field(default=0)
# (optional) additional metadata set on the workflow
    additional_metadata: typing.Optional[str] = Field(default="")
# (optional) the child workflow index (if this is a child workflow)
    child_workflow_index: typing.Optional[int] = Field(default=0)
# (optional) the child workflow key (if this is a child workflow)
    child_workflow_key: typing.Optional[str] = Field(default="")
# (optional) the parent workflow run id (if this is a child workflow)
    parent_workflow_run_id: typing.Optional[str] = Field(default="")

class WorkerListenRequest(BaseModel):
# the id of the worker
    workerId: str = Field(default="")

class WorkerUnsubscribeRequest(BaseModel):
# the id of the worker
    workerId: str = Field(default="")

class WorkerUnsubscribeResponse(BaseModel):
# the tenant id to unsubscribe from
    tenantId: str = Field(default="")
# the id of the worker
    workerId: str = Field(default="")

class GroupKeyActionEvent(BaseModel):
# the id of the worker
    workerId: str = Field(default="")
# the id of the job
    workflowRunId: str = Field(default="")
    getGroupKeyRunId: str = Field(default="")
# the action id
    actionId: str = Field(default="")
    eventTimestamp: datetime = Field(default_factory=datetime.now)
# the step event type
    eventType: GroupKeyActionEventType = Field(default=0)
# the event payload
    eventPayload: str = Field(default="")

class StepActionEvent(BaseModel):
# the id of the worker
    workerId: str = Field(default="")
# the id of the job
    jobId: str = Field(default="")
# the job run id
    jobRunId: str = Field(default="")
# the id of the step
    stepId: str = Field(default="")
# the step run id
    stepRunId: str = Field(default="")
# the action id
    actionId: str = Field(default="")
    eventTimestamp: datetime = Field(default_factory=datetime.now)
# the step event type
    eventType: StepActionEventType = Field(default=0)
# the event payload
    eventPayload: str = Field(default="")

class ActionEventResponse(BaseModel):
# the tenant id
    tenantId: str = Field(default="")
# the id of the worker
    workerId: str = Field(default="")

class SubscribeToWorkflowEventsRequest(BaseModel):
# the id of the workflow run
    workflowRunId: typing.Optional[str] = Field(default="")
# the key of the additional meta field to subscribe to
    additionalMetaKey: typing.Optional[str] = Field(default="")
# the value of the additional meta field to subscribe to
    additionalMetaValue: typing.Optional[str] = Field(default="")

class SubscribeToWorkflowRunsRequest(BaseModel):
# the id of the workflow run
    workflowRunId: str = Field(default="")

class WorkflowEvent(BaseModel):
# the id of the workflow run
    workflowRunId: str = Field(default="")
    resourceType: ResourceType = Field(default=0)
    eventType: ResourceEventType = Field(default=0)
    resourceId: str = Field(default="")
    eventTimestamp: datetime = Field(default_factory=datetime.now)
# the event payload
    eventPayload: str = Field(default="")
# whether this is the last event for the workflow run - server
# will hang up the connection but clients might want to case
    hangup: bool = Field(default=False)
# (optional) the max number of retries this step can handle
    stepRetries: typing.Optional[int] = Field(default=0)
# (optional) the retry count of this step
    retryCount: typing.Optional[int] = Field(default=0)

class StepRunResult(BaseModel):
    stepRunId: str = Field(default="")
    stepReadableId: str = Field(default="")
    jobRunId: str = Field(default="")
    error: typing.Optional[str] = Field(default="")
    output: typing.Optional[str] = Field(default="")

class WorkflowRunEvent(BaseModel):
# the id of the workflow run
    workflowRunId: str = Field(default="")
    eventType: WorkflowRunEventType = Field(default=0)
    eventTimestamp: datetime = Field(default_factory=datetime.now)
    results: typing.List[StepRunResult] = Field(default_factory=list)

class OverridesData(BaseModel):
# the step run id
    stepRunId: str = Field(default="")
# the path of the data to set
    path: str = Field(default="")
# the value to set
    value: str = Field(default="")
# the filename of the caller
    callerFilename: str = Field(default="")

class OverridesDataResponse(BaseModel):    pass

class HeartbeatRequest(BaseModel):
# the id of the worker
    workerId: str = Field(default="")
# heartbeatAt is the time the worker sent the heartbeat
    heartbeatAt: datetime = Field(default_factory=datetime.now)

class HeartbeatResponse(BaseModel):    pass

class RefreshTimeoutRequest(BaseModel):
# the id of the step run to release
    stepRunId: str = Field(default="")
    incrementTimeoutBy: str = Field(default="")

class RefreshTimeoutResponse(BaseModel):
    timeoutAt: datetime = Field(default_factory=datetime.now)

class ReleaseSlotRequest(BaseModel):
# the id of the step run to release
    stepRunId: str = Field(default="")

class ReleaseSlotResponse(BaseModel):    pass
