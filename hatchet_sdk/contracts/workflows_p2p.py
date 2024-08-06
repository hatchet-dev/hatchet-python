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

class StickyStrategy(IntEnum):
    SOFT = 0
    HARD = 1


class WorkflowKind(IntEnum):
    FUNCTION = 0
    DURABLE = 1
    DAG = 2


class ConcurrencyLimitStrategy(IntEnum):
    CANCEL_IN_PROGRESS = 0
    DROP_NEWEST = 1
    QUEUE_NEWEST = 2
    GROUP_ROUND_ROBIN = 3


class WorkerLabelComparator(IntEnum):
    EQUAL = 0
    NOT_EQUAL = 1
    GREATER_THAN = 2
    GREATER_THAN_OR_EQUAL = 3
    LESS_THAN = 4
    LESS_THAN_OR_EQUAL = 5


class RateLimitDuration(IntEnum):
    SECOND = 0
    MINUTE = 1
    HOUR = 2
    DAY = 3
    WEEK = 4
    MONTH = 5
    YEAR = 6

class CreateStepRateLimit(BaseModel):
    key: str = Field(default="")# (required) the key for the rate limit
    units: int = Field(default=0)# (required) the number of units this step consumes

class CreateWorkflowStepOpts(BaseModel):
    """
     CreateWorkflowStepOpts represents options to create a workflow step.
    """

    readable_id: str = Field(default="")# (required) the step name
    action: str = Field(default="")# (required) the step action id
    timeout: str = Field(default="")# (optional) the step timeout
    inputs: str = Field(default="")# (optional) the step inputs, assuming string representation of JSON
    parents: typing.List[str] = Field(default_factory=list)# (optional) the step parents. if none are passed in, this is a root step
    user_data: str = Field(default="")# (optional) the custom step user data, assuming string representation of JSON
    retries: int = Field(default=0)# (optional) the number of retries for the step, default 0
    rate_limits: typing.List[CreateStepRateLimit] = Field(default_factory=list)# (optional) the rate limits for the step
    worker_labels: typing.Dict[str, DesiredWorkerLabels] = Field(default_factory=dict)# (optional) the desired worker affinity state for the step

class CreateWorkflowJobOpts(BaseModel):
    """
     CreateWorkflowJobOpts represents options to create a workflow job.
    """

    name: str = Field(default="")# (required) the job name
    description: str = Field(default="")# (optional) the job description
    steps: typing.List[CreateWorkflowStepOpts] = Field(default_factory=list)# (required) the job steps

class WorkflowConcurrencyOpts(BaseModel):
    action: str = Field(default="")# (required) the action id for getting the concurrency group
    max_runs: int = Field(default=0)# (optional) the maximum number of concurrent workflow runs, default 1
    limit_strategy: ConcurrencyLimitStrategy = Field(default=0)# (optional) the strategy to use when the concurrency limit is reached, default CANCEL_IN_PROGRESS

class CreateWorkflowVersionOpts(BaseModel):
    """
     CreateWorkflowVersionOpts represents options to create a workflow version.
    """

    name: str = Field(default="")# (required) the workflow name
    description: str = Field(default="")# (optional) the workflow description
    version: str = Field(default="")# (required) the workflow version
    event_triggers: typing.List[str] = Field(default_factory=list)# (optional) event triggers for the workflow
    cron_triggers: typing.List[str] = Field(default_factory=list)# (optional) cron triggers for the workflow
    scheduled_triggers: typing.List[datetime] = Field(default_factory=list)# (optional) scheduled triggers for the workflow
    jobs: typing.List[CreateWorkflowJobOpts] = Field(default_factory=list)# (required) the workflow jobs
    concurrency: WorkflowConcurrencyOpts = Field()# (optional) the workflow concurrency options
    schedule_timeout: typing.Optional[str] = Field(default="")# (optional) the timeout for the schedule
    cron_input: typing.Optional[str] = Field(default="")# (optional) the input for the cron trigger
    on_failure_job: typing.Optional[CreateWorkflowJobOpts] = Field(default=None)# (optional) the job to run on failure
    sticky: typing.Optional[StickyStrategy] = Field(default=0)# (optional) the sticky strategy for assigning steps to workers
    kind: typing.Optional[WorkflowKind] = Field(default=0)# (optional) the kind of workflow

class PutWorkflowRequest(BaseModel):
    opts: CreateWorkflowVersionOpts = Field()

class DesiredWorkerLabels(BaseModel):
# value of the affinity
    strValue: typing.Optional[str] = Field(default="")
    intValue: typing.Optional[int] = Field(default=0)
#*
# (optional) Specifies whether the affinity setting is required.
# If required, the worker will not accept actions that do not have a truthy affinity setting.
#
# Defaults to false.
    required: typing.Optional[bool] = Field(default=False)
#*
# (optional) Specifies the comparator for the affinity setting.
# If not set, the default is EQUAL.
    comparator: typing.Optional[WorkerLabelComparator] = Field(default=0)
#*
# (optional) Specifies the weight of the affinity setting.
# If not set, the default is 100.
    weight: typing.Optional[int] = Field(default=0)

class ListWorkflowsRequest(BaseModel):
    """
     ListWorkflowsRequest is the request for ListWorkflows.
    """

class ScheduleWorkflowRequest(BaseModel):
    name: str = Field(default="")
    schedules: typing.List[datetime] = Field(default_factory=list)
# (optional) the input data for the workflow
    input: str = Field(default="")
# (optional) the parent workflow run id
    parent_id: typing.Optional[str] = Field(default="")
# (optional) the parent step run id
    parent_step_run_id: typing.Optional[str] = Field(default="")
# (optional) the index of the child workflow. if this is set, matches on the index or the
# child key will be a no-op, even if the schedule has changed.
    child_index: typing.Optional[int] = Field(default=0)
# (optional) the key for the child. if this is set, matches on the index or the
# child key will be a no-op, even if the schedule has changed.
    child_key: typing.Optional[str] = Field(default="")

class WorkflowVersion(BaseModel):
    """
     WorkflowVersion represents the WorkflowVersion model.
    """

    id: str = Field(default="")
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    version: str = Field(default="")
    order: int = Field(default=0)
    workflow_id: str = Field(default="")

class WorkflowTriggerEventRef(BaseModel):
    """
     WorkflowTriggerEventRef represents the WorkflowTriggerEventRef model.
    """

    parent_id: str = Field(default="")
    event_key: str = Field(default="")

class WorkflowTriggerCronRef(BaseModel):
    """
     WorkflowTriggerCronRef represents the WorkflowTriggerCronRef model.
    """

    parent_id: str = Field(default="")
    cron: str = Field(default="")

class TriggerWorkflowRequest(BaseModel):
    name: str = Field(default="")
# (optional) the input data for the workflow
    input: str = Field(default="")
# (optional) the parent workflow run id
    parent_id: typing.Optional[str] = Field(default="")
# (optional) the parent step run id
    parent_step_run_id: typing.Optional[str] = Field(default="")
# (optional) the index of the child workflow. if this is set, matches on the index or the
# child key will return an existing workflow run if the parent id, parent step run id, and
# child index/key match an existing workflow run.
    child_index: typing.Optional[int] = Field(default=0)
# (optional) the key for the child. if this is set, matches on the index or the
# child key will return an existing workflow run if the parent id, parent step run id, and
# child index/key match an existing workflow run.
    child_key: typing.Optional[str] = Field(default="")
# (optional) additional metadata for the workflow
    additional_metadata: typing.Optional[str] = Field(default="")
# (optional) desired worker id for the workflow run,
# requires the workflow definition to have a sticky strategy
    desired_worker_id: typing.Optional[str] = Field(default="")

class TriggerWorkflowResponse(BaseModel):
    workflow_run_id: str = Field(default="")

class PutRateLimitRequest(BaseModel):
# (required) the global key for the rate limit
    key: str = Field(default="")
# (required) the max limit for the rate limit (per unit of time)
    limit: int = Field(default=0)
# (required) the duration of time for the rate limit (second|minute|hour)
    duration: RateLimitDuration = Field(default=0)

class PutRateLimitResponse(BaseModel):    pass
