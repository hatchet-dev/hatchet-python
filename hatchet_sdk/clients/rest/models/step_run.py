# coding: utf-8

"""
    Hatchet API

    The Hatchet API

    The version of the OpenAPI document: 1.0.0
    Generated by OpenAPI Generator (https://openapi-generator.tech)

    Do not edit the class manually.
"""  # noqa: E501


from __future__ import annotations

import json
import pprint
import re  # noqa: F401
from datetime import datetime
from typing import Any, ClassVar, Dict, List, Optional, Set

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr
from typing_extensions import Self

from hatchet_sdk.clients.rest.models.api_resource_meta import APIResourceMeta
from hatchet_sdk.clients.rest.models.job_run import JobRun
from hatchet_sdk.clients.rest.models.step import Step
from hatchet_sdk.clients.rest.models.step_run_status import StepRunStatus


class StepRun(BaseModel):
    """
    StepRun
    """  # noqa: E501

    metadata: APIResourceMeta
    tenant_id: StrictStr = Field(alias="tenantId")
    job_run_id: StrictStr = Field(alias="jobRunId")
    job_run: Optional[JobRun] = Field(default=None, alias="jobRun")
    step_id: StrictStr = Field(alias="stepId")
    step: Optional[Step] = None
    child_workflows_count: Optional[StrictInt] = Field(
        default=None, alias="childWorkflowsCount"
    )
    parents: Optional[List[StrictStr]] = None
    child_workflow_runs: Optional[List[StrictStr]] = Field(
        default=None, alias="childWorkflowRuns"
    )
    worker_id: Optional[StrictStr] = Field(default=None, alias="workerId")
    input: Optional[StrictStr] = None
    output: Optional[StrictStr] = None
    status: StepRunStatus
    requeue_after: Optional[datetime] = Field(default=None, alias="requeueAfter")
    result: Optional[Dict[str, Any]] = None
    error: Optional[StrictStr] = None
    started_at: Optional[datetime] = Field(default=None, alias="startedAt")
    started_at_epoch: Optional[StrictInt] = Field(default=None, alias="startedAtEpoch")
    finished_at: Optional[datetime] = Field(default=None, alias="finishedAt")
    finished_at_epoch: Optional[StrictInt] = Field(
        default=None, alias="finishedAtEpoch"
    )
    timeout_at: Optional[datetime] = Field(default=None, alias="timeoutAt")
    timeout_at_epoch: Optional[StrictInt] = Field(default=None, alias="timeoutAtEpoch")
    cancelled_at: Optional[datetime] = Field(default=None, alias="cancelledAt")
    cancelled_at_epoch: Optional[StrictInt] = Field(
        default=None, alias="cancelledAtEpoch"
    )
    cancelled_reason: Optional[StrictStr] = Field(default=None, alias="cancelledReason")
    cancelled_error: Optional[StrictStr] = Field(default=None, alias="cancelledError")
    __properties: ClassVar[List[str]] = [
        "metadata",
        "tenantId",
        "jobRunId",
        "jobRun",
        "stepId",
        "step",
        "childWorkflowsCount",
        "parents",
        "childWorkflowRuns",
        "workerId",
        "input",
        "output",
        "status",
        "requeueAfter",
        "result",
        "error",
        "startedAt",
        "startedAtEpoch",
        "finishedAt",
        "finishedAtEpoch",
        "timeoutAt",
        "timeoutAtEpoch",
        "cancelledAt",
        "cancelledAtEpoch",
        "cancelledReason",
        "cancelledError",
    ]

    model_config = ConfigDict(
        populate_by_name=True,
        validate_assignment=True,
        protected_namespaces=(),
    )

    def to_str(self) -> str:
        """Returns the string representation of the model using alias"""
        return pprint.pformat(self.model_dump(by_alias=True))

    def to_json(self) -> str:
        """Returns the JSON representation of the model using alias"""
        # TODO: pydantic v2: use .model_dump_json(by_alias=True, exclude_unset=True) instead
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> Optional[Self]:
        """Create an instance of StepRun from a JSON string"""
        return cls.from_dict(json.loads(json_str))

    def to_dict(self) -> Dict[str, Any]:
        """Return the dictionary representation of the model using alias.

        This has the following differences from calling pydantic's
        `self.model_dump(by_alias=True)`:

        * `None` is only added to the output dict for nullable fields that
          were set at model initialization. Other fields with value `None`
          are ignored.
        """
        excluded_fields: Set[str] = set([])

        _dict = self.model_dump(
            by_alias=True,
            exclude=excluded_fields,
            exclude_none=True,
        )
        # override the default output from pydantic by calling `to_dict()` of metadata
        if self.metadata:
            _dict["metadata"] = self.metadata.to_dict()
        # override the default output from pydantic by calling `to_dict()` of job_run
        if self.job_run:
            _dict["jobRun"] = self.job_run.to_dict()
        # override the default output from pydantic by calling `to_dict()` of step
        if self.step:
            _dict["step"] = self.step.to_dict()
        return _dict

    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        """Create an instance of StepRun from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)

        _obj = cls.model_validate(
            {
                "metadata": (
                    APIResourceMeta.from_dict(obj["metadata"])
                    if obj.get("metadata") is not None
                    else None
                ),
                "tenantId": obj.get("tenantId"),
                "jobRunId": obj.get("jobRunId"),
                "jobRun": (
                    JobRun.from_dict(obj["jobRun"])
                    if obj.get("jobRun") is not None
                    else None
                ),
                "stepId": obj.get("stepId"),
                "step": (
                    Step.from_dict(obj["step"]) if obj.get("step") is not None else None
                ),
                "childWorkflowsCount": obj.get("childWorkflowsCount"),
                "parents": obj.get("parents"),
                "childWorkflowRuns": obj.get("childWorkflowRuns"),
                "workerId": obj.get("workerId"),
                "input": obj.get("input"),
                "output": obj.get("output"),
                "status": obj.get("status"),
                "requeueAfter": obj.get("requeueAfter"),
                "result": obj.get("result"),
                "error": obj.get("error"),
                "startedAt": obj.get("startedAt"),
                "startedAtEpoch": obj.get("startedAtEpoch"),
                "finishedAt": obj.get("finishedAt"),
                "finishedAtEpoch": obj.get("finishedAtEpoch"),
                "timeoutAt": obj.get("timeoutAt"),
                "timeoutAtEpoch": obj.get("timeoutAtEpoch"),
                "cancelledAt": obj.get("cancelledAt"),
                "cancelledAtEpoch": obj.get("cancelledAtEpoch"),
                "cancelledReason": obj.get("cancelledReason"),
                "cancelledError": obj.get("cancelledError"),
            }
        )
        return _obj


# TODO: Rewrite to not use raise_errors
StepRun.model_rebuild(raise_errors=False)
