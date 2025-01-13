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

from pydantic import BaseModel, ConfigDict, Field, StrictStr
from typing_extensions import Self

from hatchet_sdk.experimental.clients.rest.models.step_run_status import StepRunStatus


class SemaphoreSlots(BaseModel):
    """
    SemaphoreSlots
    """  # noqa: E501

    step_run_id: StrictStr = Field(description="The step run id.", alias="stepRunId")
    action_id: StrictStr = Field(description="The action id.", alias="actionId")
    started_at: Optional[datetime] = Field(
        default=None, description="The time this slot was started.", alias="startedAt"
    )
    timeout_at: Optional[datetime] = Field(
        default=None, description="The time this slot will timeout.", alias="timeoutAt"
    )
    workflow_run_id: StrictStr = Field(
        description="The workflow run id.", alias="workflowRunId"
    )
    status: StepRunStatus
    __properties: ClassVar[List[str]] = [
        "stepRunId",
        "actionId",
        "startedAt",
        "timeoutAt",
        "workflowRunId",
        "status",
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
        """Create an instance of SemaphoreSlots from a JSON string"""
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
        return _dict

    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        """Create an instance of SemaphoreSlots from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)

        _obj = cls.model_validate(
            {
                "stepRunId": obj.get("stepRunId"),
                "actionId": obj.get("actionId"),
                "startedAt": obj.get("startedAt"),
                "timeoutAt": obj.get("timeoutAt"),
                "workflowRunId": obj.get("workflowRunId"),
                "status": obj.get("status"),
            }
        )
        return _obj
