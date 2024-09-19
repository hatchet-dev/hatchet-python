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

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, field_validator
from typing_extensions import Annotated, Self

from hatchet_sdk.clients.rest.models.api_resource_meta import APIResourceMeta
from hatchet_sdk.clients.rest.models.recent_step_runs import RecentStepRuns
from hatchet_sdk.clients.rest.models.semaphore_slots import SemaphoreSlots
from hatchet_sdk.clients.rest.models.worker_label import WorkerLabel


class Worker(BaseModel):
    """
    Worker
    """  # noqa: E501

    metadata: APIResourceMeta
    name: StrictStr = Field(description="The name of the worker.")
    type: StrictStr
    last_heartbeat_at: Optional[datetime] = Field(
        default=None,
        description="The time this worker last sent a heartbeat.",
        alias="lastHeartbeatAt",
    )
    last_listener_established: Optional[datetime] = Field(
        default=None,
        description="The time this worker last sent a heartbeat.",
        alias="lastListenerEstablished",
    )
    actions: Optional[List[StrictStr]] = Field(
        default=None, description="The actions this worker can perform."
    )
    slots: Optional[List[SemaphoreSlots]] = Field(
        default=None, description="The semaphore slot state for the worker."
    )
    recent_step_runs: Optional[List[RecentStepRuns]] = Field(
        default=None,
        description="The recent step runs for the worker.",
        alias="recentStepRuns",
    )
    status: Optional[StrictStr] = Field(
        default=None, description="The status of the worker."
    )
    max_runs: Optional[StrictInt] = Field(
        default=None,
        description="The maximum number of runs this worker can execute concurrently.",
        alias="maxRuns",
    )
    available_runs: Optional[StrictInt] = Field(
        default=None,
        description="The number of runs this worker can execute concurrently.",
        alias="availableRuns",
    )
    dispatcher_id: Optional[
        Annotated[str, Field(min_length=36, strict=True, max_length=36)]
    ] = Field(
        default=None,
        description="the id of the assigned dispatcher, in UUID format",
        alias="dispatcherId",
    )
    labels: Optional[List[WorkerLabel]] = Field(
        default=None, description="The current label state of the worker."
    )
    webhook_url: Optional[StrictStr] = Field(
        default=None, description="The webhook URL for the worker.", alias="webhookUrl"
    )
    webhook_id: Optional[StrictStr] = Field(
        default=None, description="The webhook ID for the worker.", alias="webhookId"
    )
    __properties: ClassVar[List[str]] = [
        "metadata",
        "name",
        "type",
        "lastHeartbeatAt",
        "lastListenerEstablished",
        "actions",
        "slots",
        "recentStepRuns",
        "status",
        "maxRuns",
        "availableRuns",
        "dispatcherId",
        "labels",
        "webhookUrl",
        "webhookId",
    ]

    @field_validator("type")
    def type_validate_enum(cls, value):
        """Validates the enum"""
        if value not in set(["SELFHOSTED", "MANAGED", "WEBHOOK"]):
            raise ValueError(
                "must be one of enum values ('SELFHOSTED', 'MANAGED', 'WEBHOOK')"
            )
        return value

    @field_validator("status")
    def status_validate_enum(cls, value):
        """Validates the enum"""
        if value is None:
            return value

        if value not in set(["ACTIVE", "INACTIVE", "PAUSED"]):
            raise ValueError(
                "must be one of enum values ('ACTIVE', 'INACTIVE', 'PAUSED')"
            )
        return value

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
        """Create an instance of Worker from a JSON string"""
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
        # override the default output from pydantic by calling `to_dict()` of each item in slots (list)
        _items = []
        if self.slots:
            for _item_slots in self.slots:
                if _item_slots:
                    _items.append(_item_slots.to_dict())
            _dict["slots"] = _items
        # override the default output from pydantic by calling `to_dict()` of each item in recent_step_runs (list)
        _items = []
        if self.recent_step_runs:
            for _item_recent_step_runs in self.recent_step_runs:
                if _item_recent_step_runs:
                    _items.append(_item_recent_step_runs.to_dict())
            _dict["recentStepRuns"] = _items
        # override the default output from pydantic by calling `to_dict()` of each item in labels (list)
        _items = []
        if self.labels:
            for _item_labels in self.labels:
                if _item_labels:
                    _items.append(_item_labels.to_dict())
            _dict["labels"] = _items
        return _dict

    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        """Create an instance of Worker from a dict"""
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
                "name": obj.get("name"),
                "type": obj.get("type"),
                "lastHeartbeatAt": obj.get("lastHeartbeatAt"),
                "lastListenerEstablished": obj.get("lastListenerEstablished"),
                "actions": obj.get("actions"),
                "slots": (
                    [SemaphoreSlots.from_dict(_item) for _item in obj["slots"]]
                    if obj.get("slots") is not None
                    else None
                ),
                "recentStepRuns": (
                    [RecentStepRuns.from_dict(_item) for _item in obj["recentStepRuns"]]
                    if obj.get("recentStepRuns") is not None
                    else None
                ),
                "status": obj.get("status"),
                "maxRuns": obj.get("maxRuns"),
                "availableRuns": obj.get("availableRuns"),
                "dispatcherId": obj.get("dispatcherId"),
                "labels": (
                    [WorkerLabel.from_dict(_item) for _item in obj["labels"]]
                    if obj.get("labels") is not None
                    else None
                ),
                "webhookUrl": obj.get("webhookUrl"),
                "webhookId": obj.get("webhookId"),
            }
        )
        return _obj
