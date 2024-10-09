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
from typing import Any, ClassVar, Dict, List, Optional, Set

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr
from typing_extensions import Self

from hatchet_sdk.clients.rest.models.api_resource_meta import APIResourceMeta
from hatchet_sdk.clients.rest.models.job import Job
from hatchet_sdk.clients.rest.models.workflow import Workflow
from hatchet_sdk.clients.rest.models.workflow_concurrency import WorkflowConcurrency
from hatchet_sdk.clients.rest.models.workflow_triggers import WorkflowTriggers


class WorkflowVersion(BaseModel):
    """
    WorkflowVersion
    """  # noqa: E501

    metadata: APIResourceMeta
    version: StrictStr = Field(description="The version of the workflow.")
    order: StrictInt
    workflow_id: StrictStr = Field(alias="workflowId")
    workflow: Optional[Workflow] = None
    concurrency: Optional[WorkflowConcurrency] = None
    triggers: Optional[WorkflowTriggers] = None
    schedule_timeout: Optional[StrictStr] = Field(default=None, alias="scheduleTimeout")
    jobs: Optional[List[Job]] = None
    __properties: ClassVar[List[str]] = [
        "metadata",
        "version",
        "order",
        "workflowId",
        "workflow",
        "concurrency",
        "triggers",
        "scheduleTimeout",
        "jobs",
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
        """Create an instance of WorkflowVersion from a JSON string"""
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
        # override the default output from pydantic by calling `to_dict()` of workflow
        if self.workflow:
            _dict["workflow"] = self.workflow.to_dict()
        # override the default output from pydantic by calling `to_dict()` of concurrency
        if self.concurrency:
            _dict["concurrency"] = self.concurrency.to_dict()
        # override the default output from pydantic by calling `to_dict()` of triggers
        if self.triggers:
            _dict["triggers"] = self.triggers.to_dict()
        # override the default output from pydantic by calling `to_dict()` of each item in jobs (list)
        _items = []
        if self.jobs:
            for _item in self.jobs:
                if _item:
                    _items.append(_item.to_dict())
            _dict["jobs"] = _items
        return _dict

    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        """Create an instance of WorkflowVersion from a dict"""
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
                "version": obj.get("version"),
                "order": obj.get("order"),
                "workflowId": obj.get("workflowId"),
                "workflow": (
                    Workflow.from_dict(obj["workflow"])
                    if obj.get("workflow") is not None
                    else None
                ),
                "concurrency": (
                    WorkflowConcurrency.from_dict(obj["concurrency"])
                    if obj.get("concurrency") is not None
                    else None
                ),
                "triggers": (
                    WorkflowTriggers.from_dict(obj["triggers"])
                    if obj.get("triggers") is not None
                    else None
                ),
                "scheduleTimeout": obj.get("scheduleTimeout"),
                "jobs": (
                    [Job.from_dict(_item) for _item in obj["jobs"]]
                    if obj.get("jobs") is not None
                    else None
                ),
            }
        )
        return _obj
