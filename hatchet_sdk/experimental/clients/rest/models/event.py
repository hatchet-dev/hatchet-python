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

from pydantic import BaseModel, ConfigDict, Field, StrictStr
from typing_extensions import Self

from hatchet_sdk.experimental.clients.rest.models.api_resource_meta import (
    APIResourceMeta,
)
from hatchet_sdk.experimental.clients.rest.models.event_workflow_run_summary import (
    EventWorkflowRunSummary,
)
from hatchet_sdk.experimental.clients.rest.models.tenant import Tenant


class Event(BaseModel):
    """
    Event
    """  # noqa: E501

    metadata: APIResourceMeta
    key: StrictStr = Field(description="The key for the event.")
    tenant: Optional[Tenant] = Field(
        default=None, description="The tenant associated with this event."
    )
    tenant_id: StrictStr = Field(
        description="The ID of the tenant associated with this event.", alias="tenantId"
    )
    workflow_run_summary: Optional[EventWorkflowRunSummary] = Field(
        default=None,
        description="The workflow run summary for this event.",
        alias="workflowRunSummary",
    )
    additional_metadata: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Additional metadata for the event.",
        alias="additionalMetadata",
    )
    __properties: ClassVar[List[str]] = [
        "metadata",
        "key",
        "tenant",
        "tenantId",
        "workflowRunSummary",
        "additionalMetadata",
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
        """Create an instance of Event from a JSON string"""
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
        # override the default output from pydantic by calling `to_dict()` of tenant
        if self.tenant:
            _dict["tenant"] = self.tenant.to_dict()
        # override the default output from pydantic by calling `to_dict()` of workflow_run_summary
        if self.workflow_run_summary:
            _dict["workflowRunSummary"] = self.workflow_run_summary.to_dict()
        return _dict

    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        """Create an instance of Event from a dict"""
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
                "key": obj.get("key"),
                "tenant": (
                    Tenant.from_dict(obj["tenant"])
                    if obj.get("tenant") is not None
                    else None
                ),
                "tenantId": obj.get("tenantId"),
                "workflowRunSummary": (
                    EventWorkflowRunSummary.from_dict(obj["workflowRunSummary"])
                    if obj.get("workflowRunSummary") is not None
                    else None
                ),
                "additionalMetadata": obj.get("additionalMetadata"),
            }
        )
        return _obj
