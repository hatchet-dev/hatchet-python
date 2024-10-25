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

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictStr
from typing_extensions import Self

from hatchet_sdk.clients.rest.models.api_resource_meta import APIResourceMeta


class User(BaseModel):
    """
    User
    """  # noqa: E501

    metadata: APIResourceMeta
    name: Optional[StrictStr] = Field(
        default=None, description="The display name of the user."
    )
    email: StrictStr = Field(description="The email address of the user.")
    email_verified: StrictBool = Field(
        description="Whether the user has verified their email address.",
        alias="emailVerified",
    )
    has_password: Optional[StrictBool] = Field(
        default=None,
        description="Whether the user has a password set.",
        alias="hasPassword",
    )
    email_hash: Optional[StrictStr] = Field(
        default=None,
        description="A hash of the user's email address for use with Pylon Support Chat",
        alias="emailHash",
    )
    __properties: ClassVar[List[str]] = [
        "metadata",
        "name",
        "email",
        "emailVerified",
        "hasPassword",
        "emailHash",
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
        """Create an instance of User from a JSON string"""
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
        return _dict

    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        """Create an instance of User from a dict"""
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
                "email": obj.get("email"),
                "emailVerified": obj.get("emailVerified"),
                "hasPassword": obj.get("hasPassword"),
                "emailHash": obj.get("emailHash"),
            }
        )
        return _obj
