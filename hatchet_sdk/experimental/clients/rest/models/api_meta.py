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

from hatchet_sdk.experimental.clients.rest.models.api_meta_auth import APIMetaAuth
from hatchet_sdk.experimental.clients.rest.models.api_meta_posthog import APIMetaPosthog


class APIMeta(BaseModel):
    """
    APIMeta
    """  # noqa: E501

    auth: Optional[APIMetaAuth] = None
    pylon_app_id: Optional[StrictStr] = Field(
        default=None,
        description="the Pylon app ID for usepylon.com chat support",
        alias="pylonAppId",
    )
    posthog: Optional[APIMetaPosthog] = None
    allow_signup: Optional[StrictBool] = Field(
        default=None,
        description="whether or not users can sign up for this instance",
        alias="allowSignup",
    )
    allow_invites: Optional[StrictBool] = Field(
        default=None,
        description="whether or not users can invite other users to this instance",
        alias="allowInvites",
    )
    allow_create_tenant: Optional[StrictBool] = Field(
        default=None,
        description="whether or not users can create new tenants",
        alias="allowCreateTenant",
    )
    allow_change_password: Optional[StrictBool] = Field(
        default=None,
        description="whether or not users can change their password",
        alias="allowChangePassword",
    )
    __properties: ClassVar[List[str]] = [
        "auth",
        "pylonAppId",
        "posthog",
        "allowSignup",
        "allowInvites",
        "allowCreateTenant",
        "allowChangePassword",
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
        """Create an instance of APIMeta from a JSON string"""
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
        # override the default output from pydantic by calling `to_dict()` of auth
        if self.auth:
            _dict["auth"] = self.auth.to_dict()
        # override the default output from pydantic by calling `to_dict()` of posthog
        if self.posthog:
            _dict["posthog"] = self.posthog.to_dict()
        return _dict

    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        """Create an instance of APIMeta from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)

        _obj = cls.model_validate(
            {
                "auth": (
                    APIMetaAuth.from_dict(obj["auth"])
                    if obj.get("auth") is not None
                    else None
                ),
                "pylonAppId": obj.get("pylonAppId"),
                "posthog": (
                    APIMetaPosthog.from_dict(obj["posthog"])
                    if obj.get("posthog") is not None
                    else None
                ),
                "allowSignup": obj.get("allowSignup"),
                "allowInvites": obj.get("allowInvites"),
                "allowCreateTenant": obj.get("allowCreateTenant"),
                "allowChangePassword": obj.get("allowChangePassword"),
            }
        )
        return _obj
