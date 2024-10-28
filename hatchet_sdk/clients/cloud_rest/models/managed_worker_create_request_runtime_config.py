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

from pydantic import BaseModel, ConfigDict, Field, StrictStr, field_validator
from typing_extensions import Annotated, Self


class ManagedWorkerCreateRequestRuntimeConfig(BaseModel):
    """
    ManagedWorkerCreateRequestRuntimeConfig
    """  # noqa: E501

    num_replicas: Annotated[int, Field(le=1000, strict=True, ge=0)] = Field(
        alias="numReplicas"
    )
    regions: Optional[List[StrictStr]] = Field(
        default=None, description="The region to deploy the worker to"
    )
    cpu_kind: StrictStr = Field(
        description="The kind of CPU to use for the worker", alias="cpuKind"
    )
    cpus: Annotated[int, Field(le=64, strict=True, ge=1)] = Field(
        description="The number of CPUs to use for the worker"
    )
    memory_mb: Annotated[int, Field(le=65536, strict=True, ge=1024)] = Field(
        description="The amount of memory in MB to use for the worker", alias="memoryMb"
    )
    actions: Optional[List[StrictStr]] = None
    slots: Optional[Annotated[int, Field(le=1000, strict=True, ge=1)]] = None
    __properties: ClassVar[List[str]] = [
        "numReplicas",
        "regions",
        "cpuKind",
        "cpus",
        "memoryMb",
        "actions",
        "slots",
    ]

    @field_validator("regions")
    def regions_validate_enum(cls, value):
        """Validates the enum"""
        if value is None:
            return value

        for i in value:
            if i not in set(
                [
                    "ams",
                    "arn",
                    "atl",
                    "bog",
                    "bos",
                    "cdg",
                    "den",
                    "dfw",
                    "ewr",
                    "eze",
                    "gdl",
                    "gig",
                    "gru",
                    "hkg",
                    "iad",
                    "jnb",
                    "lax",
                    "lhr",
                    "mad",
                    "mia",
                    "nrt",
                    "ord",
                    "otp",
                    "phx",
                    "qro",
                    "scl",
                    "sea",
                    "sin",
                    "sjc",
                    "syd",
                    "waw",
                    "yul",
                    "yyz",
                ]
            ):
                raise ValueError(
                    "each list item must be one of ('ams', 'arn', 'atl', 'bog', 'bos', 'cdg', 'den', 'dfw', 'ewr', 'eze', 'gdl', 'gig', 'gru', 'hkg', 'iad', 'jnb', 'lax', 'lhr', 'mad', 'mia', 'nrt', 'ord', 'otp', 'phx', 'qro', 'scl', 'sea', 'sin', 'sjc', 'syd', 'waw', 'yul', 'yyz')"
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
        """Create an instance of ManagedWorkerCreateRequestRuntimeConfig from a JSON string"""
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
        """Create an instance of ManagedWorkerCreateRequestRuntimeConfig from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)

        _obj = cls.model_validate(
            {
                "numReplicas": obj.get("numReplicas"),
                "regions": obj.get("regions"),
                "cpuKind": obj.get("cpuKind"),
                "cpus": obj.get("cpus"),
                "memoryMb": obj.get("memoryMb"),
                "actions": obj.get("actions"),
                "slots": obj.get("slots"),
            }
        )
        return _obj
