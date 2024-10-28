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
from typing_extensions import Annotated, Self

from hatchet_sdk.clients.cloud_rest.models.github_app_list_installations200_response_rows_inner_metadata import (
    GithubAppListInstallations200ResponseRowsInnerMetadata,
)
from hatchet_sdk.clients.cloud_rest.models.github_app_list_repos200_response_inner import (
    GithubAppListRepos200ResponseInner,
)
from hatchet_sdk.clients.cloud_rest.models.managed_worker_list200_response_rows_inner_build_config_steps_inner import (
    ManagedWorkerList200ResponseRowsInnerBuildConfigStepsInner,
)


class ManagedWorkerList200ResponseRowsInnerBuildConfig(BaseModel):
    """
    ManagedWorkerList200ResponseRowsInnerBuildConfig
    """  # noqa: E501

    metadata: GithubAppListInstallations200ResponseRowsInnerMetadata
    github_installation_id: Annotated[
        str, Field(min_length=36, strict=True, max_length=36)
    ] = Field(alias="githubInstallationId")
    github_repository: GithubAppListRepos200ResponseInner = Field(
        alias="githubRepository"
    )
    github_repository_branch: StrictStr = Field(alias="githubRepositoryBranch")
    steps: Optional[
        List[ManagedWorkerList200ResponseRowsInnerBuildConfigStepsInner]
    ] = None
    __properties: ClassVar[List[str]] = [
        "metadata",
        "githubInstallationId",
        "githubRepository",
        "githubRepositoryBranch",
        "steps",
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
        """Create an instance of ManagedWorkerList200ResponseRowsInnerBuildConfig from a JSON string"""
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
        # override the default output from pydantic by calling `to_dict()` of github_repository
        if self.github_repository:
            _dict["githubRepository"] = self.github_repository.to_dict()
        # override the default output from pydantic by calling `to_dict()` of each item in steps (list)
        _items = []
        if self.steps:
            for _item in self.steps:
                if _item:
                    _items.append(_item.to_dict())
            _dict["steps"] = _items
        return _dict

    @classmethod
    def from_dict(cls, obj: Optional[Dict[str, Any]]) -> Optional[Self]:
        """Create an instance of ManagedWorkerList200ResponseRowsInnerBuildConfig from a dict"""
        if obj is None:
            return None

        if not isinstance(obj, dict):
            return cls.model_validate(obj)

        _obj = cls.model_validate(
            {
                "metadata": (
                    GithubAppListInstallations200ResponseRowsInnerMetadata.from_dict(
                        obj["metadata"]
                    )
                    if obj.get("metadata") is not None
                    else None
                ),
                "githubInstallationId": obj.get("githubInstallationId"),
                "githubRepository": (
                    GithubAppListRepos200ResponseInner.from_dict(
                        obj["githubRepository"]
                    )
                    if obj.get("githubRepository") is not None
                    else None
                ),
                "githubRepositoryBranch": obj.get("githubRepositoryBranch"),
                "steps": (
                    [
                        ManagedWorkerList200ResponseRowsInnerBuildConfigStepsInner.from_dict(
                            _item
                        )
                        for _item in obj["steps"]
                    ]
                    if obj.get("steps") is not None
                    else None
                ),
            }
        )
        return _obj
