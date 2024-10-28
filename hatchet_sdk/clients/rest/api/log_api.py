# coding: utf-8

"""
    Hatchet API

    The Hatchet API

    The version of the OpenAPI document: 1.0.0
    Generated by OpenAPI Generator (https://openapi-generator.tech)

    Do not edit the class manually.
"""  # noqa: E501

import warnings
from typing import Any, Dict, List, Optional, Tuple, Union

from pydantic import Field, StrictFloat, StrictInt, StrictStr, validate_call
from typing_extensions import Annotated

from hatchet_sdk.clients.rest.api_client import ApiClient, RequestSerialized
from hatchet_sdk.clients.rest.api_response import ApiResponse
from hatchet_sdk.clients.rest.models.log_line_level import LogLineLevel
from hatchet_sdk.clients.rest.models.log_line_list import LogLineList
from hatchet_sdk.clients.rest.models.log_line_order_by_direction import (
    LogLineOrderByDirection,
)
from hatchet_sdk.clients.rest.models.log_line_order_by_field import LogLineOrderByField
from hatchet_sdk.clients.rest.rest import RESTResponseType


class LogApi:
    """NOTE: This class is auto generated by OpenAPI Generator
    Ref: https://openapi-generator.tech

    Do not edit the class manually.
    """

    def __init__(self, api_client=None) -> None:
        if api_client is None:
            api_client = ApiClient.get_default()
        self.api_client = api_client

    @validate_call
    async def log_line_list(
        self,
        step_run: Annotated[
            str,
            Field(
                min_length=36, strict=True, max_length=36, description="The step run id"
            ),
        ],
        offset: Annotated[
            Optional[StrictInt], Field(description="The number to skip")
        ] = None,
        limit: Annotated[
            Optional[StrictInt], Field(description="The number to limit by")
        ] = None,
        levels: Annotated[
            Optional[List[LogLineLevel]],
            Field(description="A list of levels to filter by"),
        ] = None,
        search: Annotated[
            Optional[StrictStr], Field(description="The search query to filter for")
        ] = None,
        order_by_field: Annotated[
            Optional[LogLineOrderByField], Field(description="What to order by")
        ] = None,
        order_by_direction: Annotated[
            Optional[LogLineOrderByDirection], Field(description="The order direction")
        ] = None,
        _request_timeout: Union[
            None,
            Annotated[StrictFloat, Field(gt=0)],
            Tuple[
                Annotated[StrictFloat, Field(gt=0)], Annotated[StrictFloat, Field(gt=0)]
            ],
        ] = None,
        _request_auth: Optional[Dict[StrictStr, Any]] = None,
        _content_type: Optional[StrictStr] = None,
        _headers: Optional[Dict[StrictStr, Any]] = None,
        _host_index: Annotated[StrictInt, Field(ge=0, le=0)] = 0,
    ) -> LogLineList:
        """List log lines

        Lists log lines for a step run.

        :param step_run: The step run id (required)
        :type step_run: str
        :param offset: The number to skip
        :type offset: int
        :param limit: The number to limit by
        :type limit: int
        :param levels: A list of levels to filter by
        :type levels: List[LogLineLevel]
        :param search: The search query to filter for
        :type search: str
        :param order_by_field: What to order by
        :type order_by_field: LogLineOrderByField
        :param order_by_direction: The order direction
        :type order_by_direction: LogLineOrderByDirection
        :param _request_timeout: timeout setting for this request. If one
                                 number provided, it will be total request
                                 timeout. It can also be a pair (tuple) of
                                 (connection, read) timeouts.
        :type _request_timeout: int, tuple(int, int), optional
        :param _request_auth: set to override the auth_settings for an a single
                              request; this effectively ignores the
                              authentication in the spec for a single request.
        :type _request_auth: dict, optional
        :param _content_type: force content-type for the request.
        :type _content_type: str, Optional
        :param _headers: set to override the headers for a single
                         request; this effectively ignores the headers
                         in the spec for a single request.
        :type _headers: dict, optional
        :param _host_index: set to override the host_index for a single
                            request; this effectively ignores the host_index
                            in the spec for a single request.
        :type _host_index: int, optional
        :return: Returns the result object.
        """  # noqa: E501

        _param = self._log_line_list_serialize(
            step_run=step_run,
            offset=offset,
            limit=limit,
            levels=levels,
            search=search,
            order_by_field=order_by_field,
            order_by_direction=order_by_direction,
            _request_auth=_request_auth,
            _content_type=_content_type,
            _headers=_headers,
            _host_index=_host_index,
        )

        _response_types_map: Dict[str, Optional[str]] = {
            "200": "LogLineList",
            "400": "APIErrors",
            "403": "APIErrors",
        }
        response_data = await self.api_client.call_api(
            *_param, _request_timeout=_request_timeout
        )
        await response_data.read()
        return self.api_client.response_deserialize(
            response_data=response_data,
            response_types_map=_response_types_map,
        ).data

    @validate_call
    async def log_line_list_with_http_info(
        self,
        step_run: Annotated[
            str,
            Field(
                min_length=36, strict=True, max_length=36, description="The step run id"
            ),
        ],
        offset: Annotated[
            Optional[StrictInt], Field(description="The number to skip")
        ] = None,
        limit: Annotated[
            Optional[StrictInt], Field(description="The number to limit by")
        ] = None,
        levels: Annotated[
            Optional[List[LogLineLevel]],
            Field(description="A list of levels to filter by"),
        ] = None,
        search: Annotated[
            Optional[StrictStr], Field(description="The search query to filter for")
        ] = None,
        order_by_field: Annotated[
            Optional[LogLineOrderByField], Field(description="What to order by")
        ] = None,
        order_by_direction: Annotated[
            Optional[LogLineOrderByDirection], Field(description="The order direction")
        ] = None,
        _request_timeout: Union[
            None,
            Annotated[StrictFloat, Field(gt=0)],
            Tuple[
                Annotated[StrictFloat, Field(gt=0)], Annotated[StrictFloat, Field(gt=0)]
            ],
        ] = None,
        _request_auth: Optional[Dict[StrictStr, Any]] = None,
        _content_type: Optional[StrictStr] = None,
        _headers: Optional[Dict[StrictStr, Any]] = None,
        _host_index: Annotated[StrictInt, Field(ge=0, le=0)] = 0,
    ) -> ApiResponse[LogLineList]:
        """List log lines

        Lists log lines for a step run.

        :param step_run: The step run id (required)
        :type step_run: str
        :param offset: The number to skip
        :type offset: int
        :param limit: The number to limit by
        :type limit: int
        :param levels: A list of levels to filter by
        :type levels: List[LogLineLevel]
        :param search: The search query to filter for
        :type search: str
        :param order_by_field: What to order by
        :type order_by_field: LogLineOrderByField
        :param order_by_direction: The order direction
        :type order_by_direction: LogLineOrderByDirection
        :param _request_timeout: timeout setting for this request. If one
                                 number provided, it will be total request
                                 timeout. It can also be a pair (tuple) of
                                 (connection, read) timeouts.
        :type _request_timeout: int, tuple(int, int), optional
        :param _request_auth: set to override the auth_settings for an a single
                              request; this effectively ignores the
                              authentication in the spec for a single request.
        :type _request_auth: dict, optional
        :param _content_type: force content-type for the request.
        :type _content_type: str, Optional
        :param _headers: set to override the headers for a single
                         request; this effectively ignores the headers
                         in the spec for a single request.
        :type _headers: dict, optional
        :param _host_index: set to override the host_index for a single
                            request; this effectively ignores the host_index
                            in the spec for a single request.
        :type _host_index: int, optional
        :return: Returns the result object.
        """  # noqa: E501

        _param = self._log_line_list_serialize(
            step_run=step_run,
            offset=offset,
            limit=limit,
            levels=levels,
            search=search,
            order_by_field=order_by_field,
            order_by_direction=order_by_direction,
            _request_auth=_request_auth,
            _content_type=_content_type,
            _headers=_headers,
            _host_index=_host_index,
        )

        _response_types_map: Dict[str, Optional[str]] = {
            "200": "LogLineList",
            "400": "APIErrors",
            "403": "APIErrors",
        }
        response_data = await self.api_client.call_api(
            *_param, _request_timeout=_request_timeout
        )
        await response_data.read()
        return self.api_client.response_deserialize(
            response_data=response_data,
            response_types_map=_response_types_map,
        )

    @validate_call
    async def log_line_list_without_preload_content(
        self,
        step_run: Annotated[
            str,
            Field(
                min_length=36, strict=True, max_length=36, description="The step run id"
            ),
        ],
        offset: Annotated[
            Optional[StrictInt], Field(description="The number to skip")
        ] = None,
        limit: Annotated[
            Optional[StrictInt], Field(description="The number to limit by")
        ] = None,
        levels: Annotated[
            Optional[List[LogLineLevel]],
            Field(description="A list of levels to filter by"),
        ] = None,
        search: Annotated[
            Optional[StrictStr], Field(description="The search query to filter for")
        ] = None,
        order_by_field: Annotated[
            Optional[LogLineOrderByField], Field(description="What to order by")
        ] = None,
        order_by_direction: Annotated[
            Optional[LogLineOrderByDirection], Field(description="The order direction")
        ] = None,
        _request_timeout: Union[
            None,
            Annotated[StrictFloat, Field(gt=0)],
            Tuple[
                Annotated[StrictFloat, Field(gt=0)], Annotated[StrictFloat, Field(gt=0)]
            ],
        ] = None,
        _request_auth: Optional[Dict[StrictStr, Any]] = None,
        _content_type: Optional[StrictStr] = None,
        _headers: Optional[Dict[StrictStr, Any]] = None,
        _host_index: Annotated[StrictInt, Field(ge=0, le=0)] = 0,
    ) -> RESTResponseType:
        """List log lines

        Lists log lines for a step run.

        :param step_run: The step run id (required)
        :type step_run: str
        :param offset: The number to skip
        :type offset: int
        :param limit: The number to limit by
        :type limit: int
        :param levels: A list of levels to filter by
        :type levels: List[LogLineLevel]
        :param search: The search query to filter for
        :type search: str
        :param order_by_field: What to order by
        :type order_by_field: LogLineOrderByField
        :param order_by_direction: The order direction
        :type order_by_direction: LogLineOrderByDirection
        :param _request_timeout: timeout setting for this request. If one
                                 number provided, it will be total request
                                 timeout. It can also be a pair (tuple) of
                                 (connection, read) timeouts.
        :type _request_timeout: int, tuple(int, int), optional
        :param _request_auth: set to override the auth_settings for an a single
                              request; this effectively ignores the
                              authentication in the spec for a single request.
        :type _request_auth: dict, optional
        :param _content_type: force content-type for the request.
        :type _content_type: str, Optional
        :param _headers: set to override the headers for a single
                         request; this effectively ignores the headers
                         in the spec for a single request.
        :type _headers: dict, optional
        :param _host_index: set to override the host_index for a single
                            request; this effectively ignores the host_index
                            in the spec for a single request.
        :type _host_index: int, optional
        :return: Returns the result object.
        """  # noqa: E501

        _param = self._log_line_list_serialize(
            step_run=step_run,
            offset=offset,
            limit=limit,
            levels=levels,
            search=search,
            order_by_field=order_by_field,
            order_by_direction=order_by_direction,
            _request_auth=_request_auth,
            _content_type=_content_type,
            _headers=_headers,
            _host_index=_host_index,
        )

        _response_types_map: Dict[str, Optional[str]] = {
            "200": "LogLineList",
            "400": "APIErrors",
            "403": "APIErrors",
        }
        response_data = await self.api_client.call_api(
            *_param, _request_timeout=_request_timeout
        )
        return response_data.response

    def _log_line_list_serialize(
        self,
        step_run,
        offset,
        limit,
        levels,
        search,
        order_by_field,
        order_by_direction,
        _request_auth,
        _content_type,
        _headers,
        _host_index,
    ) -> RequestSerialized:

        _host = None

        _collection_formats: Dict[str, str] = {
            "levels": "multi",
        }

        _path_params: Dict[str, str] = {}
        _query_params: List[Tuple[str, str]] = []
        _header_params: Dict[str, Optional[str]] = _headers or {}
        _form_params: List[Tuple[str, str]] = []
        _files: Dict[
            str, Union[str, bytes, List[str], List[bytes], List[Tuple[str, bytes]]]
        ] = {}
        _body_params: Optional[bytes] = None

        # process the path parameters
        if step_run is not None:
            _path_params["step-run"] = step_run
        # process the query parameters
        if offset is not None:

            _query_params.append(("offset", offset))

        if limit is not None:

            _query_params.append(("limit", limit))

        if levels is not None:

            _query_params.append(("levels", levels))

        if search is not None:

            _query_params.append(("search", search))

        if order_by_field is not None:

            _query_params.append(("orderByField", order_by_field.value))

        if order_by_direction is not None:

            _query_params.append(("orderByDirection", order_by_direction.value))

        # process the header parameters
        # process the form parameters
        # process the body parameter

        # set the HTTP header `Accept`
        _header_params["Accept"] = self.api_client.select_header_accept(
            ["application/json"]
        )

        # authentication setting
        _auth_settings: List[str] = ["cookieAuth", "bearerAuth"]

        return self.api_client.param_serialize(
            method="GET",
            resource_path="/api/v1/step-runs/{step-run}/logs",
            path_params=_path_params,
            query_params=_query_params,
            header_params=_header_params,
            body=_body_params,
            post_params=_form_params,
            files=_files,
            auth_settings=_auth_settings,
            collection_formats=_collection_formats,
            _host=_host,
            _request_auth=_request_auth,
        )
