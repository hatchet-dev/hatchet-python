# coding: utf-8

"""
    Hatchet API

    The Hatchet API

    The version of the OpenAPI document: 1.0.0
    Generated by OpenAPI Generator (https://openapi-generator.tech)

    Do not edit the class manually.
"""  # noqa: E501

import warnings
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

from pydantic import Field, StrictFloat, StrictInt, StrictStr, validate_call
from typing_extensions import Annotated

from hatchet_sdk.clients.cloud_rest.api_client import ApiClient, RequestSerialized
from hatchet_sdk.clients.cloud_rest.api_response import ApiResponse
from hatchet_sdk.clients.cloud_rest.models.sample_stream import SampleStream
from hatchet_sdk.clients.cloud_rest.rest import RESTResponseType


class MetricsApi:
    """NOTE: This class is auto generated by OpenAPI Generator
    Ref: https://openapi-generator.tech

    Do not edit the class manually.
    """

    def __init__(self, api_client=None) -> None:
        if api_client is None:
            api_client = ApiClient.get_default()
        self.api_client = api_client

    @validate_call
    async def metrics_cpu_get(
        self,
        managed_worker: Annotated[
            str,
            Field(
                min_length=36,
                strict=True,
                max_length=36,
                description="The managed worker id",
            ),
        ],
        after: Annotated[
            Optional[datetime], Field(description="When the metrics should start")
        ] = None,
        before: Annotated[
            Optional[datetime], Field(description="When the metrics should end")
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
    ) -> List[SampleStream]:
        """Get CPU Metrics

        Get CPU metrics for a managed worker

        :param managed_worker: The managed worker id (required)
        :type managed_worker: str
        :param after: When the metrics should start
        :type after: datetime
        :param before: When the metrics should end
        :type before: datetime
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

        _param = self._metrics_cpu_get_serialize(
            managed_worker=managed_worker,
            after=after,
            before=before,
            _request_auth=_request_auth,
            _content_type=_content_type,
            _headers=_headers,
            _host_index=_host_index,
        )

        _response_types_map: Dict[str, Optional[str]] = {
            "200": "List[SampleStream]",
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
    async def metrics_cpu_get_with_http_info(
        self,
        managed_worker: Annotated[
            str,
            Field(
                min_length=36,
                strict=True,
                max_length=36,
                description="The managed worker id",
            ),
        ],
        after: Annotated[
            Optional[datetime], Field(description="When the metrics should start")
        ] = None,
        before: Annotated[
            Optional[datetime], Field(description="When the metrics should end")
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
    ) -> ApiResponse[List[SampleStream]]:
        """Get CPU Metrics

        Get CPU metrics for a managed worker

        :param managed_worker: The managed worker id (required)
        :type managed_worker: str
        :param after: When the metrics should start
        :type after: datetime
        :param before: When the metrics should end
        :type before: datetime
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

        _param = self._metrics_cpu_get_serialize(
            managed_worker=managed_worker,
            after=after,
            before=before,
            _request_auth=_request_auth,
            _content_type=_content_type,
            _headers=_headers,
            _host_index=_host_index,
        )

        _response_types_map: Dict[str, Optional[str]] = {
            "200": "List[SampleStream]",
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
    async def metrics_cpu_get_without_preload_content(
        self,
        managed_worker: Annotated[
            str,
            Field(
                min_length=36,
                strict=True,
                max_length=36,
                description="The managed worker id",
            ),
        ],
        after: Annotated[
            Optional[datetime], Field(description="When the metrics should start")
        ] = None,
        before: Annotated[
            Optional[datetime], Field(description="When the metrics should end")
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
        """Get CPU Metrics

        Get CPU metrics for a managed worker

        :param managed_worker: The managed worker id (required)
        :type managed_worker: str
        :param after: When the metrics should start
        :type after: datetime
        :param before: When the metrics should end
        :type before: datetime
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

        _param = self._metrics_cpu_get_serialize(
            managed_worker=managed_worker,
            after=after,
            before=before,
            _request_auth=_request_auth,
            _content_type=_content_type,
            _headers=_headers,
            _host_index=_host_index,
        )

        _response_types_map: Dict[str, Optional[str]] = {
            "200": "List[SampleStream]",
            "400": "APIErrors",
            "403": "APIErrors",
        }
        response_data = await self.api_client.call_api(
            *_param, _request_timeout=_request_timeout
        )
        return response_data.response

    def _metrics_cpu_get_serialize(
        self,
        managed_worker,
        after,
        before,
        _request_auth,
        _content_type,
        _headers,
        _host_index,
    ) -> RequestSerialized:

        _host = None

        _collection_formats: Dict[str, str] = {}

        _path_params: Dict[str, str] = {}
        _query_params: List[Tuple[str, str]] = []
        _header_params: Dict[str, Optional[str]] = _headers or {}
        _form_params: List[Tuple[str, str]] = []
        _files: Dict[str, Union[str, bytes]] = {}
        _body_params: Optional[bytes] = None

        # process the path parameters
        if managed_worker is not None:
            _path_params["managed-worker"] = managed_worker
        # process the query parameters
        if after is not None:
            if isinstance(after, datetime):
                _query_params.append(
                    (
                        "after",
                        after.strftime(self.api_client.configuration.datetime_format),
                    )
                )
            else:
                _query_params.append(("after", after))

        if before is not None:
            if isinstance(before, datetime):
                _query_params.append(
                    (
                        "before",
                        before.strftime(self.api_client.configuration.datetime_format),
                    )
                )
            else:
                _query_params.append(("before", before))

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
            resource_path="/api/v1/cloud/managed-worker/{managed-worker}/metrics/cpu",
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

    @validate_call
    async def metrics_disk_get(
        self,
        managed_worker: Annotated[
            str,
            Field(
                min_length=36,
                strict=True,
                max_length=36,
                description="The managed worker id",
            ),
        ],
        after: Annotated[
            Optional[datetime], Field(description="When the metrics should start")
        ] = None,
        before: Annotated[
            Optional[datetime], Field(description="When the metrics should end")
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
    ) -> List[SampleStream]:
        """Get Disk Metrics

        Get disk metrics for a managed worker

        :param managed_worker: The managed worker id (required)
        :type managed_worker: str
        :param after: When the metrics should start
        :type after: datetime
        :param before: When the metrics should end
        :type before: datetime
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

        _param = self._metrics_disk_get_serialize(
            managed_worker=managed_worker,
            after=after,
            before=before,
            _request_auth=_request_auth,
            _content_type=_content_type,
            _headers=_headers,
            _host_index=_host_index,
        )

        _response_types_map: Dict[str, Optional[str]] = {
            "200": "List[SampleStream]",
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
    async def metrics_disk_get_with_http_info(
        self,
        managed_worker: Annotated[
            str,
            Field(
                min_length=36,
                strict=True,
                max_length=36,
                description="The managed worker id",
            ),
        ],
        after: Annotated[
            Optional[datetime], Field(description="When the metrics should start")
        ] = None,
        before: Annotated[
            Optional[datetime], Field(description="When the metrics should end")
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
    ) -> ApiResponse[List[SampleStream]]:
        """Get Disk Metrics

        Get disk metrics for a managed worker

        :param managed_worker: The managed worker id (required)
        :type managed_worker: str
        :param after: When the metrics should start
        :type after: datetime
        :param before: When the metrics should end
        :type before: datetime
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

        _param = self._metrics_disk_get_serialize(
            managed_worker=managed_worker,
            after=after,
            before=before,
            _request_auth=_request_auth,
            _content_type=_content_type,
            _headers=_headers,
            _host_index=_host_index,
        )

        _response_types_map: Dict[str, Optional[str]] = {
            "200": "List[SampleStream]",
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
    async def metrics_disk_get_without_preload_content(
        self,
        managed_worker: Annotated[
            str,
            Field(
                min_length=36,
                strict=True,
                max_length=36,
                description="The managed worker id",
            ),
        ],
        after: Annotated[
            Optional[datetime], Field(description="When the metrics should start")
        ] = None,
        before: Annotated[
            Optional[datetime], Field(description="When the metrics should end")
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
        """Get Disk Metrics

        Get disk metrics for a managed worker

        :param managed_worker: The managed worker id (required)
        :type managed_worker: str
        :param after: When the metrics should start
        :type after: datetime
        :param before: When the metrics should end
        :type before: datetime
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

        _param = self._metrics_disk_get_serialize(
            managed_worker=managed_worker,
            after=after,
            before=before,
            _request_auth=_request_auth,
            _content_type=_content_type,
            _headers=_headers,
            _host_index=_host_index,
        )

        _response_types_map: Dict[str, Optional[str]] = {
            "200": "List[SampleStream]",
            "400": "APIErrors",
            "403": "APIErrors",
        }
        response_data = await self.api_client.call_api(
            *_param, _request_timeout=_request_timeout
        )
        return response_data.response

    def _metrics_disk_get_serialize(
        self,
        managed_worker,
        after,
        before,
        _request_auth,
        _content_type,
        _headers,
        _host_index,
    ) -> RequestSerialized:

        _host = None

        _collection_formats: Dict[str, str] = {}

        _path_params: Dict[str, str] = {}
        _query_params: List[Tuple[str, str]] = []
        _header_params: Dict[str, Optional[str]] = _headers or {}
        _form_params: List[Tuple[str, str]] = []
        _files: Dict[str, Union[str, bytes]] = {}
        _body_params: Optional[bytes] = None

        # process the path parameters
        if managed_worker is not None:
            _path_params["managed-worker"] = managed_worker
        # process the query parameters
        if after is not None:
            if isinstance(after, datetime):
                _query_params.append(
                    (
                        "after",
                        after.strftime(self.api_client.configuration.datetime_format),
                    )
                )
            else:
                _query_params.append(("after", after))

        if before is not None:
            if isinstance(before, datetime):
                _query_params.append(
                    (
                        "before",
                        before.strftime(self.api_client.configuration.datetime_format),
                    )
                )
            else:
                _query_params.append(("before", before))

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
            resource_path="/api/v1/cloud/managed-worker/{managed-worker}/metrics/disk",
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

    @validate_call
    async def metrics_memory_get(
        self,
        managed_worker: Annotated[
            str,
            Field(
                min_length=36,
                strict=True,
                max_length=36,
                description="The managed worker id",
            ),
        ],
        after: Annotated[
            Optional[datetime], Field(description="When the metrics should start")
        ] = None,
        before: Annotated[
            Optional[datetime], Field(description="When the metrics should end")
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
    ) -> List[SampleStream]:
        """Get Memory Metrics

        Get memory metrics for a managed worker

        :param managed_worker: The managed worker id (required)
        :type managed_worker: str
        :param after: When the metrics should start
        :type after: datetime
        :param before: When the metrics should end
        :type before: datetime
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

        _param = self._metrics_memory_get_serialize(
            managed_worker=managed_worker,
            after=after,
            before=before,
            _request_auth=_request_auth,
            _content_type=_content_type,
            _headers=_headers,
            _host_index=_host_index,
        )

        _response_types_map: Dict[str, Optional[str]] = {
            "200": "List[SampleStream]",
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
    async def metrics_memory_get_with_http_info(
        self,
        managed_worker: Annotated[
            str,
            Field(
                min_length=36,
                strict=True,
                max_length=36,
                description="The managed worker id",
            ),
        ],
        after: Annotated[
            Optional[datetime], Field(description="When the metrics should start")
        ] = None,
        before: Annotated[
            Optional[datetime], Field(description="When the metrics should end")
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
    ) -> ApiResponse[List[SampleStream]]:
        """Get Memory Metrics

        Get memory metrics for a managed worker

        :param managed_worker: The managed worker id (required)
        :type managed_worker: str
        :param after: When the metrics should start
        :type after: datetime
        :param before: When the metrics should end
        :type before: datetime
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

        _param = self._metrics_memory_get_serialize(
            managed_worker=managed_worker,
            after=after,
            before=before,
            _request_auth=_request_auth,
            _content_type=_content_type,
            _headers=_headers,
            _host_index=_host_index,
        )

        _response_types_map: Dict[str, Optional[str]] = {
            "200": "List[SampleStream]",
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
    async def metrics_memory_get_without_preload_content(
        self,
        managed_worker: Annotated[
            str,
            Field(
                min_length=36,
                strict=True,
                max_length=36,
                description="The managed worker id",
            ),
        ],
        after: Annotated[
            Optional[datetime], Field(description="When the metrics should start")
        ] = None,
        before: Annotated[
            Optional[datetime], Field(description="When the metrics should end")
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
        """Get Memory Metrics

        Get memory metrics for a managed worker

        :param managed_worker: The managed worker id (required)
        :type managed_worker: str
        :param after: When the metrics should start
        :type after: datetime
        :param before: When the metrics should end
        :type before: datetime
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

        _param = self._metrics_memory_get_serialize(
            managed_worker=managed_worker,
            after=after,
            before=before,
            _request_auth=_request_auth,
            _content_type=_content_type,
            _headers=_headers,
            _host_index=_host_index,
        )

        _response_types_map: Dict[str, Optional[str]] = {
            "200": "List[SampleStream]",
            "400": "APIErrors",
            "403": "APIErrors",
        }
        response_data = await self.api_client.call_api(
            *_param, _request_timeout=_request_timeout
        )
        return response_data.response

    def _metrics_memory_get_serialize(
        self,
        managed_worker,
        after,
        before,
        _request_auth,
        _content_type,
        _headers,
        _host_index,
    ) -> RequestSerialized:

        _host = None

        _collection_formats: Dict[str, str] = {}

        _path_params: Dict[str, str] = {}
        _query_params: List[Tuple[str, str]] = []
        _header_params: Dict[str, Optional[str]] = _headers or {}
        _form_params: List[Tuple[str, str]] = []
        _files: Dict[str, Union[str, bytes]] = {}
        _body_params: Optional[bytes] = None

        # process the path parameters
        if managed_worker is not None:
            _path_params["managed-worker"] = managed_worker
        # process the query parameters
        if after is not None:
            if isinstance(after, datetime):
                _query_params.append(
                    (
                        "after",
                        after.strftime(self.api_client.configuration.datetime_format),
                    )
                )
            else:
                _query_params.append(("after", after))

        if before is not None:
            if isinstance(before, datetime):
                _query_params.append(
                    (
                        "before",
                        before.strftime(self.api_client.configuration.datetime_format),
                    )
                )
            else:
                _query_params.append(("before", before))

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
            resource_path="/api/v1/cloud/managed-worker/{managed-worker}/metrics/memory",
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
