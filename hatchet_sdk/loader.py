import os
from logging import Logger, getLogger
from typing import Any, Optional, cast

import yaml

from .token import get_addresses_from_jwt, get_tenant_id_from_jwt


class ClientTLSConfig:
    def __init__(
        self,
        tls_strategy: str,
        cert_file: str,
        key_file: str,
        ca_file: str,
        server_name: str,
    ):
        self.tls_strategy = tls_strategy
        self.cert_file = cert_file
        self.key_file = key_file
        self.ca_file = ca_file
        self.server_name = server_name


class ClientConfig:
    def __init__(
        self,
        tenant_id: str | None = None,
        tls_config: ClientTLSConfig | None = None,
        token: str | None = None,
        host_port: str = "localhost:7070",
        server_url: str = "https://app.dev.hatchet-tools.com",
        namespace: str | None = None,
        listener_v2_timeout: int | None = None,
        logger: Logger | None = None,
        grpc_max_recv_message_length: int = 4 * 1024 * 1024,  # 4MB
        grpc_max_send_message_length: int = 4 * 1024 * 1024,  # 4MB
        otel_exporter_oltp_endpoint: str | None = None,
        otel_service_name: str | None = None,
        otel_exporter_oltp_headers: dict[str, str] | None = None,
        otel_exporter_oltp_protocol: str | None = None,
        worker_healthcheck_port: int | None = None,
        worker_healthcheck_enabled: bool | None = None,
    ):
        self.tenant_id = tenant_id
        self.tls_config = tls_config
        self.host_port = host_port
        self.token = token
        self.server_url = server_url
        self.namespace = ""
        self.logInterceptor = logger
        self.grpc_max_recv_message_length = grpc_max_recv_message_length
        self.grpc_max_send_message_length = grpc_max_send_message_length
        self.otel_exporter_oltp_endpoint = otel_exporter_oltp_endpoint
        self.otel_service_name = otel_service_name
        self.otel_exporter_oltp_headers = otel_exporter_oltp_headers
        self.otel_exporter_oltp_protocol = otel_exporter_oltp_protocol
        self.worker_healthcheck_port = worker_healthcheck_port
        self.worker_healthcheck_enabled = worker_healthcheck_enabled

        if not self.logInterceptor:
            self.logInterceptor = getLogger()

        # case on whether the namespace already has a trailing underscore
        if namespace and not namespace.endswith("_"):
            self.namespace = f"{namespace}_"
        elif namespace:
            self.namespace = namespace

        self.namespace = self.namespace.lower()

        self.listener_v2_timeout = listener_v2_timeout


class ConfigLoader:
    def __init__(self, directory: str):
        self.directory = directory

    def load_client_config(self, defaults: ClientConfig) -> ClientConfig:
        config_file_path = os.path.join(self.directory, "client.yaml")
        config_data: dict[str, Any] = {"tls": {}}

        # determine if client.yaml exists
        if os.path.exists(config_file_path):
            with open(config_file_path, "r") as file:
                config_data = yaml.safe_load(file)

        def get_config_value(key: str, env_var: str) -> str | None:
            if key in config_data:
                return config_data[key]

            if self._get_env_var(env_var) is not None:
                return self._get_env_var(env_var)

            return getattr(defaults, key, None)

        namespace = get_config_value("namespace", "HATCHET_CLIENT_NAMESPACE")

        tenant_id = get_config_value("tenantId", "HATCHET_CLIENT_TENANT_ID")
        token = get_config_value("token", "HATCHET_CLIENT_TOKEN")
        _listener_v2_timeout = get_config_value(
            "listener_v2_timeout", "HATCHET_CLIENT_LISTENER_V2_TIMEOUT"
        )
        listener_v2_timeout = (
            int(_listener_v2_timeout) if _listener_v2_timeout else None
        )

        if not token:
            raise ValueError(
                "Token must be set via HATCHET_CLIENT_TOKEN environment variable"
            )

        host_port = get_config_value("hostPort", "HATCHET_CLIENT_HOST_PORT")
        server_url: str | None = None

        grpc_max_recv_message_length = (
            int(_grpc_max_recv_message_length)
            if (
                _grpc_max_recv_message_length := get_config_value(
                    "grpc_max_recv_message_length",
                    "HATCHET_CLIENT_GRPC_MAX_RECV_MESSAGE_LENGTH",
                )
            )
            else None
        )

        grpc_max_send_message_length = (
            int(_grpc_max_send_message_length)
            if (
                _grpc_max_send_message_length := get_config_value(
                    "grpc_max_send_message_length",
                    "HATCHET_CLIENT_GRPC_MAX_SEND_MESSAGE_LENGTH",
                )
            )
            else None
        )

        if not host_port:
            # extract host and port from token
            server_url, grpc_broadcast_address = get_addresses_from_jwt(token)
            host_port = grpc_broadcast_address

        if not tenant_id:
            tenant_id = get_tenant_id_from_jwt(token)

        tls_config = self._load_tls_config(config_data["tls"], host_port)

        otel_exporter_oltp_endpoint = get_config_value(
            "otel_exporter_oltp_endpoint", "HATCHET_CLIENT_OTEL_EXPORTER_OTLP_ENDPOINT"
        )

        otel_service_name = get_config_value(
            "otel_service_name", "HATCHET_CLIENT_OTEL_SERVICE_NAME"
        )

        _oltp_headers = get_config_value(
            "otel_exporter_oltp_headers", "HATCHET_CLIENT_OTEL_EXPORTER_OTLP_HEADERS"
        )

        if _oltp_headers:
            try:
                otel_header_key, api_key = _oltp_headers.split("=", maxsplit=1)
                otel_exporter_oltp_headers = {otel_header_key: api_key}
            except ValueError:
                raise ValueError(
                    "HATCHET_CLIENT_OTEL_EXPORTER_OTLP_HEADERS must be in the format `key=value`"
                )
        else:
            otel_exporter_oltp_headers = None

        otel_exporter_oltp_protocol = get_config_value(
            "otel_exporter_oltp_protocol", "HATCHET_CLIENT_OTEL_EXPORTER_OTLP_PROTOCOL"
        )

        worker_healthcheck_port = int(
            get_config_value(
                "worker_healthcheck_port", "HATCHET_CLIENT_WORKER_HEALTHCHECK_PORT"
            )
            or 8001
        )

        worker_healthcheck_enabled = (
            str(
                get_config_value(
                    "worker_healthcheck_port",
                    "HATCHET_CLIENT_WORKER_HEALTHCHECK_ENABLED",
                )
            )
            == "True"
        )

        return ClientConfig(
            tenant_id=tenant_id,
            tls_config=tls_config,
            token=token,
            host_port=host_port,
            server_url=server_url,
            namespace=namespace,
            listener_v2_timeout=listener_v2_timeout,
            logger=defaults.logInterceptor,
            grpc_max_recv_message_length=grpc_max_recv_message_length,
            grpc_max_send_message_length=grpc_max_send_message_length,
            otel_exporter_oltp_endpoint=otel_exporter_oltp_endpoint,
            otel_service_name=otel_service_name,
            otel_exporter_oltp_headers=otel_exporter_oltp_headers,
            otel_exporter_oltp_protocol=otel_exporter_oltp_protocol,
            worker_healthcheck_port=worker_healthcheck_port,
            worker_healthcheck_enabled=worker_healthcheck_enabled,
        )

    def _load_tls_config(
        self, tls_data: dict[str, Any], host_port: str
    ) -> ClientTLSConfig:
        tls_strategy = (
            cast(str | None, tls_data.get("tlsStrategy"))
            or self._get_env_var("HATCHET_CLIENT_TLS_STRATEGY")
            or "tls"
        )

        cert_file = tls_data.get("tlsCertFile") or self._get_env_var(
            "HATCHET_CLIENT_TLS_CERT_FILE"
        )
        key_file = tls_data.get("tlsKeyFile") or self._get_env_var(
            "HATCHET_CLIENT_TLS_KEY_FILE"
        )
        ca_file = tls_data.get("tlsRootCAFile") or self._get_env_var(
            "HATCHET_CLIENT_TLS_ROOT_CA_FILE"
        )

        server_name = tls_data.get("tlsServerName") or self._get_env_var(
            "HATCHET_CLIENT_TLS_SERVER_NAME"
        )

        # if server_name is not set, use the host from the host_port
        if not server_name:
            server_name = host_port.split(":")[0]

        return ClientTLSConfig(tls_strategy, cert_file, key_file, ca_file, server_name)

    @staticmethod
    def _get_env_var(env_var: str, default: Optional[str] = None) -> str:
        return os.environ.get(env_var, default)
