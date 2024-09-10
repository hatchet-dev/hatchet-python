import os
from logging import Logger, getLogger
from typing import Dict, Optional

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
    logger: Logger

    def __init__(
        self,
        tenant_id: str = None,
        tls_config: ClientTLSConfig = None,
        token: str = None,
        host_port: str = "localhost:7070",
        server_url: str = "https://app.dev.hatchet-tools.com",
        namespace: str = None,
        listener_v2_timeout: int = None,
        logger: Logger = None,
        grpc_max_recv_message_length: int = 4 * 1024 * 1024,  # 4MB
        grpc_max_send_message_length: int = 4 * 1024 * 1024,  # 4MB
    ):
        self.tenant_id = tenant_id
        self.tls_config = tls_config
        self.host_port = host_port
        self.token = token
        self.server_url = server_url
        self.namespace = ""
        self.logger = logger
        self.grpc_max_recv_message_length = grpc_max_recv_message_length
        self.grpc_max_send_message_length = grpc_max_send_message_length

        if not self.logger:
            self.logger = getLogger()

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
        config_data: object = {"tls": {}}

        # determine if client.yaml exists
        if os.path.exists(config_file_path):
            with open(config_file_path, "r") as file:
                config_data = yaml.safe_load(file)

        def get_config_value(key, env_var):
            if key in config_data:
                return config_data[key]

            if self._get_env_var(env_var) is not None:
                return self._get_env_var(env_var)

            return getattr(defaults, key, None)

        namespace = get_config_value("namespace", "HATCHET_CLIENT_NAMESPACE")

        tenant_id = get_config_value("tenantId", "HATCHET_CLIENT_TENANT_ID")
        token = get_config_value("token", "HATCHET_CLIENT_TOKEN")
        listener_v2_timeout = get_config_value(
            "listener_v2_timeout", "HATCHET_CLIENT_LISTENER_V2_TIMEOUT"
        )
        listener_v2_timeout = int(listener_v2_timeout) if listener_v2_timeout else None

        if not token:
            raise ValueError(
                "Token must be set via HATCHET_CLIENT_TOKEN environment variable"
            )

        host_port = get_config_value("hostPort", "HATCHET_CLIENT_HOST_PORT")
        server_url: str | None = None

        grpc_max_recv_message_length = get_config_value(
            "grpc_max_recv_message_length",
            "HATCHET_CLIENT_GRPC_MAX_RECV_MESSAGE_LENGTH",
        )
        grpc_max_send_message_length = get_config_value(
            "grpc_max_send_message_length",
            "HATCHET_CLIENT_GRPC_MAX_SEND_MESSAGE_LENGTH",
        )

        if not host_port:
            # extract host and port from token
            server_url, grpc_broadcast_address = get_addresses_from_jwt(token)
            host_port = grpc_broadcast_address

        if not tenant_id:
            tenant_id = get_tenant_id_from_jwt(token)

        tls_config = self._load_tls_config(config_data["tls"], host_port)

        return ClientConfig(
            tenant_id=tenant_id,
            tls_config=tls_config,
            token=token,
            host_port=host_port,
            server_url=server_url,
            namespace=namespace,
            listener_v2_timeout=listener_v2_timeout,
            logger=defaults.logger,
            grpc_max_recv_message_length=grpc_max_recv_message_length,
            grpc_max_send_message_length=grpc_max_send_message_length,
        )

    def _load_tls_config(self, tls_data: Dict, host_port) -> ClientTLSConfig:
        tls_strategy = (
            tls_data["tlsStrategy"]
            if "tlsStrategy" in tls_data
            else self._get_env_var("HATCHET_CLIENT_TLS_STRATEGY")
        )

        if not tls_strategy:
            tls_strategy = "tls"

        cert_file = (
            tls_data["tlsCertFile"]
            if "tlsCertFile" in tls_data
            else self._get_env_var("HATCHET_CLIENT_TLS_CERT_FILE")
        )
        key_file = (
            tls_data["tlsKeyFile"]
            if "tlsKeyFile" in tls_data
            else self._get_env_var("HATCHET_CLIENT_TLS_KEY_FILE")
        )
        ca_file = (
            tls_data["tlsRootCAFile"]
            if "tlsRootCAFile" in tls_data
            else self._get_env_var("HATCHET_CLIENT_TLS_ROOT_CA_FILE")
        )

        server_name = (
            tls_data["tlsServerName"]
            if "tlsServerName" in tls_data
            else self._get_env_var("HATCHET_CLIENT_TLS_SERVER_NAME")
        )

        # if server_name is not set, use the host from the host_port
        if not server_name:
            server_name = host_port.split(":")[0]

        return ClientTLSConfig(tls_strategy, cert_file, key_file, ca_file, server_name)

    @staticmethod
    def _get_env_var(env_var: str, default: Optional[str] = None) -> str:
        return os.environ.get(env_var, default)
