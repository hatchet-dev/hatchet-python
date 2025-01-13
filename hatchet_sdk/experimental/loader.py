import os
from logging import Logger, getLogger
from typing import cast

from pydantic import BaseModel, ValidationError, ValidationInfo, field_validator

from .token import get_tenant_id_from_jwt


class ClientTLSConfig(BaseModel):
    tls_strategy: str
    cert_file: str | None
    key_file: str | None
    ca_file: str | None
    server_name: str


def _load_tls_config(host_port: str) -> ClientTLSConfig:
    tls_strategy = os.getenv("HATCHET_CLIENT_TLS_STRATEGY", "tls")
    cert_file = os.getenv("HATCHET_CLIENT_TLS_CERT_FILE")
    key_file = os.getenv("HATCHET_CLIENT_TLS_KEY_FILE")
    ca_file = os.getenv("HATCHET_CLIENT_TLS_ROOT_CA_FILE")
    server_name = os.getenv("HATCHET_CLIENT_TLS_SERVER_NAME", host_port.split(":")[0])

    return ClientTLSConfig(tls_strategy, cert_file, key_file, ca_file, server_name)


class ClientConfig(BaseModel):
    token: str = os.getenv("HATCHET_CLIENT_TOKEN")
    logger: Logger = getLogger()
    tenant_id: str = os.getenv("HATCHET_CLIENT_TENANT_ID", "")
    host_port: str = os.getenv("HATCHET_CLIENT_HOST_PORT", "localhost:7070")
    tls_config: ClientTLSConfig = _load_tls_config(host_port)
    server_url: str = "https://app.dev.hatchet-tools.com"
    namespace: str = os.getenv("HATCHET_CLIENT_NAMESPACE", "")
    listener_v2_timeout: int | None = (
        int(x) if (x := os.getenv("HATCHET_CLIENT_LISTENER_V2_TIMEOUT")) else None
    )
    grpc_max_recv_message_length: int = int(
        os.getenv("HATCHET_CLIENT_GRPC_MAX_RECV_MESSAGE_LENGTH", 4 * 1024 * 1024)
    )  # 4MB
    grpc_max_send_message_length: int = int(
        os.getenv("HATCHET_CLIENT_GRPC_MAX_SEND_MESSAGE_LENGTH", 4 * 1024 * 1024)
    )  # 4MB
    otel_exporter_oltp_endpoint: str | None = os.getenv(
        "HATCHET_CLIENT_OTEL_EXPORTER_OTLP_ENDPOINT"
    )
    otel_service_name: str | None = os.getenv("HATCHET_CLIENT_OTEL_SERVICE_NAME")
    otel_exporter_oltp_headers: str | None = (
        os.getenv("HATCHET_CLIENT_OTEL_EXPORTER_OTLP_HEADERS"),
    )
    otel_exporter_oltp_protocol: str | None = os.getenv(
        "HATCHET_CLIENT_OTEL_EXPORTER_OTLP_PROTOCOL"
    )
    worker_healthcheck_port: int = int(
        os.getenv("HATCHET_CLIENT_WORKER_HEALTHCHECK_PORT", 8001)
    )
    worker_healthcheck_enabled: bool = (
        os.getenv("HATCHET_CLIENT_WORKER_HEALTHCHECK_ENABLED", "False") == "True"
    )

    @field_validator(mode="after")
    @classmethod
    def validate_namespace(cls, namespace: str | None) -> str:
        if not namespace.endswith("_"):
            namespace = f"{namespace}_"

        return namespace.lower()

    @field_validator(mode="after")
    @classmethod
    def validate_tenant_id(cls, tenant_id: str, info: ValidationInfo) -> str:
        token = cast(str | None, info.data.get("token"))

        if not tenant_id:
            if not token:
                raise ValidationError(
                    "Token must be set before attempting to infer tenant ID"
                )

            return get_tenant_id_from_jwt(token)

        return tenant_id

    ## TODO: Fix host port overrides here
    ## Old code:
    ## if not host_port:
    ## ## extract host and port from token
    ## server_url, grpc_broadcast_address = get_addresses_from_jwt(token)
    ## host_port = grpc_broadcast_address
