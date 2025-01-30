import json
from logging import Logger, getLogger

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator
from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from hatchet_sdk.token import get_addresses_from_jwt, get_tenant_id_from_jwt


class ClientTLSConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="HATCHET_CLIENT_TLS_",
        env_file=(".env", ".env.hatchet", ".env.dev", ".env.local"),
    )

    strategy: str = "tls"
    cert_file: str | None = None
    key_file: str | None = None
    ca_file: str | None = Field(default=None, alias="root_ca_file")
    server_name: str = "localhost"


DEFAULT_HOST_PORT = "localhost:7070"


class ClientConfig(BaseSettings):
    model_config = SettingsConfigDict(
        arbitrary_types_allowed=True,
        extra="allow",
        env_file=(".env", ".env.hatchet", ".env.dev", ".env.local"),
        env_prefix="HATCHET_CLIENT_",
    )

    token: str = ""
    logger: Logger = getLogger()

    tenant_id: str = ""
    host_port: str = DEFAULT_HOST_PORT
    server_url: str = "https://app.dev.hatchet-tools.com"
    namespace: str = ""

    tls_config: ClientTLSConfig = Field(default_factory=lambda: ClientTLSConfig())

    listener_v2_timeout: int | None = None
    grpc_max_recv_message_length: int = Field(
        default=4 * 1024 * 1024, description="4MB default"
    )
    grpc_max_send_message_length: int = Field(
        default=4 * 1024 * 1024, description="4MB default"
    )
    otel_exporter_oltp_endpoint: str | None = None
    otel_service_name: str | None = None
    otel_exporter_oltp_headers: str | None = None
    otel_exporter_oltp_protocol: str | None = None
    worker_healthcheck_port: int = 8001
    worker_healthcheck_enabled: bool = False

    worker_preset_labels: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_token_and_tenant(self) -> "ClientConfig":
        if not self.token:
            raise ValueError("Token must be set")

        if not self.tenant_id:
            self.tenant_id = get_tenant_id_from_jwt(self.token)

        return self

    @model_validator(mode="after")
    def validate_addresses(self) -> "ClientConfig":
        if self.host_port == DEFAULT_HOST_PORT:
            server_url, grpc_broadcast_address = get_addresses_from_jwt(self.token)
            self.host_port = grpc_broadcast_address
            self.server_url = server_url

        self.tls_config.server_name = self.host_port.split(":")[0]
        return self

    @field_validator("listener_v2_timeout")
    @classmethod
    def validate_listener_timeout(cls, value: int | None | str) -> int | None:
        if value is None:
            return None

        if isinstance(value, int):
            return value

        return int(value)

    @field_validator("namespace")
    @classmethod
    def validate_namespace(cls, namespace: str) -> str:
        if not namespace:
            return ""
        if not namespace.endswith("_"):
            namespace = f"{namespace}_"
        return namespace.lower()

    def __hash__(self) -> int:
        return hash(json.dumps(self.model_dump(), default=str))
