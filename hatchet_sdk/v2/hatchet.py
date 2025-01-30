import asyncio
import logging
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Optional,
    ParamSpec,
    Type,
    TypeVar,
    Union,
)

from pydantic import BaseModel, ConfigDict
from typing_extensions import deprecated

from hatchet_sdk.clients.rest_client import RestApi
from hatchet_sdk.context.context import Context
from hatchet_sdk.contracts.workflows_pb2 import (
    ConcurrencyLimitStrategy,
    CreateStepRateLimit,
    CreateWorkflowJobOpts,
    CreateWorkflowStepOpts,
    CreateWorkflowVersionOpts,
    DesiredWorkerLabels,
    StickyStrategy,
    WorkflowConcurrencyOpts,
    WorkflowKind,
)
from hatchet_sdk.features.cron import CronClient
from hatchet_sdk.features.scheduled import ScheduledClient
from hatchet_sdk.labels import DesiredWorkerLabel
from hatchet_sdk.loader import ClientConfig
from hatchet_sdk.rate_limit import RateLimit
from hatchet_sdk.v2.workflows import Step, StepType
from hatchet_sdk.worker.worker import Worker

from ..client import Client, new_client, new_client_raw
from ..clients.admin import AdminClient
from ..clients.dispatcher.dispatcher import DispatcherClient
from ..clients.events import EventClient
from ..clients.run_event_listener import RunEventListenerClient
from ..logger import logger

R = TypeVar("R")


class HatchetRest:
    """
    Main client for interacting with the Hatchet API.

    This class provides access to various client interfaces and utility methods
    for working with Hatchet via the REST API,

    Attributes:
        rest (RestApi): Interface for REST API operations.
    """

    def __init__(self, config: ClientConfig = ClientConfig()):
        self.rest = RestApi(config.server_url, config.token, config.tenant_id)


def transform_desired_worker_label(d: DesiredWorkerLabel) -> DesiredWorkerLabels:
    value = d.value
    return DesiredWorkerLabels(
        strValue=value if not isinstance(value, int) else None,
        intValue=value if isinstance(value, int) else None,
        required=d.required,
        weight=d.weight,
        comparator=d.comparator,  # type: ignore[arg-type]
    )


class Hatchet:
    """
    Main client for interacting with the Hatchet SDK.

    This class provides access to various client interfaces and utility methods
    for working with Hatchet workers, workflows, and steps.

    Attributes:
        cron (CronClient): Interface for cron trigger operations.

        admin (AdminClient): Interface for administrative operations.
        dispatcher (DispatcherClient): Interface for dispatching operations.
        event (EventClient): Interface for event-related operations.
        rest (RestApi): Interface for REST API operations.
    """

    _client: Client
    cron: CronClient
    scheduled: ScheduledClient

    @classmethod
    def from_environment(
        cls, defaults: ClientConfig = ClientConfig(), **kwargs: Any
    ) -> "Hatchet":
        return cls(client=new_client(defaults), **kwargs)

    @classmethod
    def from_config(cls, config: ClientConfig, **kwargs: Any) -> "Hatchet":
        return cls(client=new_client_raw(config), **kwargs)

    def __init__(
        self,
        debug: bool = False,
        client: Optional[Client] = None,
        config: ClientConfig = ClientConfig(),
    ):
        """
        Initialize a new Hatchet instance.

        Args:
            debug (bool, optional): Enable debug logging. Defaults to False.
            client (Optional[Client], optional): A pre-configured Client instance. Defaults to None.
            config (ClientConfig, optional): Configuration for creating a new Client. Defaults to ClientConfig().
        """
        if client is not None:
            self._client = client
        else:
            self._client = new_client(config, debug)

        if debug:
            logger.setLevel(logging.DEBUG)

        self.cron = CronClient(self._client)
        self.scheduled = ScheduledClient(self._client)

    @property
    @deprecated(
        "Direct access to client is deprecated and will be removed in a future version. Use specific client properties (Hatchet.admin, Hatchet.dispatcher, Hatchet.event, Hatchet.rest) instead. [0.32.0]",
    )
    def client(self) -> Client:
        return self._client

    @property
    def admin(self) -> AdminClient:
        return self._client.admin

    @property
    def dispatcher(self) -> DispatcherClient:
        return self._client.dispatcher

    @property
    def event(self) -> EventClient:
        return self._client.event

    @property
    def rest(self) -> RestApi:
        return self._client.rest

    @property
    def listener(self) -> RunEventListenerClient:
        return self._client.listener

    @property
    def config(self) -> ClientConfig:
        return self._client.config

    @property
    def tenant_id(self) -> str:
        return self._client.config.tenant_id

    def step(
        self,
        name: str = "",
        timeout: str = "60m",
        parents: list[str] = [],
        retries: int = 0,
        rate_limits: list[RateLimit] = [],
        desired_worker_labels: dict[str, DesiredWorkerLabel] = {},
        backoff_factor: float | None = None,
        backoff_max_seconds: int | None = None,
    ) -> Callable[[Callable[[Any, Context], Any]], Step[R]]:
        def inner(func: Callable[[Any, Context], R]) -> Step[R]:
            return Step(
                fn=func,
                type=StepType.DEFAULT,
                name=name.lower() or str(func.__name__).lower(),
                timeout=timeout,
                parents=parents,
                retries=retries,
                rate_limits=[r for rate_limit in rate_limits if (r := rate_limit._req)],
                desired_worker_labels={
                    key: transform_desired_worker_label(d)
                    for key, d in desired_worker_labels.items()
                },
                backoff_factor=backoff_factor,
                backoff_max_seconds=backoff_max_seconds,
            )

        return inner

    def on_failure_step(
        self,
        name: str = "",
        timeout: str = "60m",
        parents: list[str] = [],
        retries: int = 0,
        rate_limits: list[RateLimit] = [],
        desired_worker_labels: dict[str, DesiredWorkerLabel] = {},
        backoff_factor: float | None = None,
        backoff_max_seconds: int | None = None,
    ) -> Callable[[Callable[[Any, Context], Any]], Step[R]]:
        def inner(func: Callable[[Any, Context], R]) -> Step[R]:
            return Step(
                fn=func,  # The Step class will handle the self parameter
                type=StepType.ON_FAILURE,
                name=name.lower() or str(func.__name__).lower(),
                timeout=timeout,
                parents=parents,
                retries=retries,
                rate_limits=[r for rate_limit in rate_limits if (r := rate_limit._req)],
                desired_worker_labels={
                    key: transform_desired_worker_label(d)
                    for key, d in desired_worker_labels.items()
                },
                backoff_factor=backoff_factor,
                backoff_max_seconds=backoff_max_seconds,
            )

        return inner

    def worker(
        self, name: str, max_runs: int | None = None, labels: dict[str, str | int] = {}
    ) -> Worker:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        return Worker(
            name=name,
            max_runs=max_runs,
            labels=labels,
            config=self._client.config,
            debug=self._client.debug,
            owned_loop=loop is None,
        )
