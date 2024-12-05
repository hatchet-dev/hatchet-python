import asyncio
import logging
from typing import Any, Callable, Optional, ParamSpec, TypeVar

from typing_extensions import deprecated

from hatchet_sdk.clients.rest_client import RestApi

## TODO: These type stubs need to be updated to mass MyPy, and then we can remove this ignore
## There are file-level type ignore lines in the corresponding .pyi files.
from hatchet_sdk.contracts.workflows_pb2 import (  # type: ignore[attr-defined]
    ConcurrencyLimitStrategy,
    CreateStepRateLimit,
    DesiredWorkerLabels,
    StickyStrategy,
)
from hatchet_sdk.features.cron import CronClient
from hatchet_sdk.features.scheduled import ScheduledClient
from hatchet_sdk.labels import DesiredWorkerLabel
from hatchet_sdk.loader import ClientConfig, ConfigLoader
from hatchet_sdk.rate_limit import RateLimit

from .client import Client, new_client, new_client_raw
from .clients.admin import AdminClient
from .clients.dispatcher.dispatcher import DispatcherClient
from .clients.events import EventClient
from .clients.run_event_listener import RunEventListenerClient
from .logger import logger
from .worker.worker import Worker
from .workflow import ConcurrencyExpression, WorkflowMeta

P = ParamSpec("P")
R = TypeVar("R")


## TODO: Fix return type here to properly type hint the metaclass
def workflow(  # type: ignore[no-untyped-def]
    name: str = "",
    on_events: list[str] | None = None,
    on_crons: list[str] | None = None,
    version: str = "",
    timeout: str = "60m",
    schedule_timeout: str = "5m",
    sticky: StickyStrategy = None,
    default_priority: int | None = None,
    concurrency: ConcurrencyExpression | None = None,
):
    on_events = on_events or []
    on_crons = on_crons or []

    def inner(cls: Any) -> WorkflowMeta:
        cls.on_events = on_events
        cls.on_crons = on_crons
        cls.name = name or str(cls.__name__)
        cls.version = version
        cls.timeout = timeout
        cls.schedule_timeout = schedule_timeout
        cls.sticky = sticky
        cls.default_priority = default_priority
        cls.concurrency_expression = concurrency
        # Define a new class with the same name and bases as the original, but
        # with WorkflowMeta as its metaclass

        ## TODO: Figure out how to type this metaclass correctly
        return WorkflowMeta(cls.name, cls.__bases__, dict(cls.__dict__))  # type: ignore[no-untyped-call]

    return inner


def step(
    name: str = "",
    timeout: str = "",
    parents: list[str] | None = None,
    retries: int = 0,
    rate_limits: list[RateLimit] | None = None,
    desired_worker_labels: dict[str, DesiredWorkerLabel] = {},
    backoff_factor: float | None = None,
    backoff_max_seconds: int | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    parents = parents or []

    def inner(func: Callable[P, R]) -> Callable[P, R]:
        limits = None
        if rate_limits:
            limits = [
                CreateStepRateLimit(key=rate_limit.key, units=rate_limit.units)
                for rate_limit in rate_limits or []
            ]

        ## TODO: Use Protocol here to help with MyPy errors
        func._step_name = name.lower() or str(func.__name__).lower()  # type: ignore[attr-defined]
        func._step_parents = parents  # type: ignore[attr-defined]
        func._step_timeout = timeout  # type: ignore[attr-defined]
        func._step_retries = retries  # type: ignore[attr-defined]
        func._step_rate_limits = limits  # type: ignore[attr-defined]
        func._step_backoff_factor = backoff_factor  # type: ignore[attr-defined]
        func._step_backoff_max_seconds = backoff_max_seconds  # type: ignore[attr-defined]

        func._step_desired_worker_labels = {}  # type: ignore[attr-defined]

        for key, d in desired_worker_labels.items():
            value = d["value"] if "value" in d else None
            func._step_desired_worker_labels[key] = DesiredWorkerLabels(  # type: ignore[attr-defined]
                strValue=str(value) if not isinstance(value, int) else None,
                intValue=value if isinstance(value, int) else None,
                required=d["required"] if "required" in d else None,
                weight=d["weight"] if "weight" in d else None,
                comparator=d["comparator"] if "comparator" in d else None,
            )

        return func

    return inner


def on_failure_step(
    name: str = "",
    timeout: str = "",
    retries: int = 0,
    rate_limits: list[RateLimit] | None = None,
    backoff_factor: float | None = None,
    backoff_max_seconds: int | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def inner(func: Callable[P, R]) -> Callable[P, R]:
        limits = None
        if rate_limits:
            limits = [
                CreateStepRateLimit(key=rate_limit.key, units=rate_limit.units)
                for rate_limit in rate_limits or []
            ]

        ## TODO: Use Protocol here to help with MyPy errors
        func._on_failure_step_name = name.lower() or str(func.__name__).lower()  # type: ignore[attr-defined]
        func._on_failure_step_timeout = timeout  # type: ignore[attr-defined]
        func._on_failure_step_retries = retries  # type: ignore[attr-defined]
        func._on_failure_step_rate_limits = limits  # type: ignore[attr-defined]
        func._on_failure_step_backoff_factor = backoff_factor  # type: ignore[attr-defined]
        func._on_failure_step_backoff_max_seconds = backoff_max_seconds  # type: ignore[attr-defined]

        return func

    return inner


def concurrency(
    name: str = "",
    max_runs: int = 1,
    limit_strategy: ConcurrencyLimitStrategy = ConcurrencyLimitStrategy.CANCEL_IN_PROGRESS,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    def inner(func: Callable[P, R]) -> Callable[P, R]:
        ## TODO: Use Protocol here to help with MyPy errors
        func._concurrency_fn_name = name.lower() or str(func.__name__).lower()  # type: ignore[attr-defined]
        func._concurrency_max_runs = max_runs  # type: ignore[attr-defined]
        func._concurrency_limit_strategy = limit_strategy  # type: ignore[attr-defined]

        return func

    return inner


class HatchetRest:
    """
    Main client for interacting with the Hatchet API.

    This class provides access to various client interfaces and utility methods
    for working with Hatchet via the REST API,

    Attributes:
        rest (RestApi): Interface for REST API operations.
    """

    rest: RestApi

    def __init__(self, config: ClientConfig = ClientConfig()):
        _config: ClientConfig = ConfigLoader(".").load_client_config(config)
        self.rest = RestApi(_config.server_url, _config.token, _config.tenant_id)


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

    concurrency = staticmethod(concurrency)

    workflow = staticmethod(workflow)

    step = staticmethod(step)

    on_failure_step = staticmethod(on_failure_step)

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
