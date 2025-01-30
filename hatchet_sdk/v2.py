import asyncio
from hatchet_sdk.contracts.workflows_pb2 import (
    CreateWorkflowJobOpts,
    CreateWorkflowStepOpts,
    CreateWorkflowVersionOpts,
    StickyStrategy,
    WorkflowConcurrencyOpts,
    WorkflowKind,
)
import logging
from typing import Any, Callable, Optional, ParamSpec, Type, TypeVar, Union

from pydantic import BaseModel, ConfigDict
from typing_extensions import deprecated
from enum import Enum

from hatchet_sdk.clients.rest_client import RestApi
from hatchet_sdk.context.context import Context
from hatchet_sdk.contracts.workflows_pb2 import (
    ConcurrencyLimitStrategy,
    CreateStepRateLimit,
    DesiredWorkerLabels,
    StickyStrategy,
    WorkerLabelComparator,
)
from hatchet_sdk.features.cron import CronClient
from hatchet_sdk.features.scheduled import ScheduledClient
from hatchet_sdk.labels import DesiredWorkerLabel
from hatchet_sdk.loader import ClientConfig
from hatchet_sdk.rate_limit import RateLimit

from .client import Client, new_client, new_client_raw
from .clients.admin import AdminClient
from .clients.dispatcher.dispatcher import DispatcherClient
from .clients.events import EventClient
from .clients.run_event_listener import RunEventListenerClient
from .logger import logger
from .worker.worker import Worker
from .workflow import (
    ConcurrencyExpression,
    WorkflowInterface,
    WorkflowMeta,
    WorkflowStepProtocol,
)

T = TypeVar("T", bound=BaseModel)
R = TypeVar("R")
P = ParamSpec("P")

TWorkflow = TypeVar("TWorkflow", bound=object)

class EmptyModel(BaseModel):
    model_config = ConfigDict(extra="allow")


class WorkflowConfig(BaseModel):
    name: str = ""
    on_events: list[str] = []
    on_crons: list[str] = []
    version: str = ""
    timeout: str = "60m"
    schedule_timeout: str = "5m"
    sticky: Union[StickyStrategy, None] = None
    default_priority: int = 0
    concurrency: ConcurrencyExpression | None = None
    input_validator: Type[BaseModel] = EmptyModel

class StepType(str, Enum):
    DEFAULT = "default"
    CONCURRENCY = "concurrency"
    ON_FAILURE = "on_failure"


class Step:
    def __init__(self) -> None:
        self.type = StepType.DEFAULT
        self.timeout = "60s"
        self.name = "name"
        self.parents: list[Step] = []
        self.retries: int = 0
        self.rate_limits: list[RateLimit] = []
        self.desired_worker_labels: dict[str, DesiredWorkerLabel] = {}
        self.backoff_factor: float | None = None
        self.backoff_max_seconds: int | None = None
        self.concurrency__max_runs = 1
        self.concurrency__limit_strategy = ConcurrencyLimitStrategy.CANCEL_IN_PROGRESS


class Workflow:
    config: WorkflowConfig = WorkflowConfig()

    def get_service_name(self, namespace: str) -> str:
        return f"{namespace}{self.config.name.lower()}"

    @property
    def on_failure_steps(self) -> list[Step]:
        return [
            inst
            for attr in dir(self)
            if isinstance(inst := getattr(self, attr), Step) and inst.type == StepType.ON_FAILURE
        ]

    @property
    def concurrency_actions(self) -> list[Step]:
        return [
            inst
            for attr in dir(self)
            if isinstance(inst := getattr(self, attr), Step) and inst.type == StepType.CONCURRENCY
        ]

    @property
    def default_steps(self) -> list[Step]:
        return [
            inst
            for attr in dir(self)
            if isinstance(inst := getattr(self, attr), Step) and inst.type == StepType.DEFAULT
        ]


    @property
    def steps(self) -> list[Step]:
        return  self.default_steps + self.concurrency_actions + self.on_failure_steps


    @property
    def actions(self, namespace: str) -> list[Step]:
        service_name = self.get_service_name(namespace)

        return [
            service_name + ":" + step
            for step in self.steps
        ]


    def __init__(self) -> None:
        self.config.name = self.config.name or str(self.__class__.__name__)

    def get_name(self, namespace: str) -> str:
        return namespace + self.config.name

    def validate_concurrency_actions(self, service_name: str) -> WorkflowConcurrencyOpts | None:
        if len(self.concurrency_actions) > 0 and self.config.concurrency:
            raise ValueError(
                "Error: Both concurrencyActions and concurrency_expression are defined. Please use only one concurrency configuration method."
            )

        if len(self.concurrency_actions) > 0:
            action = self.concurrency_actions[0]

            return WorkflowConcurrencyOpts(
                action=service_name + ":" + action.name,
                max_runs=action.concurrency__max_runs,
                limit_strategy=action.concurrency__limit_strategy,
            )

        if self.config.concurrency:
            return WorkflowConcurrencyOpts(
                expression=self.config.concurrency.expression,
                max_runs=self.config.concurrency.max_runs,
                limit_strategy=self.config.concurrency.limit_strategy,
            )

    def validate_on_failure_steps(self, name: str, service_name: str) -> CreateWorkflowJobOpts | None:
        if not self.on_failure_steps:
            return None

        on_failure_step = next(iter(self.on_failure_steps))

        return CreateWorkflowJobOpts(
            name=name + "-on-failure",
            steps=[
                CreateWorkflowStepOpts(
                    readable_id=on_failure_step.name,
                    action=service_name + ":" + on_failure_step.name,
                    timeout=on_failure_step.timeout or "60s",
                    inputs="{}",
                    parents=[],
                    retries=on_failure_step.retries,
                    rate_limits=on_failure_step.rate_limits,  # type: ignore[arg-type]
                    backoff_factor=on_failure_step.backoff_factor,
                    backoff_max_seconds=on_failure_step.backoff_max_seconds,
                )
            ],
        )

    def validate_priority(self, default_priority: int | None) -> int | None:
        validated_priority = (
            max(1, min(3, default_priority)) if default_priority else None
        )
        if validated_priority != default_priority:
            logger.warning(
                "Warning: Default Priority Must be between 1 and 3 -- inclusively. Adjusted to be within the range."
            )

        return validated_priority

    def get_create_opts(self, namespace: str) -> CreateWorkflowVersionOpts:
        service_name = self.get_service_name(namespace)

        name = self.get_name(namespace)
        event_triggers = [namespace + event for event in self.config.on_events]

        create_step_opts = [
            CreateWorkflowStepOpts(
                readable_id=step.name,
                action=service_name + ":" + step.name,
                timeout=step.timeout or "60s",
                inputs="{}",
                parents=[x for x in step.parents],
                retries=step.retries,
                rate_limits=step.rate_limits,  # type: ignore[arg-type]
                worker_labels=step.desired_worker_labels,  # type: ignore[arg-type]
                backoff_factor=step.backoff_factor,
                backoff_max_seconds=step.backoff_max_seconds,
            )
            for step in self.steps
        ]

        concurrency = self.validate_concurrency_actions(service_name)
        on_failure_job = self.validate_on_failure_steps(name, service_name)
        validated_priority = self.validate_priority(self.config.default_priority)

        return CreateWorkflowVersionOpts(
            name=name,
            kind=WorkflowKind.DAG,
            version=self.config.version,
            event_triggers=event_triggers,
            cron_triggers=self.config.on_crons,
            schedule_timeout=self.config.schedule_timeout,
            sticky=self.config.sticky,
            jobs=[
                CreateWorkflowJobOpts(
                    name=name,
                    steps=create_step_opts,
                )
            ],
            on_failure_job=on_failure_job,
            concurrency=concurrency,
            default_priority=validated_priority,
        )


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
            limits = [rate_limit._req for rate_limit in rate_limits or []]

        setattr(func, "_step_name", name.lower() or str(func.__name__).lower())
        setattr(func, "_step_parents", parents)
        setattr(func, "_step_timeout", timeout)
        setattr(func, "_step_retries", retries)
        setattr(func, "_step_rate_limits", limits)
        setattr(func, "_step_backoff_factor", backoff_factor)
        setattr(func, "_step_backoff_max_seconds", backoff_max_seconds)

        def create_label(d: DesiredWorkerLabel) -> DesiredWorkerLabels:
            value = d.value
            return DesiredWorkerLabels(
                strValue=value if not isinstance(value, int) else None,
                intValue=value if isinstance(value, int) else None,
                required=d.required,
                weight=d.weight,
                comparator=d.comparator,  # type: ignore[arg-type]
            )

        setattr(
            func,
            "_step_desired_worker_labels",
            {key: create_label(d) for key, d in desired_worker_labels.items()},
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
) -> Callable[..., Any]:
    def inner(func: Callable[[Context], Any]) -> Callable[[Context], Any]:
        limits = None
        if rate_limits:
            limits = [
                CreateStepRateLimit(key=rate_limit.static_key, units=rate_limit.units)  # type: ignore[arg-type]
                for rate_limit in rate_limits or []
            ]

        setattr(
            func, "_on_failure_step_name", name.lower() or str(func.__name__).lower()
        )
        setattr(func, "_on_failure_step_timeout", timeout)
        setattr(func, "_on_failure_step_retries", retries)
        setattr(func, "_on_failure_step_rate_limits", limits)
        setattr(func, "_on_failure_step_backoff_factor", backoff_factor)
        setattr(func, "_on_failure_step_backoff_max_seconds", backoff_max_seconds)

        return func

    return inner


def concurrency(
    name: str = "",
    max_runs: int = 1,
    limit_strategy: ConcurrencyLimitStrategy = ConcurrencyLimitStrategy.CANCEL_IN_PROGRESS,
) -> Callable[..., Any]:
    def inner(func: Callable[[Context], Any]) -> Callable[[Context], Any]:
        setattr(
            func,
            "_concurrency_fn_name",
            name.lower() or str(func.__name__).lower(),
        )
        setattr(func, "_concurrency_max_runs", max_runs)
        setattr(func, "_concurrency_limit_strategy", limit_strategy)

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

    def __init__(self, config: ClientConfig = ClientConfig()):
        self.rest = RestApi(config.server_url, config.token, config.tenant_id)


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
