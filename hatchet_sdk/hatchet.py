import logging
from typing import List, Optional, TypedDict

from hatchet_sdk.loader import ClientConfig
from hatchet_sdk.rate_limit import RateLimit

from .client import ClientImpl, new_client, new_client_raw
from .logger import logger
from .worker import Worker
from .workflow import WorkflowMeta
from .workflows_pb2 import (
    ConcurrencyLimitStrategy,
    CreateStepRateLimit,
    DesiredWorkerLabels,
    StickyStrategy,
)


def workflow(
    name: str = "",
    on_events: list | None = None,
    on_crons: list | None = None,
    version: str = "",
    timeout: str = "60m",
    schedule_timeout: str = "5m",
    sticky: StickyStrategy = None,
):
    on_events = on_events or []
    on_crons = on_crons or []

    def inner(cls) -> WorkflowMeta:
        cls.on_events = on_events
        cls.on_crons = on_crons
        cls.name = name or str(cls.__name__)
        cls.version = version
        cls.timeout = timeout
        cls.schedule_timeout = schedule_timeout
        cls.sticky = sticky

        # Define a new class with the same name and bases as the original, but
        # with WorkflowMeta as its metaclass
        return WorkflowMeta(cls.name, cls.__bases__, dict(cls.__dict__))

    return inner


class DesiredWorkerLabel(TypedDict):
    value: str | int
    required: bool | None = None
    weight: int | None = None
    comparator: int | None = (
        None  # _ClassVar[WorkerLabelComparator] TODO figure out type
    )


def step(
    name: str = "",
    timeout: str = "",
    parents: List[str] | None = None,
    retries: int = 0,
    rate_limits: List[RateLimit] | None = None,
    desired_worker_labels: dict[str:DesiredWorkerLabel] = {},
):
    parents = parents or []

    def inner(func):
        limits = None
        if rate_limits:
            limits = [
                CreateStepRateLimit(key=rate_limit.key, units=rate_limit.units)
                for rate_limit in rate_limits or []
            ]

        func._step_name = name.lower() or str(func.__name__).lower()
        func._step_parents = parents
        func._step_timeout = timeout
        func._step_retries = retries
        func._step_rate_limits = limits

        func._step_desired_worker_labels = {}

        for key, d in desired_worker_labels.items():
            value = d["value"] if "value" in d else None
            func._step_desired_worker_labels[key] = DesiredWorkerLabels(
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
    rate_limits: List[RateLimit] | None = None,
):
    def inner(func):
        limits = None
        if rate_limits:
            limits = [
                CreateStepRateLimit(key=rate_limit.key, units=rate_limit.units)
                for rate_limit in rate_limits or []
            ]

        func._on_failure_step_name = name.lower() or str(func.__name__).lower()
        func._on_failure_step_timeout = timeout
        func._on_failure_step_retries = retries
        func._on_failure_step_rate_limits = limits
        return func

    return inner


def concurrency(
    name: str = "",
    max_runs: int = 1,
    limit_strategy: ConcurrencyLimitStrategy = ConcurrencyLimitStrategy.CANCEL_IN_PROGRESS,
):
    def inner(func):
        func._concurrency_fn_name = name.lower() or str(func.__name__).lower()
        func._concurrency_max_runs = max_runs
        func._concurrency_limit_strategy = limit_strategy

        return func

    return inner


class Hatchet:
    client: ClientImpl

    @classmethod
    def from_environment(cls, defaults: ClientConfig = ClientConfig(), **kwargs):
        return cls(client=new_client(defaults), **kwargs)

    @classmethod
    def from_config(cls, config: ClientConfig, **kwargs):
        return cls(client=new_client_raw(config), **kwargs)

    def __init__(
        self,
        debug: bool = False,
        client: Optional[ClientImpl] = None,
        config: ClientConfig = ClientConfig(),
    ):
        if client is not None:
            self.client = client
        else:
            self.client = new_client(config)

        if debug:
            logger.setLevel(logging.DEBUG)

    concurrency = staticmethod(concurrency)

    workflow = staticmethod(workflow)

    step = staticmethod(step)

    on_failure_step = staticmethod(on_failure_step)

    def worker(
        self, name: str, max_runs: int | None = None, labels: dict[str, str | int] = {}
    ):
        return Worker(
            name=name, max_runs=max_runs, labels=labels, config=self.client.config
        )
