import asyncio
from enum import Enum
from typing import Any, Callable, Generic, ParamSpec, Type, TypeVar, Union

from pydantic import BaseModel, ConfigDict

from hatchet_sdk.clients.rest_client import RestApi
from hatchet_sdk.context.context import Context
from hatchet_sdk.contracts.workflows_pb2 import (
    ConcurrencyLimitStrategy,
    CreateStepRateLimit,
    CreateWorkflowJobOpts,
    CreateWorkflowStepOpts,
    CreateWorkflowVersionOpts,
    DesiredWorkerLabels,
)
from hatchet_sdk.contracts.workflows_pb2 import StickyStrategy as StickyStrategyProto
from hatchet_sdk.contracts.workflows_pb2 import WorkflowConcurrencyOpts, WorkflowKind
from hatchet_sdk.labels import DesiredWorkerLabel
from hatchet_sdk.rate_limit import RateLimit

from ..logger import logger

R = TypeVar("R")
P = ParamSpec("P")


class ConcurrencyExpression:
    """
    Defines concurrency limits for a workflow using a CEL expression.

    Args:
        expression (str): CEL expression to determine concurrency grouping. (i.e. "input.user_id")
        max_runs (int): Maximum number of concurrent workflow runs.
        limit_strategy (ConcurrencyLimitStrategy): Strategy for handling limit violations.

    Example:
        ConcurrencyExpression("input.user_id", 5, ConcurrencyLimitStrategy.CANCEL_IN_PROGRESS)
    """

    def __init__(
        self, expression: str, max_runs: int, limit_strategy: ConcurrencyLimitStrategy
    ):
        self.expression = expression
        self.max_runs = max_runs
        self.limit_strategy = limit_strategy


class EmptyModel(BaseModel):
    model_config = ConfigDict(extra="allow")


class StickyStrategy(str, Enum):
    SOFT = "SOFT"
    HARD = "HARD"


class WorkflowConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)
    name: str = ""
    on_events: list[str] = []
    on_crons: list[str] = []
    version: str = ""
    timeout: str = "60m"
    schedule_timeout: str = "5m"
    sticky: Union[StickyStrategy, None] = None
    default_priority: int = 1
    concurrency: ConcurrencyExpression | None = None
    input_validator: Type[BaseModel] = EmptyModel


class StepType(str, Enum):
    DEFAULT = "default"
    CONCURRENCY = "concurrency"
    ON_FAILURE = "on_failure"


class Step(Generic[R]):
    def __init__(
        self,
        fn: Callable[[Any, Context], R],
        type: StepType,
        name: str = "",
        timeout: str = "60m",
        parents: list[str] = [],
        retries: int = 0,
        rate_limits: list[CreateStepRateLimit] = [],
        desired_worker_labels: dict[str, DesiredWorkerLabels] = {},
        backoff_factor: float | None = None,
        backoff_max_seconds: int | None = None,
    ) -> None:
        self.fn = fn
        self.is_async_function = bool(asyncio.iscoroutinefunction(fn))

        self.type = type
        self.timeout = timeout
        self.name = name
        self.parents = parents
        self.retries = retries
        self.rate_limits = rate_limits
        self.desired_worker_labels = desired_worker_labels
        self.backoff_factor = backoff_factor
        self.backoff_max_seconds = backoff_max_seconds
        self.concurrency__max_runs = 1
        self.concurrency__limit_strategy = ConcurrencyLimitStrategy.CANCEL_IN_PROGRESS

    def __call__(self, ctx: Context) -> R:
        return self.fn(None, ctx)


class Workflow:
    config: WorkflowConfig = WorkflowConfig()

    def get_service_name(self, namespace: str) -> str:
        return f"{namespace}{self.config.name.lower()}"

    def _get_steps_by_type(self, step_type: StepType) -> list[Step[Any]]:
        return [
            attr
            for _, attr in self.__class__.__dict__.items()
            if isinstance(attr, Step) and attr.type == step_type
        ]

    @property
    def on_failure_steps(self) -> list[Step[Any]]:
        return self._get_steps_by_type(StepType.ON_FAILURE)

    @property
    def concurrency_actions(self) -> list[Step[Any]]:
        return self._get_steps_by_type(StepType.CONCURRENCY)

    @property
    def default_steps(self) -> list[Step[Any]]:
        return self._get_steps_by_type(StepType.DEFAULT)

    @property
    def steps(self) -> list[Step[Any]]:
        return self.default_steps + self.concurrency_actions + self.on_failure_steps

    def create_action_name(self, namespace: str, step: Step[Any]) -> str:
        return self.get_service_name(namespace) + ":" + step.name

    def __init__(self) -> None:
        self.config.name = self.config.name or str(self.__class__.__name__)

    def get_name(self, namespace: str) -> str:
        return namespace + self.config.name

    def validate_concurrency_actions(
        self, service_name: str
    ) -> WorkflowConcurrencyOpts | None:
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

        return None

    def validate_on_failure_steps(
        self, name: str, service_name: str
    ) -> CreateWorkflowJobOpts | None:
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
                    rate_limits=on_failure_step.rate_limits,
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

    def validate_sticky(
        self, sticky: Union[StickyStrategy, None]
    ) -> StickyStrategyProto | None:
        if sticky:
            return StickyStrategyProto(sticky)
        return None

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
                rate_limits=step.rate_limits,
                worker_labels=step.desired_worker_labels,
                backoff_factor=step.backoff_factor,
                backoff_max_seconds=step.backoff_max_seconds,
            )
            for step in self.steps
            if step.type == StepType.DEFAULT
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
            sticky=self.validate_sticky(self.config.sticky),
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
