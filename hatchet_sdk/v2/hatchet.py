from typing import Any, Callable, Generic, Type, TypeVar, Union, cast

from pydantic import BaseModel, ConfigDict

from hatchet_sdk import Worker
from hatchet_sdk.clients.admin import ChildTriggerWorkflowOptions
from hatchet_sdk.context.context import Context
from hatchet_sdk.contracts.workflows_pb2 import (  # type: ignore[attr-defined]
    ConcurrencyLimitStrategy,
    StickyStrategy,
)
from hatchet_sdk.hatchet import Hatchet as HatchetV1
from hatchet_sdk.hatchet import workflow
from hatchet_sdk.labels import DesiredWorkerLabel
from hatchet_sdk.rate_limit import RateLimit
from hatchet_sdk.v2.callable import DurableContext, EmptyModel, HatchetCallable
from hatchet_sdk.v2.concurrency import ConcurrencyFunction
from hatchet_sdk.worker.worker import register_on_worker
from hatchet_sdk.workflow_run import WorkflowRunRef

T = TypeVar("T")
TWorkflowInput = TypeVar("TWorkflowInput", bound=BaseModel)


class DeclarativeWorkflowConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    input_validator: Type[BaseModel] = EmptyModel
    name: str = ""
    on_events: list[str] | None = None
    on_crons: list[str] | None = None
    version: str = ""
    timeout: str = "60m"
    schedule_timeout: str = "5m"
    concurrency: ConcurrencyFunction | None = None
    default_priority: int | None = None


def function(
    input_validator: Type[BaseModel],
    name: str = "",
    auto_register: bool = True,
    on_events: list[str] | None = None,
    on_crons: list[str] | None = None,
    version: str = "",
    timeout: str = "60m",
    schedule_timeout: str = "5m",
    sticky: StickyStrategy = None,
    retries: int = 0,
    rate_limits: list[RateLimit] | None = None,
    desired_worker_labels: dict[str, DesiredWorkerLabel] = {},
    concurrency: ConcurrencyFunction | None = None,
    on_failure: Union["HatchetCallable[T]", None] = None,
    default_priority: int | None = None,
) -> Callable[[Callable[[Context], str]], HatchetCallable[T]]:
    def inner(func: Callable[[Context], T]) -> HatchetCallable[T]:
        return HatchetCallable(
            func=func,
            name=name,
            auto_register=auto_register,
            on_events=on_events,
            on_crons=on_crons,
            version=version,
            timeout=timeout,
            schedule_timeout=schedule_timeout,
            sticky=sticky,
            retries=retries,
            rate_limits=rate_limits,
            desired_worker_labels=desired_worker_labels,
            concurrency=concurrency,
            on_failure=on_failure,
            default_priority=default_priority,
            input_validator=input_validator,
        )

    return inner


def durable(
    name: str = "",
    auto_register: bool = True,
    on_events: list[str] | None = None,
    on_crons: list[str] | None = None,
    version: str = "",
    timeout: str = "60m",
    schedule_timeout: str = "5m",
    sticky: StickyStrategy = None,
    retries: int = 0,
    rate_limits: list[RateLimit] | None = None,
    desired_worker_labels: dict[str, DesiredWorkerLabel] = {},
    concurrency: ConcurrencyFunction | None = None,
    on_failure: HatchetCallable[T] | None = None,
    default_priority: int | None = None,
) -> Callable[[HatchetCallable[T]], HatchetCallable[T]]:
    def inner(func: HatchetCallable[T]) -> HatchetCallable[T]:
        func.durable = True

        f = function(
            name=name,
            auto_register=auto_register,
            on_events=on_events,
            on_crons=on_crons,
            version=version,
            timeout=timeout,
            schedule_timeout=schedule_timeout,
            sticky=sticky,
            retries=retries,
            rate_limits=rate_limits,
            desired_worker_labels=desired_worker_labels,
            concurrency=concurrency,
            on_failure=on_failure,
            default_priority=default_priority,
        )

        resp = f(func)

        resp.durable = True

        return resp

    return inner


def concurrency(
    name: str = "concurrency",
    max_runs: int = 1,
    limit_strategy: ConcurrencyLimitStrategy = ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
) -> Callable[[Callable[[Context], str]], ConcurrencyFunction]:
    def inner(func: Callable[[Context], str]) -> ConcurrencyFunction:
        return ConcurrencyFunction(func, name, max_runs, limit_strategy)

    return inner


class SpawnWorkflowInput(BaseModel):
    workflow_name: str
    input: BaseModel
    key: str | None = None
    options: ChildTriggerWorkflowOptions | None = None


class DeclarativeWorkflow(Generic[TWorkflowInput]):
    def __init__(self, config: DeclarativeWorkflowConfig, hatchet: Hatchet):
        self.config = config
        self.hatchet = hatchet

    def run(self, input: TWorkflowInput) -> WorkflowRunRef:
        return self.hatchet.admin.run_workflow(
            workflow_name=self.config.name, input=input.model_dump()
        )

    async def spawn(
        self, context: Context, input: SpawnWorkflowInput
    ) -> WorkflowRunRef:
        return await context.aio.spawn_workflow(
            workflow_name=input.workflow_name,
            input=input.input.model_dump(),
            key=input.key,
            options=input.options,
        )

    def construct_spawn_workflow_input(
        self, input: TWorkflowInput
    ) -> SpawnWorkflowInput:
        return SpawnWorkflowInput(workflow_name=self.config.name, input=input)

    def declare(self) -> Callable[[Callable[[Context], Any]], Callable[[Context], Any]]:
        return self.hatchet.function(**self.config.model_dump())

    def workflow_input(self, ctx: Context) -> TWorkflowInput:
        return cast(TWorkflowInput, ctx.workflow_input())


class Hatchet(HatchetV1):
    dag = staticmethod(workflow)
    concurrency = staticmethod(concurrency)

    functions: list[HatchetCallable[T]] = []

    def function(
        self,
        input_validator: Type[BaseModel],
        name: str = "",
        auto_register: bool = True,
        on_events: list[str] | None = None,
        on_crons: list[str] | None = None,
        version: str = "",
        timeout: str = "60m",
        schedule_timeout: str = "5m",
        retries: int = 0,
        rate_limits: list[RateLimit] | None = None,
        desired_worker_labels: dict[str, DesiredWorkerLabel] = {},
        concurrency: ConcurrencyFunction | None = None,
        on_failure: Union["HatchetCallable[T]", None] = None,
        default_priority: int | None = None,
    ) -> Callable[[Callable[[Context], Any]], Callable[[Context], Any]]:
        resp = function(
            name=name,
            auto_register=auto_register,
            on_events=on_events,
            on_crons=on_crons,
            version=version,
            timeout=timeout,
            schedule_timeout=schedule_timeout,
            retries=retries,
            rate_limits=rate_limits,
            desired_worker_labels=desired_worker_labels,
            concurrency=concurrency,
            on_failure=on_failure,
            default_priority=default_priority,
            input_validator=input_validator,
        )

        def wrapper(func: Callable[[Context], Any]) -> HatchetCallable[T]:
            wrapped_resp = resp(func)

            if wrapped_resp.function_auto_register:
                self.functions.append(wrapped_resp)

            wrapped_resp.with_namespace(self._client.config.namespace)

            return wrapped_resp

        return wrapper

    def durable(
        self,
        name: str = "",
        auto_register: bool = True,
        on_events: list[str] | None = None,
        on_crons: list[str] | None = None,
        version: str = "",
        timeout: str = "60m",
        schedule_timeout: str = "5m",
        sticky: StickyStrategy = None,
        retries: int = 0,
        rate_limits: list[RateLimit] | None = None,
        desired_worker_labels: dict[str, DesiredWorkerLabel] = {},
        concurrency: ConcurrencyFunction | None = None,
        on_failure: Union["HatchetCallable[T]", None] = None,
        default_priority: int | None = None,
    ) -> Callable[[Callable[[DurableContext], Any]], Callable[[DurableContext], Any]]:
        resp = durable(
            name=name,
            auto_register=auto_register,
            on_events=on_events,
            on_crons=on_crons,
            version=version,
            timeout=timeout,
            schedule_timeout=schedule_timeout,
            sticky=sticky,
            retries=retries,
            rate_limits=rate_limits,
            desired_worker_labels=desired_worker_labels,
            concurrency=concurrency,
            on_failure=on_failure,
            default_priority=default_priority,
        )

        def wrapper(func: HatchetCallable[T]) -> HatchetCallable[T]:
            wrapped_resp = resp(func)

            if wrapped_resp.function_auto_register:
                self.functions.append(wrapped_resp)

            wrapped_resp.with_namespace(self._client.config.namespace)

            return wrapped_resp

        return wrapper

    def worker(
        self, name: str, max_runs: int | None = None, labels: dict[str, str | int] = {}
    ):
        worker = Worker(
            name=name,
            max_runs=max_runs,
            labels=labels,
            config=self._client.config,
            debug=self._client.debug,
        )

        for func in self.functions:
            register_on_worker(func, worker)

        return worker

    def declare_workflow(
        self,
        input_validator: Type[TWorkflowInput],
        name: str = "",
        on_events: list[str] | None = None,
        on_crons: list[str] | None = None,
        version: str = "",
        timeout: str = "60m",
        schedule_timeout: str = "5m",
        concurrency: ConcurrencyFunction | None = None,
        default_priority: int | None = None,
    ) -> DeclarativeWorkflow[TWorkflowInput]:
        return DeclarativeWorkflow[input_validator](
            hatchet=self,
            config=DeclarativeWorkflowConfig(
                input_validator=input_validator,
                name=name,
                on_events=on_events,
                on_crons=on_crons,
                version=version,
                timeout=timeout,
                schedule_timeout=schedule_timeout,
                concurrency=concurrency,
                default_priority=default_priority,
            ),
        )
