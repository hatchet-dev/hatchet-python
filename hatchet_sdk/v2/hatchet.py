from typing import Any, Callable, List, Optional, TypeVar

from hatchet_sdk.context import Context
from hatchet_sdk.contracts.workflows_pb2 import ConcurrencyLimitStrategy
from hatchet_sdk.hatchet import Hatchet as HatchetV1
from hatchet_sdk.hatchet import workflow
from hatchet_sdk.rate_limit import RateLimit
from hatchet_sdk.v2.callable import HatchetCallable
from hatchet_sdk.v2.concurrency import ConcurrencyFunction
from hatchet_sdk.worker.worker import register_on_worker

from ..worker import Worker

T = TypeVar("T")


def function(
    name: str = "",
    auto_register: bool = True,
    on_events: list | None = None,
    on_crons: list | None = None,
    version: str = "",
    timeout: str = "60m",
    schedule_timeout: str = "5m",
    retries: int = 0,
    rate_limits: List[RateLimit] | None = None,
    concurrency: ConcurrencyFunction | None = None,
    on_failure: Optional["HatchetCallable"] = None,
):
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
            retries=retries,
            rate_limits=rate_limits,
            concurrency=concurrency,
            on_failure=on_failure,
        )

    return inner


def durable(
    name: str = "",
    auto_register: bool = True,
    on_events: list | None = None,
    on_crons: list | None = None,
    version: str = "",
    timeout: str = "60m",
    schedule_timeout: str = "5m",
    retries: int = 0,
    rate_limits: List[RateLimit] | None = None,
    concurrency: ConcurrencyFunction | None = None,
    on_failure: HatchetCallable | None = None,
):
    def inner(func: HatchetCallable) -> HatchetCallable:
        func.durable = True

        f = function(
            name=name,
            auto_register=auto_register,
            on_events=on_events,
            on_crons=on_crons,
            version=version,
            timeout=timeout,
            schedule_timeout=schedule_timeout,
            retries=retries,
            rate_limits=rate_limits,
            concurrency=concurrency,
            on_failure=on_failure,
        )

        resp = f(func)

        resp.durable = True

        return resp

    return inner


def concurrency(
    name: str = "concurrency",
    max_runs: int = 1,
    limit_strategy: ConcurrencyLimitStrategy = ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
):
    def inner(func: Callable[[Context], str]) -> ConcurrencyFunction:
        return ConcurrencyFunction(func, name, max_runs, limit_strategy)

    return inner


class Hatchet(HatchetV1):
    dag = staticmethod(workflow)
    concurrency = staticmethod(concurrency)

    functions: List[HatchetCallable] = []

    def function(
        self,
        name: str = "",
        auto_register: bool = True,
        on_events: list | None = None,
        on_crons: list | None = None,
        version: str = "",
        timeout: str = "60m",
        schedule_timeout: str = "5m",
        retries: int = 0,
        rate_limits: List[RateLimit] | None = None,
        concurrency: ConcurrencyFunction | None = None,
        on_failure: Optional["HatchetCallable"] = None,
    ):
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
            concurrency=concurrency,
            on_failure=on_failure,
        )

        def wrapper(func: Callable[[Context], T]) -> HatchetCallable[T]:
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
        on_events: list | None = None,
        on_crons: list | None = None,
        version: str = "",
        timeout: str = "60m",
        schedule_timeout: str = "5m",
        retries: int = 0,
        rate_limits: List[RateLimit] | None = None,
        concurrency: ConcurrencyFunction | None = None,
        on_failure: Optional["HatchetCallable"] = None,
    ) -> Callable[[HatchetCallable], HatchetCallable]:
        resp = durable(
            name=name,
            auto_register=auto_register,
            on_events=on_events,
            on_crons=on_crons,
            version=version,
            timeout=timeout,
            schedule_timeout=schedule_timeout,
            retries=retries,
            rate_limits=rate_limits,
            concurrency=concurrency,
            on_failure=on_failure,
        )

        def wrapper(func: Callable[[Context], T]) -> HatchetCallable[T]:
            wrapped_resp = resp(func)

            if wrapped_resp.function_auto_register:
                self.functions.append(wrapped_resp)

            wrapped_resp.with_namespace(self._client.config.namespace)

            return wrapped_resp

        return wrapper

    def worker(self, name: str, max_runs: int | None = None):
        worker = Worker(
            name=name,
            max_runs=max_runs,
            config=self._client.config,
            debug=self._client.debug,
        )

        for func in self.functions:
            register_on_worker(func, worker)

        return worker
