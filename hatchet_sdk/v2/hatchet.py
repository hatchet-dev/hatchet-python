import functools
import inspect
from typing import Callable, List, Optional, ParamSpec, TypeVar, Dict

import hatchet_sdk.hatchet as v1
import hatchet_sdk.runtime.registry as hatchet_registry
import hatchet_sdk.v2.callable as v2_callable
from hatchet_sdk.context import Context
from hatchet_sdk.contracts.workflows_pb2 import ConcurrencyLimitStrategy, StickyStrategy

# import Hatchet as HatchetV1
# from hatchet_sdk.hatchet import workflow
from hatchet_sdk.labels import DesiredWorkerLabel
from hatchet_sdk.rate_limit import RateLimit

from ..worker import Worker

# from hatchet_sdk.v2.concurrency import ConcurrencyFunction
from hatchet_sdk.worker.worker import register_on_worker


T = TypeVar("T")
P = ParamSpec("P")


# def durable(
#     name: str = "",
#     auto_register: bool = True,
#     on_events: list | None = None,
#     on_crons: list | None = None,
#     version: str = "",
#     timeout: str = "60m",
#     schedule_timeout: str = "5m",
#     sticky: StickyStrategy = None,
#     retries: int = 0,
#     rate_limits: List[RateLimit] | None = None,
#     desired_worker_labels: dict[str:DesiredWorkerLabel] = {},
#     concurrency: v2.concurrency.ConcurrencyFunction | None = None,
#     on_failure: v2.callable.HatchetCallable | None = None,
#     default_priority: int | None = None,
# ):
#     def inner(func: v2.callable.HatchetCallable) -> v2.callable.HatchetCallable:
#         func.durable = True

#         f = function(
#             name=name,
#             auto_register=auto_register,
#             on_events=on_events,
#             on_crons=on_crons,
#             version=version,
#             timeout=timeout,
#             schedule_timeout=schedule_timeout,
#             sticky=sticky,
#             retries=retries,
#             rate_limits=rate_limits,
#             desired_worker_labels=desired_worker_labels,
#             concurrency=concurrency,
#             on_failure=on_failure,
#             default_priority=default_priority,
#         )

#         resp = f(func)

#         resp.durable = True

#         return resp

#     return inner


# def concurrency(
#     name: str = "concurrency",
#     max_runs: int = 1,
#     limit_strategy: ConcurrencyLimitStrategy = ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
# ):
#     def inner(func: Callable[[Context], str]) -> v2.concurrency.ConcurrencyFunction:
#         return v2.concurrency.ConcurrencyFunction(func, name, max_runs, limit_strategy)

#     return inner


class Hatchet(v1.Hatchet):
    # dag = staticmethod(v1.workflow)
    # concurrency = staticmethod(concurrency)

    _registry: hatchet_registry.ActionRegistry = hatchet_registry.ActionRegistry()

    def function(
        self,
        name: str = "",
        namespace: str = "default",
        options: v2_callable.Options = v2_callable.Options(),
    ):
        options.hatchet = self

        def inner(func: Callable[P, T]) -> v2_callable.HatchetCallable[P, T]:
            if inspect.iscoroutinefunction(func):
                callable = v2_callable.HatchetAwaitable(
                    func=func,
                    name=name,
                    namespace=namespace,
                    options=options,
                )
                callable = functools.update_wrapper(callable, func)
                callable.action_name = self._registry.register(callable)
                return callable
            elif inspect.isfunction(func):
                callable = v2_callable.HatchetCallable(
                    func=func,
                    name=name,
                    namespace=namespace,
                    options=options,
                )
                callable = functools.update_wrapper(callable, func)
                callable.action_name = self._registry.register(callable)
                return callable
            else:
                raise TypeError(
                    "the @function decorator can only be applied to functions (def) and async functions (async def)"
                )

        return inner

    # def durable(
    #     self,
    #     name: str = "",
    #     auto_register: bool = True,
    #     on_events: list | None = None,
    #     on_crons: list | None = None,
    #     version: str = "",
    #     timeout: str = "60m",
    #     schedule_timeout: str = "5m",
    #     sticky: StickyStrategy = None,
    #     retries: int = 0,
    #     rate_limits: List[RateLimit] | None = None,
    #     desired_worker_labels: dict[str:DesiredWorkerLabel] = {},
    #     concurrency: v2.concurrency.ConcurrencyFunction | None = None,
    #     on_failure: Optional["HatchetCallable"] = None,
    #     default_priority: int | None = None,
    # ) -> Callable[[v2.callable.HatchetCallable], v2.callable.HatchetCallable]:
    #     resp = durable(
    #         name=name,
    #         auto_register=auto_register,
    #         on_events=on_events,
    #         on_crons=on_crons,
    #         version=version,
    #         timeout=timeout,
    #         schedule_timeout=schedule_timeout,
    #         sticky=sticky,
    #         retries=retries,
    #         rate_limits=rate_limits,
    #         desired_worker_labels=desired_worker_labels,
    #         concurrency=concurrency,
    #         on_failure=on_failure,
    #         default_priority=default_priority,
    #     )

    #     def wrapper(func: Callable[[Context], T]) -> v2.callable.HatchetCallable[T]:
    #         wrapped_resp = resp(func)

    #         if wrapped_resp.function_auto_register:
    #             self.functions.append(wrapped_resp)

    #         wrapped_resp.with_namespace(self._client.config.namespace)

    #         return wrapped_resp

    #     return wrapper

    def worker(
        self,
        name: str,
        max_runs: Optional[int] = None,
        labels: Dict[str, str | int] = {},
    ):
        worker = Worker(
            name=name,
            max_runs=max_runs,
            labels=labels,
            config=self._client.config,
            debug=self._client.debug,
        )

        for func in self._registry.registry.values():
            register_on_worker(func, worker)

        return worker
