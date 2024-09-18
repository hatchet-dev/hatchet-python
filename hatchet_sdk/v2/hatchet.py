import asyncio
import functools
import inspect
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict, List, Optional, ParamSpec, Tuple, TypeVar

import hatchet_sdk.hatchet as v1
import hatchet_sdk.v2.callable as callable
import hatchet_sdk.v2.runtime.config as config
import hatchet_sdk.v2.runtime.logging as logging
import hatchet_sdk.v2.runtime.registry as registry
import hatchet_sdk.v2.runtime.runner as runner
import hatchet_sdk.v2.runtime.runtime as runtime
import hatchet_sdk.v2.runtime.worker as worker
import hatchet_sdk.v2.runtime.context as context

# import hatchet_sdk.runtime.registry as hatchet_registry
# import hatchet_sdk.v2.callable as v2_callable
# from hatchet_sdk.context import Context
# from hatchet_sdk.contracts.workflows_pb2 import ConcurrencyLimitStrategy, StickyStrategy

# import Hatchet as HatchetV1
# from hatchet_sdk.hatchet import workflow
# from hatchet_sdk.labels import DesiredWorkerLabel
# from hatchet_sdk.rate_limit import RateLimit

# from hatchet_sdk.v2.concurrency import ConcurrencyFunction
# from hatchet_sdk.worker.worker import register_on_worker

# from ..worker import Worker


T = TypeVar("T")
P = ParamSpec("P")


class Hatchet:
    def __init__(
        self,
        config: config.ClientConfig = config.ClientConfig(),
        debug=False,
        executor: ThreadPoolExecutor = ThreadPoolExecutor(),
    ):
        # ensure a event loop is created before gRPC
        try:
            asyncio.get_event_loop()
        finally:
            pass

        self.registry = registry.ActionRegistry()
        self.v1: v1.Hatchet = v1.Hatchet.from_environment(
            defaults=config,
            debug=debug,
        )
        self.executor = executor

        context.ensure_background_context(client=self)

    @property
    def admin(self):
        return self.v1.admin

    @property
    def dispatcher(self):
        return self.v1.dispatcher

    @property
    def config(self):
        return self.v1.config

    @property
    def logger(self):
        return logging.logger

    def function(
        self,
        name: str = "",
        namespace: str = "default",
        options: "callable.Options" = callable.Options(),
    ):
        def inner(func: Callable[P, T]) -> "callable.HatchetCallable[P, T]":
            if inspect.iscoroutinefunction(func):
                wrapped = callable.HatchetAwaitable(
                    func=func,
                    name=name,
                    namespace=namespace,
                    client=self,
                    options=options,
                )
                wrapped = functools.update_wrapper(wrapped, func)
                return wrapped
            elif inspect.isfunction(func):
                wrapped = callable.HatchetCallable(
                    func=func,
                    name=name,
                    namespace=namespace,
                    client=self,
                    options=options,
                )
                wrapped = functools.update_wrapper(wrapped, func)
                return wrapped
            else:
                raise TypeError(
                    "the @function decorator can only be applied to functions (def) and async functions (async def)"
                )

        return inner

    def worker(self, options: "worker.WorkerOptions") -> "runtime.Runtime":
        return runtime.Runtime(self, options)

    def _grpc_metadata(self) -> List[Tuple]:
        return [("authorization", f"bearer {self.config.token}")]

    # def durable(s
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

    # class OldHatchet(v1.Hatchet):
    #     # dag = staticmethod(v1.workflow)
    #     # concurrency = staticmethod(concurrency)

    #     _registry: hatchet_registry.ActionRegistry = hatchet_registry.ActionRegistry()


#     # def durable(
#     #     self,
#     #     name: str = "",
#     #     auto_register: bool = True,
#     #     on_events: list | None = None,
#     #     on_crons: list | None = None,
#     #     version: str = "",
#     #     timeout: str = "60m",
#     #     schedule_timeout: str = "5m",
#     #     sticky: StickyStrategy = None,
#     #     retries: int = 0,
#     #     rate_limits: List[RateLimit] | None = None,
#     #     desired_worker_labels: dict[str:DesiredWorkerLabel] = {},
#     #     concurrency: v2.concurrency.ConcurrencyFunction | None = None,
#     #     on_failure: Optional["HatchetCallable"] = None,
#     #     default_priority: int | None = None,
#     # ) -> Callable[[v2.callable.HatchetCallable], v2.callable.HatchetCallable]:
#     #     resp = durable(
#     #         name=name,
#     #         auto_register=auto_register,
#     #         on_events=on_events,
#     #         on_crons=on_crons,
#     #         version=version,
#     #         timeout=timeout,
#     #         schedule_timeout=schedule_timeout,
#     #         sticky=sticky,
#     #         retries=retries,
#     #         rate_limits=rate_limits,
#     #         desired_worker_labels=desired_worker_labels,
#     #         concurrency=concurrency,
#     #         on_failure=on_failure,
#     #         default_priority=default_priority,
#     #     )

#     #     def wrapper(func: Callable[[Context], T]) -> v2.callable.HatchetCallable[T]:
#     #         wrapped_resp = resp(func)

#     #         if wrapped_resp.function_auto_register:
#     #             self.functions.append(wrapped_resp)

#     #         wrapped_resp.with_namespace(self._client.config.namespace)

#     #         return wrapped_resp

#     #     return wrapper

#     def worker(
#         self,
#         name: str,
#         max_runs: Optional[int] = None,
#         labels: Dict[str, str | int] = {},
#     ):
#         worker = Worker(
#             name=name,
#             max_runs=max_runs,
#             labels=labels,
#             config=self._client.config,
#             debug=self._client.debug,
#         )

#         for func in self._registry.registry.values():
#             register_on_worker(func, worker)

#         return worker
