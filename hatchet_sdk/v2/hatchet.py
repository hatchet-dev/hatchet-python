import asyncio
import functools
import inspect
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from typing import Callable, Dict, List, Optional, ParamSpec, Tuple, TypeVar

import hatchet_sdk.hatchet as v1
import hatchet_sdk.v2.callable as callable
import hatchet_sdk.v2.runtime.config as config
import hatchet_sdk.v2.runtime.context as context
import hatchet_sdk.v2.runtime.logging as logging
import hatchet_sdk.v2.runtime.registry as registry
import hatchet_sdk.v2.runtime.runner as runner
import hatchet_sdk.v2.runtime.runtime as runtime
import hatchet_sdk.v2.runtime.worker as worker

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

        with suppress(RuntimeError):
            asyncio.get_event_loop()

        self.registry = registry.ActionRegistry()
        self.v1: v1.Hatchet = v1.Hatchet.from_environment(
            defaults=config,
            debug=debug,
        )
        self.executor = executor

        self._runtime: Optional["runtime.Runtime"] = None

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

    # TODO: make it 1 worker : 1 client, which means moving the options to the initializer, and cache the result.
    def worker(self, options: "worker.WorkerOptions") -> "runtime.Runtime":
        if self._runtime is None:
            self._runtime = runtime.Runtime(self, options)
        return self._runtime

    def _grpc_metadata(self) -> List[Tuple]:
        return [("authorization", f"bearer {self.config.token}")]
