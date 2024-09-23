import asyncio
import functools
import inspect
from concurrent.futures import ThreadPoolExecutor, Future
from contextlib import suppress
from typing import Callable, List, Optional, ParamSpec, Tuple, TypeVar

import hatchet_sdk.hatchet as v1
import hatchet_sdk.v2.callable as callable
import hatchet_sdk.v2.runtime.config as config
import hatchet_sdk.v2.runtime.context as context
import hatchet_sdk.v2.runtime.logging as logging
import hatchet_sdk.v2.runtime.registry as registry
import hatchet_sdk.v2.runtime.runner as runner
import hatchet_sdk.v2.runtime.runtime as runtime
import hatchet_sdk.v2.runtime.worker as worker

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

    # FIXME: consider separating this into @func and @afunc for better type hints.
    # Right now, the type hint for the return type is (P -> T) | (P -> Future[T]) and this is because we
    # don't statically know whether "func" is a def or an async def.
    def function(
        self,
        *,
        name: str = "",
        namespace: str = "default",
        options: "callable.Options" = callable.Options(),
    ):
        # TODO: needs to detect and reject an already decorated free function.
        # TODO: needs to detect and reject a classmethod/staticmethod.
        def inner(func: Callable[P, T]):
            if inspect.iscoroutinefunction(func):
                wrapped = callable.HatchetAwaitable[P, T](
                    func=func,
                    name=name,
                    namespace=namespace,
                    client=self,
                    options=options,
                )
                # TODO: investigate the type error here.
                aret: Callable[P, T] = functools.update_wrapper(wrapped, func)
                return aret
            elif inspect.isfunction(func):
                wrapped = callable.HatchetCallable(
                    func=func,
                    name=name,
                    namespace=namespace,
                    client=self,
                    options=options,
                )
                ret: Callable[P, Future[T]] = functools.update_wrapper(wrapped, func)
                return ret
            else:
                raise TypeError(
                    "the @function decorator can only be applied to functions (def) and async functions (async def)"
                )

        return inner

    # TODO: make it 1 worker : 1 client, which means moving the options to the initializer, and cache the result.
    # TODO: rename it to runtime
    def worker(
        self, *, options: Optional["worker.WorkerOptions"] = None
    ) -> "runtime.Runtime":
        if self._runtime is None:
            assert options is not None
            self._runtime = runtime.Runtime(client=self, options=options)
        return self._runtime

    def _grpc_metadata(self) -> List[Tuple]:
        return [("authorization", f"bearer {self.config.token}")]
