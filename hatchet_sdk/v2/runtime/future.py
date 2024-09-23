import asyncio
import multiprocessing.queues as mpq
import queue
import threading
import time
from collections.abc import Callable, MutableSet
from concurrent.futures import CancelledError, Future, ThreadPoolExecutor
from contextlib import suppress
from typing import Dict, Generic, Optional, TypeAlias, TypeVar

from google.protobuf.json_format import MessageToDict
from loguru import logger

import hatchet_sdk.v2.runtime.messages as messages
import hatchet_sdk.v2.runtime.utils as utils
from hatchet_sdk.contracts.dispatcher_pb2 import (
    SubscribeToWorkflowRunsRequest,
    WorkflowRunEvent,
)

# TODO: use better generics for Python >= 3.12
T = TypeVar("T")
RespT = TypeVar("RespT")
ReqT = TypeVar("ReqT")


_ThreadSafeQueue: TypeAlias = queue.Queue[T] | mpq.Queue[T]


class RequestResponseBroker(Generic[ReqT, RespT]):
    def __init__(
        self,
        *,
        inbound: _ThreadSafeQueue[RespT],
        outbound: _ThreadSafeQueue[ReqT],
        req_key: Callable[[ReqT], str],
        resp_key: Callable[[RespT], str],
        executor: ThreadPoolExecutor,
    ):
        """A broker that can send/forward a request and returns a future for the caller to wait upon.

        This is to be used in the main process. The broker loop runs forever and quits upon asyncio.CancelledError.
        The broker is essentially an adaptor from server-streams to either concurrent.futures.Future or asyncio.Future.
        For the blocking case (i.e. concurrent.futures.Future), the broker uses polling.

        The class needs to be thread-safe for the concurrent.futures.Future case.

        Args:
            outbound: a thread-safe blocking queue to which the request should be forwarded to
            inbound: a thread-safe blocking queue from which the responses will come
            req_key: a function that computes the key of the request, which is used to match the responses
            resp_key: a function that computes the key of the response, which is used to match the requests
            executor: a thread pool for running any blocking code
        """
        logger.trace("init broker")
        self._inbound = inbound
        self._outbound = outbound
        self._req_key = req_key
        self._resp_key = resp_key

        # NOTE: this is used for running the polling tasks for results.
        # The tasks we submit to the executor (or any executor) should NOT wait indefinitely.
        # We must provide it with a way to self-cancelling.
        self._executor = executor

        # Used to signal to the tasks on the executor to quit
        self._shutdown = False

        self._lock = threading.Lock()  # lock for self._keys and self._futures
        self._keys: MutableSet[str] = set()
        self._futures: Dict[str, Optional[RespT]] = dict()

        self._akeys: MutableSet[str] = set()
        self._afutures: Dict[str, asyncio.Future[RespT]] = dict()

        self._task: Optional[asyncio.Task] = None

    def start(self):
        logger.trace("starting broker")
        self._task = asyncio.create_task(self._loop())
        return

    async def shutdown(self):
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _loop(self):
        """The main broker loop.

        The loop listens for any responses and resolves the corresponding futures.
        """
        logger.trace("broker started")
        try:
            async for resp in utils.QueueAgen(self._inbound):
                logger.trace("broker got: {}", resp)
                key = self._resp_key(resp)

                # if the response is for a concurrent.futures.Future,
                # finds/resolves it and return True.
                def update():
                    with self._lock:
                        if key in self._futures:
                            self._futures[key] = resp
                            return True
                        # NOTE: the clean up happens at submission time
                        # See self.submit()
                    return False

                if await asyncio.to_thread(update):
                    continue

                # if the previous step didn't find a corresponding future,
                # looks for the asyncio.Future instead.
                if key in self._afutures:
                    self._afutures[key].set_result(resp)

                    # clean up
                    self._akeys.remove(key)
                    del self._afutures[key]
                    continue

                raise KeyError(f"key not found: {key}")
        finally:
            logger.trace("broker shutting down")
            self._shutdown = True

    async def asubmit(self, req: ReqT) -> asyncio.Future[RespT]:
        """Submits a request for an asyncio.Future."""
        key = self._req_key(req)
        assert key not in self._keys

        f = self._afutures.get(key, None)
        if f is None:
            self._afutures[key] = asyncio.Future()
            f = self._afutures[key]
            self._akeys.add(key)
            # TODO: pyright can't figure out that both alternatives in the union type is individualy type-checked
            await asyncio.to_thread(self._outbound.put, req)  # type: ignore

        return f

    def submit(self, req: ReqT) -> Future[RespT]:
        """Submits a request for a concurrent.futures.Future.

        The future may raise CancelledError if the broker is shutting down.
        """
        key = self._req_key(req)
        assert key not in self._akeys

        def poll():
            with self._lock:
                if key not in self._keys:
                    self._futures[key] = None
                    self._keys.add(key)
                    self._outbound.put(req)

            resp = None
            while resp is None and not self._shutdown:
                while self._futures.get(key, None) is None:
                    time.sleep(1)
                with self._lock:
                    resp = self._futures.get(key, None)
                    if resp is not None:
                        self._keys.remove(key)
                        del self._futures[key]

            if self._shutdown:
                logger.trace("broker polling task shutting down")
                raise CancelledError("shutting down")

            assert resp is not None
            return resp

        return self._executor.submit(poll)


class WorkflowRunFutures:
    """A workflow run listener to be used in the main process.

    It is a high-level interface that wraps a RequestResponseBroker.
    """

    def __init__(
        self,
        *,
        executor: ThreadPoolExecutor,
        broker: RequestResponseBroker["messages.Message", "messages.Message"],
    ):
        self._broker = broker
        self._executor = executor

    def start(self):
        logger.trace("starting main-process workflow run listener")
        self._broker.start()

    async def shutdown(self):
        logger.trace("shutting down main-process workflow run listener")
        await self._broker.shutdown()
        logger.trace("bye: main-process workflow run listener")

    def submit(self, req: SubscribeToWorkflowRunsRequest) -> Future[WorkflowRunEvent]:
        logger.trace("requesting workflow run result: {}", MessageToDict(req))
        f = self._broker.submit(
            messages.Message(_subscribe_to_workflow_run=MessageToDict(req))
        )
        return self._executor.submit(lambda: f.result().workflow_run_event)

    async def asubmit(
        self, req: SubscribeToWorkflowRunsRequest
    ) -> asyncio.Future[WorkflowRunEvent]:
        logger.trace("requesting workflow run result: {}", MessageToDict(req))
        f = await self._broker.asubmit(
            messages.Message(_subscribe_to_workflow_run=MessageToDict(req))
        )
        event: asyncio.Future[WorkflowRunEvent] = asyncio.Future()
        f.add_done_callback(lambda f: event.set_result(f.result().workflow_run_event))
        return event
