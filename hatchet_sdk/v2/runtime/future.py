import asyncio
import multiprocessing as mp
import multiprocessing.queues as mpq
import queue
import threading
import time
from collections.abc import Callable, MutableSet
from concurrent.futures import Future, ThreadPoolExecutor
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
        """A broker that can send a request and returns a future for the response.

        The broker loop runs forever and quits upon asyncio.CancelledError.

        Args:
            outbound: a thread-safe blocking queue to which the request should be forwarded to
            inbound: a thread-safe blocking queue from which the responses will come
            req_key: a function that computes the key of the request, which is used to match the responses
            resp_key: a function that computes the key of the response, which is used to match the requests
            executor: a thread pool for running any blocking code
        """
        self._inbound = inbound
        self._outbound = outbound
        self._req_key = req_key
        self._resp_key = resp_key

        # NOTE: this is used for running the polling tasks for results.
        # The tasks we submit to the (any) executor should NOT wait indefinitely.
        # We must provide it with a way to self-cancelling.
        self._executor = executor

        # Used to signal to the tasks on the executor to quit
        self._shutdown = False

        self._lock = threading.Lock()  # lock for self._keys and self._futures
        self._keys: MutableSet[str] = set()
        self._futures: Dict[str, Optional[RespT]] = dict()

        self._akeys: MutableSet[str] = set()
        self._afutures: Dict[str, asyncio.Future[RespT]] = dict()

        self.loop_task: Optional[asyncio.Task] = None

    def start(self):
        logger.trace("starting broker on {}", threading.get_native_id())
        self.loop_task = asyncio.create_task(self.loop())
        return

    async def shutdown(self):
        self.loop_task.cancel()
        with suppress(asyncio.CancelledError):
            await self.loop_task

    async def loop(self):
        try:
            async for resp in utils.QueueAgen(self._inbound):
                logger.trace("broker got: {}", resp)
                key = self._resp_key(resp)

                def update():
                    with self._lock:
                        if key in self._futures:
                            self._futures[key] = resp
                            return True
                    return False

                if await asyncio.to_thread(update):
                    continue

                if key in self._afutures:
                    self._afutures[key].set_result(resp)
                    self._akeys.remove(key)
                    del self._afutures[key]
                    continue

                raise KeyError(f"key not found: {key}")
        finally:
            self._shutdown = True

    async def asubmit(self, req: ReqT) -> asyncio.Future[RespT]:
        key = self._req_key(req)
        assert key not in self._keys

        f = None
        if key not in self._akeys:
            self._afutures[key] = asyncio.Future()
            f = self._afutures[key]
            self._akeys.add(key)
            await asyncio.to_thread(self._outbound.put, key)

        return f

    def submit(self, req: ReqT) -> Future[RespT]:
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

            return resp

        return self._executor.submit(poll)


class WorkflowRunFutures:
    def __init__(
        self,
        broker: RequestResponseBroker["messages.Message", "messages.Message"],
    ):
        self._broker = broker
        self._thread = None

    def start(self):
        logger.trace("starting workflow run wrapper on {}", threading.get_native_id())
        self._thread = threading.Thread(target=asyncio.run, args=[self._broker.start()], name="workflow run event broker")
        self._thread.start()

    async def shutdown(self):
        del self._thread

    def submit(self, req: SubscribeToWorkflowRunsRequest) -> Future[WorkflowRunEvent]:
        logger.trace("requesting workflow run result: {}", req)
        f = self._broker.submit(
            messages.Message(_subscribe_to_workflow_run=MessageToDict(req))
        )
        logger.trace("submitted")
        return self._broker._executor.submit(lambda: f.result().workflow_run_event)

    async def asubmit(
        self, req: SubscribeToWorkflowRunsRequest
    ) -> asyncio.Future[WorkflowRunEvent]:
        logger.trace("requesting workflow run result: {}", req)
        f = await self._broker.asubmit(req)
        event: asyncio.Future[WorkflowRunEvent] = asyncio.Future()
        f.add_done_callback(lambda f: event.set_result(f.result().workflow_run_event))
        return event
