import asyncio
from asyncio.taskgroups import TaskGroup
import multiprocessing as mp
import os
import threading
import time
from collections.abc import AsyncGenerator, Generator, AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Generic, List, Optional, Set, TypeVar, Literal
from contextlib import suppress


import grpc
from google.protobuf import timestamp_pb2
from google.protobuf.json_format import MessageToDict, MessageToJson
from loguru import logger

import hatchet_sdk.contracts.dispatcher_pb2
import hatchet_sdk.v2.hatchet as hatchet
import hatchet_sdk.v2.runtime.connection as connection
import hatchet_sdk.v2.runtime.context as context
import hatchet_sdk.v2.runtime.messages as messages
import hatchet_sdk.v2.runtime.worker as worker
from hatchet_sdk.contracts.dispatcher_pb2 import (
    ActionType,
    AssignedAction,
    HeartbeatRequest,
    StepActionEvent,
    SubscribeToWorkflowRunsRequest,
    WorkerLabels,
    WorkerListenRequest,
    WorkerRegisterRequest,
    WorkerRegisterResponse,
    WorkerUnsubscribeRequest,
    WorkflowRunEventType,
    StepRunResult,
    WorkflowRunEvent,
)
from hatchet_sdk.contracts.dispatcher_pb2_grpc import DispatcherStub


T = TypeVar("T")


# class _GrpcAioListnerBase(Generic[T]):
#     def __init__(self):
#         self.attempt = 0
#         self.interrupt = False

#     def channel(self):
#         raise NotImplementedError()

#     def stub(self, channel):
#         raise NotImplementedError

#     def request(self, stub):
#         raise NotImplementedError()

#     def interrupt(self):
#         self.interrupt = True

#     async def listen(self) -> AsyncGenerator[T]:
#         while True:
#             stub: DispatcherStub = self.stub()
#             stub.ListenV2

#             stream = None
#             try:
#                 stream = self.request(stub)
#                 async for msg in stream:
#                     if not self.interrupt:
#                         yield msg
#             except grpc.aio.AioRpcError as e:
#                 logger.warning(e)
#             finally:
#                 if stream is not None:
#                     stream.cancel()
#                 self.interrupt = False
#                 self.attempt += 1

#     def read(self) -> Generator[T]:
#         stub = self.stub()
#         stream = self.request(stub)


class WorkflowRunEventListener:

    @dataclass
    class Sub:
        id: str
        run_id: str
        future: asyncio.Future[List[StepRunResult]]

        def __hash__(self):
            return hash(self.id)

    def __init__(self):
        logger.trace("init workflow run event listener")
        self._token = context.ensure_background_context(None).client.config.token

        # the set of active subscriptions
        self._subs: Set[WorkflowRunEventListener.Sub] = set()

        # counter used for generating subscription ids
        # not thread safe
        self._counter = 0

        # index from run id to subscriptions
        self._by_run_id: Dict[str, WorkflowRunEventListener.Sub] = dict()

        # queue used for iterating requests
        # must be created inside the loop
        self._q_request: asyncio.Queue[SubscribeToWorkflowRunsRequest] = asyncio.Queue()

        self._events_agen: AsyncGenerator[WorkflowRunEvent] = self._events()

    async def loop(self):
        await self._events_agen.aclose()
        async for event in self._events_agen:
            assert (
                event.eventType == WorkflowRunEventType.WORKFLOW_RUN_EVENT_TYPE_FINISHED
            )
            self._by_run_id[event.workflowRunId].future.set_result(list(event.results))
            self._unsubscribe(event.workflowRunId)

    async def _events(self) -> AsyncGenerator[WorkflowRunEvent]:

        # keep trying until asyncio.CancelledError is raised into this coroutine
        # TODO: handle retry, backoff, etc.
        stub = DispatcherStub(channel=connection.ensure_background_achannel())
        agen = self._requests()
        while True:
            try:
                stream: grpc.aio.StreamStreamCall[
                    SubscribeToWorkflowRunsRequest, WorkflowRunEvent
                ] = stub.SubscribeToWorkflowRuns(
                    agen,
                    metadata=[("authorization", f"bearer {self._token}")],
                )
                logger.trace("stream established")
                async for event in stream:
                    logger.trace(event)
                    yield event

            except grpc.aio.AioRpcError as e:
                logger.exception(e)
                pass

            await self._resubscribe()

    async def _requests(self) -> AsyncGenerator[SubscribeToWorkflowRunsRequest]:
        while True:
            req = await self._q_request.get()
            logger.trace("client streaming req to server: {}", MessageToDict(req))
            yield req

    async def _resubscribe(self):
        logger.trace("re-subscribing all")
        async with asyncio.TaskGroup() as tg:
            for id in self._by_run_id.keys():
                tg.create_task(
                    self._q_request.put(
                        SubscribeToWorkflowRunsRequest(workflowRunId=id)
                    )
                )

    async def subscribe(self, run_id: str) -> "WorkflowRunEventListener.Sub":
        if run_id in self._by_run_id:
            return
        logger.trace("subscribing: {}", run_id)
        await self._q_request.put(SubscribeToWorkflowRunsRequest(workflowRunId=run_id))
        sub = self.Sub(id=self._counter, run_id=run_id, future=asyncio.Future())
        self._subs.add(sub)
        self._by_run_id[run_id] = sub
        self._counter += 1
        return sub

    def _unsubscribe(self, run_id: str):
        logger.trace("unsubscribing {}", run_id)
        sub = self._by_run_id.get(run_id, None)
        if sub is None:
            return
        self._subs.remove(sub)
        del self._by_run_id[run_id]
