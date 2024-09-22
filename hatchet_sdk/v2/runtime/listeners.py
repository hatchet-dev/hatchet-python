import asyncio
import multiprocessing as mp
import os
import threading
import time
from asyncio.taskgroups import TaskGroup
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Generic, List, Literal, Optional, Set, TypeVar

import grpc
from google.protobuf import timestamp_pb2
from google.protobuf.json_format import MessageToDict, MessageToJson
from loguru import logger

import hatchet_sdk.contracts.dispatcher_pb2
import hatchet_sdk.v2.hatchet as hatchet
import hatchet_sdk.v2.runtime.connection as connection
import hatchet_sdk.v2.runtime.context as context
import hatchet_sdk.v2.runtime.messages as messages
import hatchet_sdk.v2.runtime.utils as utils
import hatchet_sdk.v2.runtime.worker as worker
from hatchet_sdk.contracts.dispatcher_pb2 import (
    ActionType,
    AssignedAction,
    HeartbeatRequest,
    StepActionEvent,
    StepRunResult,
    SubscribeToWorkflowRunsRequest,
    WorkerLabels,
    WorkerListenRequest,
    WorkerRegisterRequest,
    WorkerRegisterResponse,
    WorkerUnsubscribeRequest,
    WorkflowRunEvent,
    WorkflowRunEventType,
)
from hatchet_sdk.contracts.dispatcher_pb2_grpc import DispatcherStub

T = TypeVar("T")


class WorkflowRunEventListener:
    @dataclass
    class Sub:
        id: str
        run_id: str
        future: asyncio.Future[WorkflowRunEvent]

        def __hash__(self):
            return hash(self.id)

    def __init__(self):
        logger.debug("init workflow run event listener")

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

        self._task = None

    def start(self):
        self._task = asyncio.create_task(
            self.loop(), name="workflow run event listener loop"
        )
        logger.debug("started workflow run event listener")

    async def shutdown(self):
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def loop(self):
        try:
            agen = utils.ForeverAgen(self._events, exceptions=(grpc.aio.AioRpcError,))
            async for event in agen:
                if isinstance(event, grpc.aio.AioRpcError):
                    logger.trace("encountered error, retrying: {}", event)
                    await self._resubscribe()

                else:
                    self._by_run_id[event.workflowRunId].future.set_result(event)
                    self._unsubscribe(event.workflowRunId)
        finally:
            logger.debug("bye: workflow run event listner shuts down")

    async def _events(self) -> AsyncGenerator[WorkflowRunEvent]:
        # keep trying until asyncio.CancelledError is raised into this coroutine
        # TODO: handle retry, backoff, etc.
        stub = DispatcherStub(channel=connection.ensure_background_achannel())
        requests = utils.QueueAgen(self._q_request)

        stream: grpc.aio.StreamStreamCall[
            SubscribeToWorkflowRunsRequest, WorkflowRunEvent
        ] = stub.SubscribeToWorkflowRuns(
            requests,
            metadata=context.ensure_background_context().client._grpc_metadata(),
        )
        logger.trace("stream established")
        async for event in stream:
            logger.trace("received workflow run event:\n{}", event)
            assert (
                event.eventType == WorkflowRunEventType.WORKFLOW_RUN_EVENT_TYPE_FINISHED
            )
            yield event

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
        logger.trace("unsubscribing: {}", run_id)
        sub = self._by_run_id.get(run_id, None)
        if sub is None:
            return
        self._subs.remove(sub)
        del self._by_run_id[run_id]


class AssignedActionListner:
    def __init__(self, worker: "worker.Worker", interrupt: asyncio.Queue[T]):
        logger.debug("init assigned action listener")
        self._worker = worker
        self._interrupt = interrupt

        self._task = None

    def start(
        self, async_on: Callable[[AssignedAction | grpc.aio.AioRpcError | T], Any]
    ):
        self._task = asyncio.create_task(self.loop(async_on))
        logger.debug("started assigned action listener")

    async def shutdown(self):
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _action_stream(self) -> AsyncGenerator[AssignedAction]:
        stub = DispatcherStub(connection.ensure_background_achannel())
        proto = WorkerListenRequest(workerId=self._worker.id)
        resp = stub.ListenV2(
            proto,
            metadata=context.ensure_background_context(None).client._grpc_metadata(),
        )
        logger.trace("connection established")
        async for action in resp:
            logger.trace("assigned action:\n{}", MessageToDict(action))
            yield action

    async def listen(self) -> AsyncGenerator[AssignedAction | grpc.aio.AioRpcError | T]:
        try:

            def agen_factory():
                return utils.InterruptableAgen(
                    self._action_stream(), interrupt=self._interrupt, timeout=5
                )

            agen = utils.ForeverAgen(agen_factory, exceptions=(grpc.aio.AioRpcError,))
            async for action in agen:
                if isinstance(action, grpc.aio.AioRpcError):
                    logger.trace("encountered error, retrying: {}", action)
                    yield action
                else:
                    yield action
        finally:
            logger.debug("bye: assigned action listener")

    async def loop(
        self, async_on: Callable[[AssignedAction | grpc.aio.AioRpcError | T], Any]
    ):
        async for event in self.listen():
            await async_on(event)


class StepEventListener:
    def __init__(self, inbound: asyncio.Queue["messages.Message"]):
        logger.debug("init event listener")
        self.inbound = inbound
        self.stub = DispatcherStub(connection.ensure_background_channel())

        self.task = None

    def start(self):
        self.task = asyncio.create_task(self.listen())

    async def shutdown(self):
        if self.task:
            self.task.cancel()
            with suppress(asyncio.CancelledError):
                await self.task
            self.task = None

    async def _message_stream(self) -> AsyncGenerator["messages.Message"]:
        while True:
            msg: "messages.Message" = await self.inbound.get()
            assert msg.kind in [messages.MessageKind.STEP_EVENT]
            logger.trace("event:\n{}", msg)
            yield msg

    async def listen(self):
        try:
            async for msg in self._message_stream():
                match msg.kind:
                    case messages.MessageKind.STEP_EVENT:
                        await self._on_step_event(msg.step_event)
                    case _:
                        raise NotImplementedError(msg.kind)
        except Exception as e:
            logger.exception(e)
            raise
        finally:
            logger.debug("bye: step event listener")

    async def _on_step_event(self, e: StepActionEvent):
        # TODO: need retry
        logger.trace("emit step action:\n{}", MessageToDict(e))
        resp = await asyncio.to_thread(
            self.stub.SendStepActionEvent,
            e,
            metadata=context.ensure_background_context().client._grpc_metadata(),
        )
        logger.trace(resp)
