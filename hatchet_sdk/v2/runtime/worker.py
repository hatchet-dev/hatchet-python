import asyncio
import multiprocessing as mp
import os
import threading
import time
from collections.abc import AsyncGenerator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Generic, List, Optional, Set, TypeVar

import grpc
from google.protobuf import timestamp_pb2
from google.protobuf.json_format import MessageToDict, MessageToJson
from loguru import logger

import hatchet_sdk.contracts.dispatcher_pb2
import hatchet_sdk.v2.hatchet as hatchet
import hatchet_sdk.v2.runtime.connection as connection
import hatchet_sdk.v2.runtime.messages as messages
import hatchet_sdk.v2.runtime.listeners as listeners
import hatchet_sdk.v2.runtime.context as context
import hatchet_sdk.v2.runtime.utils as utils


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
    StepRunResult,
    WorkerUnsubscribeRequest,
    WorkflowRunEvent,
)
from hatchet_sdk.contracts.dispatcher_pb2_grpc import DispatcherStub


@dataclass
class WorkerOptions:
    name: str
    actions: List[str]
    slots: int = 5
    debug: bool = False
    labels: Dict[str, str | int] = field(default_factory=dict)
    heartbeat: int = 4  # heartbeat period in seconds

    @property
    def labels_proto(self) -> Dict[str, WorkerLabels]:
        ret = dict()
        for k, v in self.labels.items():
            if isinstance(v, int):
                ret[k] = WorkerLabels(intValue=v)
            else:
                ret[k] = WorkerLabels(strValue=str(v))
        return ret


class WorkerStatus(Enum):
    UNKNOWN = 1
    REGISTERED = 2
    # STARTING = 2
    HEALTHY = 3
    UNHEALTHY = 4


class _HeartBeater:
    def __init__(self, worker: "Worker"):
        logger.debug("init heartbeater")
        self.worker = worker
        self.last_heartbeat: int = -1  # unix epoch in seconds
        self.stub = DispatcherStub(
            connection.new_conn(self.worker.client.config, aio=False)
        )
        self.missed = 0
        self.error = 0

    async def heartbeat(self):
        try:
            # It will exit the loop when a asyncio.CancelledError is raised
            # by calling task.cancel() from outside.
            while True:
                now = int(time.time())
                proto = HeartbeatRequest(
                    workerId=self.worker.id,
                    heartbeatAt=timestamp_pb2.Timestamp(seconds=now),  # TODO
                )
                try:
                    _ = self.stub.Heartbeat(
                        proto, timeout=5, metadata=self.worker._grpc_metadata()
                    )
                    logger.trace("heartbeat")
                except grpc.RpcErrors:
                    # TODO
                    self.error += 1

                if self.last_heartbeat < 0:
                    self.last_heartbeat = now
                    self.status = WorkerStatus.HEALTHY
                else:
                    diff = proto.heartbeatAt.seconds - self.last_heartbeat
                    if diff > self.worker.options.heartbeat:
                        self.missed += 1
                await asyncio.sleep(self.worker.options.heartbeat)

        finally:
            logger.debug("bye")


T = TypeVar("T")


class Worker:
    def __init__(
        self,
        client: "hatchet.Hatchet",
        inbound: mp.Queue["messages.Message"],
        outbound: mp.Queue["messages.Message"],
        options: WorkerOptions,
    ):
        logger.debug("init worker")
        context.ensure_background_context(client=client)

        self.options = options
        self.client = client
        self.id: Optional[str] = None
        self.inbound = inbound
        self.outbound = outbound
        self.status = WorkerStatus.UNKNOWN

        self._heartbeater = _HeartBeater(self)
        self._heartbeater_task: Optional[asyncio.Task] = None

        self._action_listener_interrupt: asyncio.Queue[StopAsyncIteration] = (
            asyncio.Queue()
        )
        self._action_listener = listeners.AssignedActionListner(
            worker=self, interrupt=self._action_listener_interrupt
        )

        self._event_listner_q: asyncio.Queue["messages.Message"] = asyncio.Queue()
        self._event_listner = listeners.StepEventListener(self._event_listner_q)
        self._event_listner_task: Optional[asyncio.Task] = None

        self._workflow_run_event_listener = listeners.WorkflowRunEventListener()
        self._workflow_run_event_listener_task: Optional[asyncio.Task] = None

    def _register(self) -> str:
        req = self._to_register_proto()
        logger.trace("registering worker:\n{}", req)
        resp: WorkerRegisterResponse = self.client.dispatcher.client.Register(
            req,
            timeout=30,
            metadata=context.ensure_background_context().client._grpc_metadata(),
        )
        logger.debug("worker registered:\n{}", MessageToDict(resp))
        self.id = resp.workerId
        self.status = WorkerStatus.REGISTERED
        return resp.workerId

    async def start(self):
        logger.trace("starting worker")
        self._register()
        self._heartbeat_task = asyncio.create_task(
            self._heartbeater.heartbeat(), name="heartbeater"
        )
        self._event_listner_task = asyncio.create_task(
            self._event_listner.listen(), name="event_listener"
        )
        while True:
            if self._heartbeater.last_heartbeat > 0:
                logger.debug("worker started: {}", self.id)
                return
            await asyncio.sleep(0.1)

    async def server_message_loop(self):
        async for action in self._action_listener.listen():
            if isinstance(action, StopAsyncIteration):
                # interrupted, ignore
                pass
            elif isinstance(action, grpc.aio.AioRpcError):
                # errored out, ignored
                pass
            else:
                assert isinstance(action, AssignedAction)
                msg = messages.Message(_action=MessageToDict(action))
                await asyncio.to_thread(self.outbound.put, msg)

    async def client_message_loop(self):
        async for msg in utils.QueueAgen(self.inbound):
            match msg.kind:
                case messages.MessageKind.STEP_EVENT:
                    await asyncio.to_thread(self._event_listner_q.put, msg)
                case messages.MessageKind.SUBSCRIBE_TO_WORKFLOW_RUN:
                    await self.on_workflow_run_subscription(msg)

    async def on_workflow_run_subscription(self, msg: "messages.Message"):
        def callback(f: asyncio.Future[WorkflowRunEvent]):
            self.outbound.put(
                messages.Message(_workflow_run_event=MessageToDict(f.result()))
            )

        sub = await self._workflow_run_event_listener.subscribe(
            msg.subscribe_to_workflow_run.workflowRunId
        )
        sub.future.add_done_callback(callback)

    async def shutdown(self):
        logger.trace("shutting down worker {}", self.id)
        tg: asyncio.Future = asyncio.gather(
            self._heartbeat_task, self._action_listener_task, self._event_listner_task
        )
        tg.cancel()
        try:
            await tg
        except asyncio.CancelledError:
            logger.debug("bye")

    def _to_register_proto(self) -> WorkerRegisterRequest:
        options = self.options
        proto = WorkerRegisterRequest(
            workerName=options.name,
            services=["default"],
            actions=list(options.actions),
            maxRuns=options.slots,
            labels=options.labels_proto,
        )
        return proto
