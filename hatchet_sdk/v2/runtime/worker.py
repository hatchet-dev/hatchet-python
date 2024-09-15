import asyncio
import multiprocessing as mp
import os
import threading
import time
from collections.abc import AsyncGenerator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set

import grpc
from google.protobuf import timestamp_pb2
from google.protobuf.json_format import MessageToDict, MessageToJson
from loguru import logger

import hatchet_sdk.contracts.dispatcher_pb2
import hatchet_sdk.v2.hatchet as hatchet
import hatchet_sdk.v2.runtime.connection as connection
import hatchet_sdk.v2.runtime.messages as messages
from hatchet_sdk.contracts.dispatcher_pb2 import (
    ActionType,
    AssignedAction,
    HeartbeatRequest,
    StepActionEvent,
    WorkerLabels,
    WorkerListenRequest,
    WorkerRegisterRequest,
    WorkerRegisterResponse,
    WorkerUnsubscribeRequest,
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


class _ActionListner:
    def __init__(self, worker: "Worker"):
        logger.debug("init action listener")
        self.worker = worker
        self.attempt = 0
        self.stub = DispatcherStub(
            connection.new_conn(self.worker.client.config, aio=True)
        )

    async def listen(self):
        resp = None
        try:
            # It will exit the loop when asyncio.CancelledError is
            # raised by calling task.cancel() from outside.
            while True:
                proto = WorkerListenRequest(workerId=self.worker.id)
                try:
                    resp = self.stub.ListenV2(
                        proto, metadata=self.worker._grpc_metadata()
                    )
                    logger.trace("connection established")
                    async for event in resp:
                        msg = messages.Message(_action=MessageToDict(event))
                        logger.trace("assigned action:\n{}", msg)
                        await asyncio.to_thread(self.queue.put, msg)

                    resp = None
                    self.attempt += 1
                except grpc.aio.AioRpcError as e:
                    logger.warning(e)

                # TODO: expotential backoff, retry limit, etc

        finally:
            if resp:
                resp.cancel()
            logger.debug("bye")

    @property
    def queue(self):
        return self.worker.outbound


class _EventListner:
    def __init__(self, worker: "Worker"):
        logger.debug("init event listener")
        self.worker = worker
        self.stub = DispatcherStub(connection.new_conn(self.worker.client.config))

    async def listen(self):
        try:
            while True:
                msg: "messages.Message" = await asyncio.to_thread(self.queue.get)
                logger.trace("event:\n{}", msg)
                assert msg.kind in [messages.MessageKind.STEP_EVENT]
                match msg.kind:
                    case messages.MessageKind.STEP_EVENT:
                        await self.on_step_event(msg.step_event)
                    case _:
                        raise NotImplementedError(msg.kind)
        finally:
            logger.debug("bye")

    async def on_step_event(self, e: StepActionEvent):
        # TODO: need retry
        logger.trace("emit step action:\n{}", MessageToDict(e))
        resp = await asyncio.to_thread(self.stub.SendStepActionEvent, e, metadata=self.worker._grpc_metadata())
        logger.trace(resp)

    @property
    def queue(self):
        return self.worker.inbound


class Worker:
    def __init__(
        self,
        client: "hatchet.Hatchet",
        inbound: mp.Queue,
        outbound: mp.Queue,
        options: WorkerOptions,
    ):
        logger.debug("init worker")
        self.options = options
        self.client = client
        self.status = WorkerStatus.UNKNOWN
        self.id: Optional[str] = None
        self.inbound = inbound
        self.outbound = outbound

        self._heartbeater = _HeartBeater(self)
        self._heartbeater_task: Optional[asyncio.Task] = None
        self._action_listener = _ActionListner(self)
        self._action_listener_task: Optional[asyncio.Task] = None
        self._event_listner = _EventListner(self)
        self._event_listner_task: Optional[asyncio.Task] = None

    def _register(self) -> str:
        req = self._to_register_proto()
        logger.trace("registering worker:\n{}", req)
        resp: WorkerRegisterResponse = self.client.dispatcher.client.Register(
            req,
            timeout=30,
            metadata=self._grpc_metadata(),
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
        self._action_listener_task = asyncio.create_task(
            self._action_listener.listen(), name="action_listener"
        )
        while True:
            if self._heartbeater.last_heartbeat > 0:
                logger.debug("worker started: {}", self.id)
                return
            await asyncio.sleep(0.1)

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

    def _grpc_metadata(self):
        return [("authorization", f"bearer {self.client.config.token}")]

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
