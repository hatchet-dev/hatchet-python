import sys
import asyncio
import multiprocessing as mp
import multiprocessing.queues as mpq
import multiprocessing.synchronize as mps
import os
import threading
import time
from collections.abc import AsyncGenerator
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Generic, List, Optional, Set, TypeVar
import logging
import grpc
from google.protobuf import timestamp_pb2
from google.protobuf.json_format import MessageToDict, MessageToJson
from loguru import logger

import hatchet_sdk.contracts.dispatcher_pb2
import hatchet_sdk.v2.hatchet as hatchet
import hatchet_sdk.v2.runtime.config as config
import hatchet_sdk.v2.runtime.connection as connection
import hatchet_sdk.v2.runtime.context as context
import hatchet_sdk.v2.runtime.listeners as listeners
import hatchet_sdk.v2.runtime.messages as messages
import hatchet_sdk.v2.runtime.utils as utils
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


class HeartBeater:
    def __init__(self, worker: "Worker"):
        logger.debug("init heartbeater")
        self.worker = worker
        self.last_heartbeat: int = -1  # unix epoch in seconds
        self.stub = DispatcherStub(connection.ensure_background_channel())
        self.missed = 0
        self.error = 0

        self.task = None

    async def start(self):
        self.task = asyncio.create_task(self.heartbeat())
        while self.last_heartbeat < 0:
            await asyncio.sleep(1)

    async def shutdown(self):
        if self.task:
            self.task.cancel()
            with suppress(asyncio.CancelledError):
                await self.task
            self.task = None

    async def heartbeat(self):
        try:
            while True:
                now = int(time.time())
                proto = HeartbeatRequest(
                    workerId=self.worker.id,
                    heartbeatAt=timestamp_pb2.Timestamp(seconds=now),  # TODO
                )
                try:
                    _ = self.stub.Heartbeat(
                        proto,
                        timeout=5,
                        metadata=context.ensure_background_context().client._grpc_metadata(),
                    )
                    logger.debug("heartbeat")
                except grpc.RpcError as e:
                    # TODO
                    logger.exception(e)
                    self.error += 1

                if self.last_heartbeat < 0:
                    self.last_heartbeat = now
                else:
                    diff = proto.heartbeatAt.seconds - self.last_heartbeat
                    if diff > self.worker.options.heartbeat:
                        self.missed += 1
                await asyncio.sleep(self.worker.options.heartbeat)
        except Exception as e:
            logger.exception(e)
            raise

        finally:
            logger.debug("bye")


T = TypeVar("T")


class Worker:
    def __init__(
        self,
        *,
        options: WorkerOptions,
        client: "hatchet.Hatchet",
        inbound: mpq.Queue["messages.Message"],
        outbound: mpq.Queue["messages.Message"],
    ):
        logger.debug("init worker")
        context.ensure_background_context(client=client)

        self.id: Optional[str] = None

        self.options = options
        self.inbound = inbound
        self.outbound = outbound

        self._heartbeater = HeartBeater(self)

        self._action_listener_interrupt: asyncio.Queue[StopAsyncIteration] = (
            asyncio.Queue()
        )
        self._action_listener = listeners.AssignedActionListner(
            worker=self,
            interrupt=self._action_listener_interrupt,
        )

        self._to_event_listner: asyncio.Queue["messages.Message"] = asyncio.Queue()
        self._event_listner = listeners.StepEventListener(self._to_event_listner)

        self._workflow_run_event_listener = listeners.WorkflowRunEventListener()

        self.main_loop_task = None

    def _register(self) -> str:
        req = self._to_register_proto()
        logger.trace("registering worker:\n{}", req)
        resp: (
            WorkerRegisterResponse
        ) = context.ensure_background_context().client.dispatcher.client.Register(
            req,
            timeout=30,
            metadata=context.ensure_background_context().client._grpc_metadata(),
        )
        logger.debug("worker registered:\n{}", MessageToDict(resp))
        return resp.workerId

    async def start(self) -> str:
        logger.trace("starting worker")
        self.id = self._register()
        self._event_listner.start()
        self._workflow_run_event_listener.start()
        self._action_listener.start(async_on=self.on_assigned_action)

        self.main_loop_task = asyncio.create_task(self.loop())

        await self._heartbeater.start()
        await asyncio.to_thread(self.outbound.put, messages.Message(worker_id=self.id))
        logger.debug("worker started: {}", self.id)
        return self.id

    async def shutdown(self):
        logger.trace("shutting down worker {}", self.id)

        if self.main_loop_task:
            self.main_loop_task.cancel()
            with suppress(asyncio.CancelledError):
                await self.main_loop_task
                self.main_loop_task = None

        tg: asyncio.Future = asyncio.gather(
            self._heartbeater.shutdown(),
            self._event_listner.shutdown(),
            self._action_listener.shutdown(),
            self._workflow_run_event_listener.shutdown(),
        )
        await tg
        logger.debug("bye")

    async def loop(self):
        try:
            async for msg in utils.QueueAgen(self.inbound):
                logger.trace("worker received msg: {}", msg)
                match msg.kind:
                    case messages.MessageKind.STEP_EVENT:
                        await self._to_event_listner.put(msg)
                    case messages.MessageKind.SUBSCRIBE_TO_WORKFLOW_RUN:
                        await self.on_workflow_run_subscription(msg)
                    case _:
                        raise NotImplementedError
        except Exception as e:
            logger.exception(e)
            raise
        finally:
            logger.trace("bye: worker")

    async def on_assigned_action(
        self, action: StopAsyncIteration | grpc.aio.AioRpcError | AssignedAction
    ):
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

    async def on_workflow_run_subscription(self, msg: "messages.Message"):
        def callback(f: asyncio.Future[WorkflowRunEvent]):
            logger.trace("workflow run event future resolved")
            self.outbound.put(
                messages.Message(_workflow_run_event=MessageToDict(f.result()))
            )

        sub = await self._workflow_run_event_listener.subscribe(
            msg.subscribe_to_workflow_run.workflowRunId
        )
        sub.future.add_done_callback(callback)

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


def _worker_process(
    config: "config.ClientConfig",
    options: WorkerOptions,
    inbound: mpq.Queue["messages.Message"],
    outbound: mpq.Queue["messages.Message"],
    shutdown: mps.Event,
):
    client = hatchet.Hatchet(config=config, debug=True)
    logger.remove()
    logger.add(sys.stdout, level="TRACE")

    async def loop():
        worker = Worker(
            client=client,
            inbound=inbound,
            outbound=outbound,
            options=options,
        )
        try:
            id = await worker.start()
            while not await asyncio.to_thread(shutdown.wait, 1):
                pass
            asyncio.current_task().cancel()
        except Exception as e:
            logger.exception(e)
            raise
        finally:
            with suppress(asyncio.CancelledError):
                await worker.shutdown()
                logger.trace("worker process shuts down")

    asyncio.run(loop(), debug=True)
    logger.trace("here")


class WorkerProcess:
    def __init__(
        self,
        *,
        config: "config.ClientConfig",
        options: WorkerOptions,
        inbound: mpq.Queue["messages.Message"],
        outbound: mpq.Queue["messages.Message"],
    ):
        self.to_worker = inbound
        self.shutdown_ev = mp.Event()
        self.proc = mp.Process(
            target=_worker_process,
            kwargs={
                "config": config,
                "options": options,
                "inbound": inbound,
                "outbound": outbound,
                "shutdown": self.shutdown_ev,
            },
        )

    def start(self):
        logger.debug("starting worker process")
        self.proc.start()

    def shutdown(self):
        self.shutdown_ev.set()
        logger.debug("worker process shuts down")
