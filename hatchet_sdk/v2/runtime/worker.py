import asyncio
import multiprocessing as mp
import multiprocessing.queues as mpq
import multiprocessing.synchronize as mps
import sys
import time
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Dict, List, Optional, TypeVar

import grpc
from google.protobuf import timestamp_pb2
from google.protobuf.json_format import MessageToDict
from loguru import logger

import hatchet_sdk.v2.hatchet as hatchet
import hatchet_sdk.v2.runtime.config as config
import hatchet_sdk.v2.runtime.connection as connection
import hatchet_sdk.v2.runtime.context as context
import hatchet_sdk.v2.runtime.listeners as listeners
import hatchet_sdk.v2.runtime.messages as messages
import hatchet_sdk.v2.runtime.utils as utils
from hatchet_sdk.contracts.dispatcher_pb2 import (
    AssignedAction,
    HeartbeatRequest,
    WorkerLabels,
    WorkerRegisterRequest,
    WorkerRegisterResponse,
    WorkflowRunEvent,
)
from hatchet_sdk.contracts.dispatcher_pb2_grpc import DispatcherStub


# TODO: change it to RuntimeOptions
@dataclass
class WorkerOptions:
    """Options for the runtime behavior of a Runtime."""

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
        logger.trace("init heartbeater")
        self._worker = worker  # used to access worker id
        self._stub = DispatcherStub(connection.ensure_background_channel())

        self.last_heartbeat: int = -1  # unix epoch in seconds
        self.missed = 0
        self.error = 0

        self._task = None

    async def start(self):
        logger.trace("starting heart beater")
        self._task = asyncio.create_task(self._heartbeat())
        while self.last_heartbeat < 0:
            await asyncio.sleep(1)

    async def shutdown(self):
        logger.trace("shutting down heart beater")
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _heartbeat(self):
        """The main heart beater loop."""
        try:
            while True:
                now = int(time.time())
                proto = HeartbeatRequest(
                    workerId=self._worker.id,
                    heartbeatAt=timestamp_pb2.Timestamp(seconds=now),  # TODO
                )
                try:
                    _ = self._stub.Heartbeat(
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
                    if diff > self._worker.options.heartbeat:
                        self.missed += 1
                await asyncio.sleep(self._worker.options.heartbeat)
        except Exception as e:
            logger.exception(e)
            raise

        finally:
            logger.debug("bye")


T = TypeVar("T")


class Worker:
    """The main worker logic for the sidecar process."""

    def __init__(
        self,
        *,
        options: WorkerOptions,
        client: "hatchet.Hatchet",
        inbound: mpq.Queue["messages.Message"],
        outbound: mpq.Queue["messages.Message"],
    ):
        logger.trace("init worker")
        context.ensure_background_context(client=client)

        self.id: Optional[str] = None
        self.options = options

        # the main queues to/from the main process
        self._inbound = inbound
        self._outbound = outbound

        self._heartbeater = HeartBeater(self)

        # used to interrupt the action listener
        # TODO: need to hook this up to the heart beater so that the exceptions from heart beater can interrupt the action listener
        self._action_listener_interrupt: asyncio.Queue[StopAsyncIteration] = (
            asyncio.Queue()
        )
        self._action_listener = listeners.AssignedActionListner(
            worker=self,
            interrupt=self._action_listener_interrupt,
        )

        # the step event forwarder
        self._to_event_listner: asyncio.Queue["messages.Message"] = asyncio.Queue()
        self._event_listner = listeners.StepEventListener(
            inbound=self._to_event_listner
        )

        # the workflow run listener
        self._workflow_run_event_listener = listeners.WorkflowRunEventListener()

        self._main_loop_task = None

    def _register(self) -> str:
        req = self._to_register_proto()
        logger.trace("registering worker: {}", MessageToDict(req))
        resp: WorkerRegisterResponse = (
            context.ensure_background_context().client.dispatcher.client.Register(
                req,
                timeout=30,
                metadata=context.ensure_background_context().client._grpc_metadata(),
            )
        )
        logger.debug("worker registered: {}", MessageToDict(resp))
        return resp.workerId

    async def start(self) -> str:
        logger.trace("starting worker")
        self.id = self._register()

        # NOTE: order matters, we start them in topological order
        self._event_listner.start()
        self._workflow_run_event_listener.start()
        self._action_listener.start(async_on=self._on_assigned_action)

        self._main_loop_task = asyncio.create_task(self._loop())

        await self._heartbeater.start()

        # notify the worker id to the main process
        await asyncio.to_thread(self._outbound.put, messages.Message(worker_id=self.id))

        logger.debug("worker started: {}", self.id)
        return self.id

    async def shutdown(self):
        logger.trace("shutting down worker {}", self.id)

        if self._main_loop_task:
            self._main_loop_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._main_loop_task
                self._main_loop_task = None

        tg: asyncio.Future = asyncio.gather(
            self._heartbeater.shutdown(),
            self._event_listner.shutdown(),
            self._action_listener.shutdown(),
            self._workflow_run_event_listener.shutdown(),
        )
        await tg
        logger.debug("bye: worker {}", self.id)

    async def _loop(self):
        try:
            async for msg in utils.QueueAgen(self._inbound):
                logger.trace("worker received msg: {}", msg)
                match msg.kind:
                    case messages.MessageKind.STEP_EVENT:
                        await self._to_event_listner.put(msg)
                    case messages.MessageKind.SUBSCRIBE_TO_WORKFLOW_RUN:
                        await self._on_workflow_run_subscription(msg)
                    case _:
                        raise NotImplementedError
        except Exception as e:
            logger.exception(e)
            raise
        finally:
            logger.trace("bye: worker")

    async def _on_assigned_action(
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
            await asyncio.to_thread(self._outbound.put, msg)

    async def _on_workflow_run_subscription(self, msg: "messages.Message"):
        def callback(f: asyncio.Future[WorkflowRunEvent]):
            logger.trace("workflow run event future resolved")
            self._outbound.put(
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
    """The worker process logic.

    It has to be a top-level function since it needs to be pickled.
    """
    # TODO: propagate options, debug, etc.
    client = hatchet.Hatchet(config=config, debug=True)

    # TODO: re-configure the loggers based on the options, etc.
    logger.remove()
    logger.add(sys.stdout, level="TRACE")

    # FIXME: the loop is not exiting correctly. It hangs, instead. Investigate why.
    async def loop():
        worker = Worker(
            client=client,
            inbound=inbound,
            outbound=outbound,
            options=options,
        )
        try:
            _ = await worker.start()
            while not await asyncio.to_thread(shutdown.wait, 1):
                pass
            # asyncio.current_task().cancel()
        except Exception as e:
            logger.exception(e)
            raise
        finally:
            with suppress(asyncio.CancelledError):
                await worker.shutdown()
                logger.trace("worker process shuts down")

    asyncio.run(loop(), debug=True)
    logger.trace("bye: worker process")


class WorkerProcess:
    """A wrapper to control the sidecar worker process."""

    def __init__(
        self,
        *,
        config: "config.ClientConfig",
        options: WorkerOptions,
        inbound: mpq.Queue["messages.Message"],
        outbound: mpq.Queue["messages.Message"],
    ):
        self._to_worker = inbound
        self._shutdown_ev = mp.Event()
        self.proc = mp.Process(
            target=_worker_process,
            kwargs={
                "config": config,
                "options": options,
                "inbound": inbound,
                "outbound": outbound,
                "shutdown": self._shutdown_ev,
            },
        )

    def start(self):
        logger.debug("starting worker process")
        self.proc.start()

    def shutdown(self):
        self._shutdown_ev.set()
        logger.debug("worker process shuts down")
