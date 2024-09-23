import asyncio
from collections.abc import AsyncGenerator, Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Dict, Generic, Set, TypeVar

import grpc
from google.protobuf.json_format import MessageToDict
from loguru import logger

import hatchet_sdk.v2.runtime.connection as connection
import hatchet_sdk.v2.runtime.context as context
import hatchet_sdk.v2.runtime.messages as messages
import hatchet_sdk.v2.runtime.utils as utils
import hatchet_sdk.v2.runtime.worker as worker
from hatchet_sdk.contracts.dispatcher_pb2 import (
    AssignedAction,
    StepActionEvent,
    SubscribeToWorkflowRunsRequest,
    WorkerListenRequest,
    WorkflowRunEvent,
    WorkflowRunEventType,
)
from hatchet_sdk.contracts.dispatcher_pb2_grpc import DispatcherStub


class WorkflowRunEventListener:
    """A multiplexing workflow run event listener. It should only be used in the sidecar process."""

    @dataclass
    class Sub:
        """A subscription for a workflow run. This is only to be used in the sidecar process."""

        id: str  # TODO: the id is not used right now since one can only subscribe a run_id once.
        run_id: str
        future: asyncio.Future[WorkflowRunEvent]

        def __hash__(self):
            return hash(self.id)

    def __init__(self):
        logger.trace("init workflow run event listener")

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
        logger.trace("starting workflow run event listener")
        self._task = asyncio.create_task(
            self._loop(), name="workflow run event listener loop"
        )

    async def shutdown(self):
        logger.trace("shutting down workflow run event listener")
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _loop(self):
        """The main listener loop.

        The loop forwards subscription requests over the grpc stream to the server while giving
        out a future to the caller. Then it listens for workflow run events and resolves the futures.
        """
        logger.trace("started workflow run event listener")
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
            logger.trace("bye: workflow run event listner shuts down")

    async def _events(self) -> AsyncGenerator[WorkflowRunEvent]:
        """The async generator backed by server-streamed WorkflowRunEvents."""
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
            logger.trace("received workflow run event: {}", MessageToDict(event))
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
            return self._by_run_id[run_id]
        logger.trace("subscribing: {}", run_id)
        await self._q_request.put(SubscribeToWorkflowRunsRequest(workflowRunId=run_id))
        sub = self.Sub(id=str(self._counter), run_id=run_id, future=asyncio.Future())
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


# TODO: use better generics with Python >= 3.12
T = TypeVar("T")


class AssignedActionListner(Generic[T]):
    """An assigned action listener that runs a callback on every server-streamed assigned actions."""

    def __init__(self, *, worker: "worker.Worker", interrupt: asyncio.Queue[T]):
        logger.trace("init assigned action listener")

        # used to get the worker id, which is not immediately available.
        self._worker = worker

        # used to interrupt the action listener
        self._interrupt = interrupt

        self._task = None

    def start(
        self, async_on: Callable[[AssignedAction | grpc.aio.AioRpcError | T], Any]
    ):
        """Starts the assigned action listener loop.

        Args:
            async_on: the callback to be invoked when an assigned action is received.
        """
        logger.trace("starting assigned action listener")
        self._task = asyncio.create_task(self._loop(async_on))

    async def shutdown(self):
        logger.trace("shutting down assigned action listener")
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _action_stream(self) -> AsyncGenerator[AssignedAction]:
        """The async generator backed by the server-streamed assigend actions."""
        stub = DispatcherStub(connection.ensure_background_achannel())
        proto = WorkerListenRequest(workerId=self._worker.id)
        resp = stub.ListenV2(
            proto,
            metadata=context.ensure_background_context(None).client._grpc_metadata(),
        )
        logger.trace("connection established")
        async for action in resp:
            logger.trace("assigned action: {}", MessageToDict(action))
            yield action

    async def _listen(
        self,
    ) -> AsyncGenerator[AssignedAction | grpc.aio.AioRpcError | T]:
        """The wrapped assigned action async generator that handles retries, etc."""

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

    async def _loop(
        self, async_on: Callable[[AssignedAction | grpc.aio.AioRpcError | T], Any]
    ):
        """The main assigned action listener loop."""
        try:
            logger.trace("started assigned action listener")
            async for event in self._listen():
                await async_on(event)
        finally:
            logger.trace("bye: assigned action listener")


class StepEventListener:
    """A step event listener that forwards the step event from the main process to the server."""

    def __init__(self, *, inbound: asyncio.Queue["messages.Message"]):
        logger.trace("init step event listener")
        self._inbound = inbound
        self._stub = DispatcherStub(connection.ensure_background_channel())
        self._task = None

    def start(self):
        logger.trace("starting step event listener")
        self._task = asyncio.create_task(self._listen())

    async def shutdown(self):
        logger.trace("shutting down step event listener")
        if self._task:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _message_stream(self) -> AsyncGenerator["messages.Message"]:
        while True:
            msg: "messages.Message" = await self._inbound.get()
            assert msg.kind in [messages.MessageKind.STEP_EVENT]
            logger.trace("event: {}", msg)
            yield msg

    async def _listen(self):
        """The main listener loop."""
        logger.trace("step event listener started")
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
        logger.trace("emit step action: {}", MessageToDict(e))
        resp = await asyncio.to_thread(
            self._stub.SendStepActionEvent,
            e,
            metadata=context.ensure_background_context().client._grpc_metadata(),
        )
        logger.trace("resp: {}", MessageToDict(resp))
