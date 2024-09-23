import asyncio
import multiprocessing.queues as mpq
import queue
import time
import traceback
from contextlib import suppress
from typing import Dict, Optional, Tuple, TypeAlias, TypeVar

from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from loguru import logger

import hatchet_sdk.v2.callable as callable
import hatchet_sdk.v2.runtime.messages as messages
import hatchet_sdk.v2.runtime.registry as registry
import hatchet_sdk.v2.runtime.utils as utils
from hatchet_sdk.contracts.dispatcher_pb2 import (
    ActionType,
    AssignedAction,
    StepActionEvent,
    StepActionEventType,
)


def _timestamp():
    ns = time.time_ns()
    return Timestamp(seconds=int(ns // 1e9), nanos=int(ns % 1e9))


def _format_exc(e: Exception):
    trace = "".join(traceback.format_exception(e))
    return "\n".join([str(e), trace])


async def _invoke(
    action: AssignedAction, registry: Dict[str, "callable.HatchetCallableBase"]
) -> Tuple[str, None] | Tuple[None, Exception]:
    key = action.actionId
    # TODO: handle cases when it's not registered more gracefully
    fn: "callable.HatchetCallableBase" = registry[key]
    logger.trace("invoking: {}", repr(fn))
    try:
        if isinstance(fn, callable.HatchetCallable):
            logger.trace("invoking {} on a separate thread", fn._hatchet.name)
            return await asyncio.to_thread(fn._run, action), None
        elif isinstance(fn, callable.HatchetAwaitable):
            return await fn._run(action), None
        else:
            raise NotImplementedError(f"unsupported callable case: {type(fn)}")
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.exception(e)
        return None, e


# TODO: Use better generics for Python >= 3.12
T = TypeVar("T")
_ThreadSafeQueue: TypeAlias = queue.Queue[T] | mpq.Queue[T]


class RunnerLoop:
    def __init__(
        self,
        *,
        reg: "registry.ActionRegistry",
        inbound: _ThreadSafeQueue["messages.Message"],  # inbound queue, not owned
        outbound: _ThreadSafeQueue["messages.Message"],  # outbound queue, not owned
    ):
        logger.trace("init runner loop")
        self.worker_id: Optional[str] = None

        self._registry: Dict[str, "callable.HatchetCallableBase"] = reg.registry
        self._inbound = inbound
        self._outbound = outbound
        self._loop_task: Optional[asyncio.Task] = None

        # a dict from StepRunId to its tasks
        self._tasks: Dict[str, asyncio.Task] = dict()

    def start(self):
        logger.trace("starting runner loop")
        self._loop_task = asyncio.create_task(self._loop(), name="runner loop")

    async def shutdown(self):
        logger.trace("shutting down runner loop")
        # finishing all the tasks
        t = asyncio.gather(*self._tasks.values())
        await t

        if self._loop_task is not None:
            self._loop_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._loop_task
            self._loop_task = None
        logger.trace("bye: runner loop")

    async def _loop(self):
        """The main runner loop.

        It listens for actions from the sidecar process and executes them.
        """
        async for msg in utils.QueueAgen(self._inbound):
            logger.trace("received: {}", msg)
            assert msg.kind == messages.MessageKind.ACTION
            match msg.action.actionType:
                case ActionType.START_STEP_RUN:
                    self._on_run(msg)
                case ActionType.CANCEL_STEP_RUN:
                    self._on_cancel(msg)
                case _:
                    raise NotImplementedError(msg)

    def _on_run(self, msg: "messages.Message"):
        async def task():
            logger.trace("running {}", msg.action.stepRunId)
            try:
                await self._emit_started(msg)
                result, e = await _invoke(msg.action, self._registry)
                if e is None:
                    assert result is not None
                    await self._emit_finished(msg, result)
                else:
                    assert result is None
                    await self._emit_failed(msg, _format_exc(e))
            finally:
                del self._tasks[msg.action.stepRunId]

        self._tasks[msg.action.stepRunId] = asyncio.create_task(
            task(), name=msg.action.stepRunId
        )

    def _step_event(self, msg: "messages.Message", **kwargs) -> StepActionEvent:
        """Makes a StepActionEvent proto."""
        base = StepActionEvent(
            jobId=msg.action.jobId,
            jobRunId=msg.action.jobRunId,
            stepId=msg.action.stepId,
            stepRunId=msg.action.stepRunId,
            actionId=msg.action.actionId,
            eventTimestamp=_timestamp(),
        )
        base.MergeFrom(StepActionEvent(**kwargs))
        return base

    def _on_cancel(self, msg: "messages.Message"):
        # TODO
        pass

    async def _emit_started(self, msg: "messages.Message"):
        await self._send(
            messages.Message(
                _step_event=MessageToDict(
                    self._step_event(
                        msg, eventType=StepActionEventType.STEP_EVENT_TYPE_STARTED
                    )
                )
            )
        )

    async def _emit_finished(self, msg: "messages.Message", payload: str):
        await self._send(
            messages.Message(
                _step_event=MessageToDict(
                    self._step_event(
                        msg,
                        eventType=StepActionEventType.STEP_EVENT_TYPE_COMPLETED,
                        eventPayload=payload,
                    )
                )
            )
        )

    async def _emit_failed(self, msg: "messages.Message", payload: str):
        await self._send(
            messages.Message(
                _step_event=MessageToDict(
                    self._step_event(
                        msg,
                        eventType=StepActionEventType.STEP_EVENT_TYPE_FAILED,
                        eventPayload=payload,
                    )
                )
            )
        )

    async def _send(self, msg: "messages.Message"):
        """Sends a message to the sidecar process."""
        logger.trace("send: {}", msg)
        # TODO: pyright could not figure this out
        await asyncio.to_thread(self._outbound.put, msg)  # type: ignore
