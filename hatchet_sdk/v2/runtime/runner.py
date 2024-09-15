import asyncio
import json
import multiprocessing as mp
import time
import traceback
from typing import Any, Dict, Optional, Tuple

from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from loguru import logger

import hatchet_sdk.v2.callable as callable
import hatchet_sdk.v2.hatchet as hatchet
import hatchet_sdk.v2.runtime.messages as messages
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
):
    key = action.actionId
    fn: "callable.HatchetCallableBase" = registry[key]  # TODO
    logger.trace("invoking: {}", repr(fn))
    try:
        if isinstance(fn, callable.HatchetCallable):
            return await asyncio.to_thread(fn._run, action), None
        else:
            return await fn._run(action), None
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.exception(e)
        return None, e


class BaseRunnerLoop:
    def __init__(
        self,
        client: "hatchet.Hatchet",
        inbound: mp.Queue,  # inbound queue, not owned
        outbound: mp.Queue,  # outbound queue, not owned
    ):
        logger.trace("init runner loop")
        self.client = client
        self.registry: Dict[str, "callable.HatchetCallableBase"] = (
            client.registry.registry
        )
        self.worker_id: Optional[str] = None

        self.inbound = inbound
        self.outbound = outbound

        self.looptask: Optional[asyncio.Task] = None

        # a dict from StepRunId to its tasks
        self.tasks: Dict[str, asyncio.Task] = dict()

    def start(self):
        logger.debug("runner loop started")
        self.looptask = asyncio.create_task(self.loop(), name="runner loop")

    async def shutdown(self):
        logger.trace("shutting down runner loop")
        t = asyncio.gather(*self.tasks.values(), self.looptask)
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            logger.debug("bye")

    async def loop(self):
        while True:
            msg: "messages.Message" = await self.next()
            assert msg.kind == messages.MessageKind.ACTION
            match msg.action.actionType:
                case ActionType.START_STEP_RUN:
                    self.on_run(msg)
                case ActionType.CANCEL_STEP_RUN:
                    self.on_cancel(msg)
                case _:
                    logger.debug(msg)

    def on_run(self, msg: "messages.Message"):
        async def task():
            logger.trace("running {}", msg.action.stepRunId)
            try:
                await self.emit_started(msg)
                result, e = await _invoke(msg.action, self.registry)
                if e is None:
                    await self.emit_finished(msg, result)
                else:
                    await self.emit_failed(msg, _format_exc(e))
            finally:
                del self.tasks[msg.action.stepRunId]

        self.tasks[msg.action.stepRunId] = asyncio.create_task(
            task(), name=msg.action.stepRunId
        )

    def step_event(self, msg: "messages.Message", **kwargs) -> StepActionEvent:
        base = StepActionEvent(
            jobId=msg.action.jobId,
            jobRunId=msg.action.jobRunId,
            stepId=msg.action.stepId,
            stepRunId=msg.action.stepRunId,
            actionId=msg.action.actionId,
            eventTimestamp=_timestamp(),
        )
        base.MergeFrom(StepActionEvent(**kwargs))
        return MessageToDict(base)

    def on_cancel(self, msg: "messages.Message"):
        pass

    async def emit_started(self, msg: "messages.Message"):
        await self.send(
            messages.Message(
                _step_event=self.step_event(
                    msg, eventType=StepActionEventType.STEP_EVENT_TYPE_STARTED
                )
            )
        )

    async def emit_finished(self, msg: "messages.Message", payload: str):
        await self.send(
            messages.Message(
                _step_event=self.step_event(
                    msg,
                    eventType=StepActionEventType.STEP_EVENT_TYPE_COMPLETED,
                    eventPayload=payload,
                )
            )
        )

    async def emit_failed(self, msg: "messages.Message", payload: str):
        await self.send(
            messages.Message(
                _step_event=self.step_event(
                    msg,
                    eventType=StepActionEventType.STEP_EVENT_TYPE_FAILED,
                    eventPayload=payload,
                )
            )
        )

    async def send(self, msg: "messages.Message"):
        logger.trace("send:\n{}", msg)
        await asyncio.to_thread(self.outbound.put, msg)

    async def next(self) -> "messages.Message":
        msg = await asyncio.to_thread(
            self.inbound.get
        )  # raise EOFError if the queue is closed
        logger.trace("recv:\n{}", msg)
        return msg
