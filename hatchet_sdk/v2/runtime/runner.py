import asyncio
import json
import multiprocessing as mp
from typing import Any, Dict, Optional, Tuple

from loguru import logger

from hatchet_sdk.contracts.dispatcher_pb2 import (
    ActionType,
    StepActionEvent,
    StepActionEventType,
)
import hatchet_sdk.v2.callable as callable
import hatchet_sdk.v2.hatchet as hatchet
import hatchet_sdk.v2.runtime.messages as messages
from google.protobuf.timestamp_pb2 import Timestamp
import time
from google.protobuf.json_format import MessageToDict
import traceback


def _timestamp():
    ns = time.time_ns()
    return Timestamp(seconds=int(ns // 1e9), nanos=int(ns % 1e9))


def _format_exc(e: Exception):
    trace = "".join(traceback.format_exception(e))
    return "\n".join[str(e), trace]


class _Runner:
    def __init__(
        self,
        registry: Dict[str, "callable.HatchetCallableBase"],
        msg: "messages.Message",
    ):
        self.registry = registry
        assert msg.kind == messages.MessageKind.ACTION
        self.msg = msg

    async def run(self) -> Tuple[str, Exception]:
        logger.trace("runner invoking: {}", repr(self.fn))
        try:
            if isinstance(self.fn, callable.HatchetCallable):
                return await asyncio.to_thread(self.fn._run, self.msg.action), None
            else:
                return await self.fn._run(self.msg.action), None
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception(e)
            return None, e

    @property
    def action(self):
        return self.msg.action.actionId

    @property
    def fn(self):
        return self.registry[self.action]


class BaseRunnerLoop:
    def __init__(
        self,
        client: "hatchet.Hatchet",
        inbound: mp.Queue,  # inbound queue
        outbound: mp.Queue,  # outbound queue
    ):
        self.client = client
        self.registry: Dict[str, "callable.HatchetCallableBase"] = (
            client.registry.registry
        )
        self.worker_id:Optional[str] = None

        self.inbound = inbound
        self.outbound = outbound

        self.looptask: Optional[asyncio.Task] = None

        # a dict from StepRunId to its tasks
        self.runners: Dict[str, asyncio.Task] = dict()

    def start(self):
        self.looptask = asyncio.create_task(self.loop(), name="runnerloop")

    async def shutdown(self):
        logger.info("shutting down runner loop")
        t = asyncio.gather(*self.runners.values(), self.looptask)
        self.outbound.close()
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            logger.info("bye")

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
            try:
                await self.emit_started(msg)
                result, e = await _Runner(self.registry, msg).run()
                if e is None:
                    await self.emit_finished(msg, result)
                else:
                    await self.emit_failed(msg, _format_exc(e))
            finally:
                del self.runners[msg.action.stepRunId]

        self.runners[msg.action.stepRunId] = asyncio.create_task(
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
        logger.trace("sending: {}", msg)
        await asyncio.to_thread(self.outbound.put, msg)

    async def next(self) -> "messages.Message":
        msg = await asyncio.to_thread(self.inbound.get) # raise EOFError if the queue is closed
        logger.trace("recv: {}", msg)
        return msg
