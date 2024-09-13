import asyncio
import json
import multiprocessing as mp
from typing import Any, Dict, Optional, Tuple

from loguru import logger

import hatchet_sdk.contracts.dispatcher_pb2
import hatchet_sdk.v2.callable as callable
import hatchet_sdk.v2.hatchet as hatchet
import hatchet_sdk.v2.runtime.messages as messages


class _Runner:
    def __init__(
        self,
        registry: Dict[str, "callable.HatchetCallableBase"],
        msg: "messages.Message",
    ):
        self.registry = registry
        logger.info(self.registry.keys())
        self.msg = msg

    async def run(self) -> Tuple[Any, Exception]:
        args = self.input["args"]
        kwargs = self.input["kwargs"]
        logger.trace("trying to run {} with input {}", self.action, self.input)
        logger.trace(repr(self.fn))
        try:
            if isinstance(self.fn, callable.HatchetCallable):
                return await asyncio.to_thread(self.fn._run, *args, **kwargs), None
            else:
                return await self.fn._run(*args, **kwargs)
        except Exception as e:
            logger.exception(e)
            return None, e

    @property
    def action(self):
        return self.msg.action.get("actionId")

    @property
    def parent(self):
        return self.msg.action.parent_workflow_run_id

    @property
    def input(self):
        return json.loads(self.msg.action.get("actionPayload")).get("input")

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
        self.inbound = inbound
        self.outbound = outbound

        self.looptask: Optional[asyncio.Task] = None

    def start(self):
        self.looptask = asyncio.create_task(self._runner_loop(), name="runner")

    async def shutdown(self):
        self.looptask.cancel()
        try:
            await self.looptask
        except asyncio.CancelledError:
            logger.info("bye")

    async def _runner_loop(self):
        while True:
            msg: "messages.Message" = await asyncio.to_thread(
                self._next_message_blocking
            )
            match msg.type:
                case messages.MessageType.ACTION_RUN:
                    await self._on_run(msg)
                case messages.MessageType.ACTION_CANCEL:
                    await self._on_cancel(msg)
                case _:
                    logger.debug(msg)

    async def _on_run(self, msg: "messages.Message"):
        await asyncio.to_thread(self._emit_start_blocking)
        result, e = await _Runner(self.registry, msg).run()
        if e is None:
            await asyncio._to_thread(self._emit_finish_blocking)

    async def _on_cancel(self, msg: "messages.Message"):
        pass

    def _next_message_blocking(self) -> messages.Message:
        return self.inbound.get()

    def _emit_start_blocking(self):
        msg = messages.Message(
            action=messages.Action(),
            type=messages.MessageType.EVENT_FINISHED,
        )
        self.outbound.put(msg)

    def _emit_finish_blocking(self):
        msg = messages.Message(
            action=messages.Action(),
            type=messages.MessageType.EVENT_STARTED,
        )
        self.outbound.put(msg)
