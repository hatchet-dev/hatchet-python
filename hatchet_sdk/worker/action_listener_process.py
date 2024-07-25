import asyncio
import logging
import signal
import sys
from dataclasses import dataclass, field
from multiprocessing import Queue
from typing import Any, List, Optional, Union

import grpc

from hatchet_sdk.clients.admin import new_admin
from hatchet_sdk.clients.dispatcher import (
    Action,
    ActionListenerImpl,
    GetActionListenerRequest,
    new_dispatcher,
)
from hatchet_sdk.contracts.dispatcher_pb2 import (
    GROUP_KEY_EVENT_TYPE_STARTED,
    STEP_EVENT_TYPE_STARTED,
    ActionType,
    GroupKeyActionEventType,
    StepActionEventType,
)
from hatchet_sdk.loader import ClientConfig
from hatchet_sdk.logger import logger


@dataclass
class ActionEvent:
    action: Action
    type: Any  # TODO type
    payload: Optional[str] = None


@dataclass
class WorkerActionListenerProcess:
    name: str
    actions: List[str]
    max_runs: int
    config: ClientConfig
    action_queue: Queue
    event_queue: Queue
    handle_kill: bool = True
    debug: bool = False

    listener: ActionListenerImpl = field(init=False, default=None)

    killing: bool = field(init=False, default=False)

    action_loop_task: asyncio.Task = field(init=False, default=None)
    event_send_loop_task: asyncio.Task = field(init=False, default=None)

    def __post_init__(self):
        if self.debug:
            logger.setLevel(logging.DEBUG)

        loop = asyncio.get_event_loop()
        loop.add_signal_handler(
            signal.SIGINT, lambda: asyncio.create_task(self.exit_gracefully())
        )
        loop.add_signal_handler(
            signal.SIGTERM, lambda: asyncio.create_task(self.exit_gracefully())
        )
        loop.add_signal_handler(signal.SIGQUIT, lambda: self.exit_forcefully())

    async def start(self):
        logger.debug(f"starting action listener: {self.name}")

        try:
            self.dispatcher_client = new_dispatcher(self.config)

            self.listener: ActionListenerImpl = (
                await self.dispatcher_client.get_action_listener(
                    GetActionListenerRequest(
                        worker_name=self.name,
                        services=["default"],
                        actions=self.actions,
                        max_runs=self.max_runs,
                    )
                )
            )

            logger.debug(f"acquired action listener: {self.listener.worker_id}")
            logger.info(f"connection established, awaiting actions")
        except grpc.RpcError as rpc_error:
            logger.error(f"could not start action listener: {rpc_error}")
            return  # Exit if we couldn't start the listener

        # Start both loops as background tasks
        self.action_loop_task = asyncio.create_task(self.start_action_loop())
        self.event_send_loop_task = asyncio.create_task(self.start_event_send_loop())

    async def exit_gracefully(self):
        if self.killing:
            return self.exit_forcefully()
        self.killing = True
        logger.debug("exiting listener gracefully")

        if self.listener:
            self.listener.unregister()

        if self.action_loop_task:
            self.action_loop_task.cancel()

        if self.event_send_loop_task:
            self.event_send_loop_task.cancel()

        loop = asyncio.get_event_loop()
        loop.stop()

    def exit_forcefully(self):
        logger.debug("exiting listener forcefully")
        sys.exit(0)

    async def start_event_send_loop(self):
        while True:
            event: ActionEvent = await self._get_event()
            logger.debug(f"tx: event: {event.action.action_id}/{event.type}")
            asyncio.create_task(self.send_event(event))
            # await self.listener.send_event(event)

    async def send_event(self, event: ActionEvent, retry_count: int = 3):
        try:
            match event.action.action_type:
                case ActionType.START_STEP_RUN:
                    asyncio.create_task(
                        self.dispatcher_client.send_step_action_event_simple(
                            event.action, event.type, event.payload
                        )
                    )
                case ActionType.CANCEL_STEP_RUN:
                    logger.debug(f"unimplemented event send")
                case ActionType.START_GET_GROUP_KEY:
                    asyncio.create_task(
                        self.dispatcher_client.send_group_key_action_event_simple(
                            event.action, event.type, event.payload
                        )
                    )
                case _:
                    logger.error(f"unknown action type for event send")
        except Exception as e:
            logger.error(f"could not send action event: {e}")
            if retry_count > 0:
                await asyncio.sleep(1)
                await self.send_event(event, retry_count - 1)

    async def _get_event(self):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.event_queue.get)

    async def start_action_loop(self):
        async for action in self.listener:
            match action.action_type:
                case ActionType.START_STEP_RUN:
                    self.event_queue.put(
                        ActionEvent(
                            action=action,
                            type=STEP_EVENT_TYPE_STARTED,  # TODO ack type
                        )
                    )
                    logger.debug(
                        f"rx: start step run: {action.step_run_id}/{action.action_id}"
                    )
                case ActionType.CANCEL_STEP_RUN:
                    logger.debug(f"rx: cancel step run: {action.step_run_id}")
                case ActionType.START_GET_GROUP_KEY:
                    self.event_queue.put(
                        ActionEvent(
                            action=action,
                            type=GROUP_KEY_EVENT_TYPE_STARTED,  # TODO ack type
                        )
                    )
                    logger.debug(f"rx: start group key: {action.get_group_key_run_id}")
                case _:
                    logger.error(
                        f"rx: unknown action type ({action.action_type}): {action.action_type}"
                    )
            self.action_queue.put(action)

    async def stop(self):
        if self.action_loop_task:
            self.action_loop_task.cancel()
            await self.action_loop_task
        if self.event_send_loop_task:
            self.event_send_loop_task.cancel()
            await self.event_send_loop_task


def worker_action_listener_process(*args, **kwargs):
    async def run():
        process = WorkerActionListenerProcess(*args, **kwargs)
        await process.start()
        # Keep the process running
        while not process.killing:
            await asyncio.sleep(0.1)

    asyncio.run(run())
