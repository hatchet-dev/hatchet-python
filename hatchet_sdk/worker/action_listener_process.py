import asyncio
import logging
import multiprocessing
import os
import signal
import sys
from dataclasses import dataclass, field
from multiprocessing import Queue
from typing import Any, List, Optional, Union

import grpc

from hatchet_sdk.clients.admin import new_admin
from hatchet_sdk.clients.dispatcher.action_listener import Action
from hatchet_sdk.clients.dispatcher.dispatcher import (
    ActionListener,
    GetActionListenerRequest,
    new_dispatcher,
)
from hatchet_sdk.contracts.dispatcher_pb2 import (
    GROUP_KEY_EVENT_TYPE_STARTED,
    STEP_EVENT_TYPE_STARTED,
    ActionType,
)
from hatchet_sdk.loader import ClientConfig
from hatchet_sdk.logger import logger
from hatchet_sdk.utils.backoff import exp_backoff_sleep

ACTION_EVENT_RETRY_COUNT = 5


@dataclass
class ActionEvent:
    action: Action
    type: Any  # TODO type
    payload: Optional[str] = None


STOP_LOOP = "STOP_LOOP"  # Sentinel object to stop the loop


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

    listener: ActionListener = field(init=False, default=None)

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

    async def start(self, retry_attempt=0):
        if retry_attempt > 5:
            logger.error("could not start action listener")
            return

        logger.debug(f"starting action listener: {self.name}")

        try:
            self.dispatcher_client = new_dispatcher(self.config)

            self.listener: ActionListener = (
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
        except grpc.RpcError as rpc_error:
            logger.error(f"could not start action listener: {rpc_error}")
            return

        # Start both loops as background tasks
        self.action_loop_task = asyncio.create_task(self.start_action_loop())
        self.event_send_loop_task = asyncio.create_task(self.start_event_send_loop())

    # TODO move event methods to separate class
    async def _get_event(self):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.event_queue.get)

    async def start_event_send_loop(self):
        while True:
            event: ActionEvent = await self._get_event()

            if event == STOP_LOOP:
                logger.debug("stopping event send loop...")
                break

            logger.debug(f"tx: event: {event.action.action_id}/{event.type}")
            asyncio.create_task(self.send_event(event))

    async def send_event(self, event: ActionEvent, retry_attempt: int = 1):
        try:
            match event.action.action_type:
                case ActionType.START_STEP_RUN:
                    asyncio.create_task(
                        self.dispatcher_client.send_step_action_event(
                            event.action, event.type, event.payload
                        )
                    )
                case ActionType.CANCEL_STEP_RUN:
                    logger.debug(f"unimplemented event send")
                case ActionType.START_GET_GROUP_KEY:
                    asyncio.create_task(
                        self.dispatcher_client.send_group_key_action_event(
                            event.action, event.type, event.payload
                        )
                    )
                case _:
                    logger.error(f"unknown action type for event send")
        except Exception as e:
            logger.error(
                f"could not send action event ({retry_attempt}/{ACTION_EVENT_RETRY_COUNT}): {e}"
            )
            if retry_attempt <= ACTION_EVENT_RETRY_COUNT:
                await exp_backoff_sleep(retry_attempt, 1)
                await self.send_event(event, retry_attempt + 1)

    async def start_action_loop(self):
        try:
            async for action in self.listener:
                if action is None:
                    break

                # Process the action here
                match action.action_type:
                    case ActionType.START_STEP_RUN:
                        self.event_queue.put(
                            ActionEvent(
                                action=action,
                                type=STEP_EVENT_TYPE_STARTED,  # TODO ack type
                            )
                        )
                        logger.info(
                            f"rx: start step run: {action.step_run_id}/{action.action_id}"
                        )
                    case ActionType.CANCEL_STEP_RUN:
                        logger.info(f"rx: cancel step run: {action.step_run_id}")
                    case ActionType.START_GET_GROUP_KEY:
                        self.event_queue.put(
                            ActionEvent(
                                action=action,
                                type=GROUP_KEY_EVENT_TYPE_STARTED,  # TODO ack type
                            )
                        )
                        logger.info(
                            f"rx: start group key: {action.get_group_key_run_id}"
                        )
                    case _:
                        logger.error(
                            f"rx: unknown action type ({action.action_type}): {action.action_type}"
                        )
                try:
                    self.action_queue.put(action)
                except Exception as e:
                    logger.error("error putting action: {e}")

        except Exception as e:
            logger.error(f"error in action loop: {e}")
        finally:
            logger.info("action loop closed")
            if not self.killing:
                await self.exit_gracefully(skip_unregister=True)

    async def cleanup(self):
        self.killing = True

        if self.listener is not None:
            self.listener.cleanup()

        self.event_queue.put(STOP_LOOP)

    async def exit_gracefully(self, skip_unregister=False):
        if self.killing:
            return

        logger.debug("closing action listener...")

        await self.cleanup()

        while not self.event_queue.empty():
            pass

        logger.info("action listener closed")

    def exit_forcefully(self):
        asyncio.run(self.cleanup())
        logger.debug("forcefully closing listener...")


def worker_action_listener_process(*args, **kwargs):
    async def run():
        process = WorkerActionListenerProcess(*args, **kwargs)
        await process.start()
        # Keep the process running
        while not process.killing:
            await asyncio.sleep(0.1)

    asyncio.run(run())