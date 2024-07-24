import asyncio
import logging
import sys
from dataclasses import dataclass, field
from multiprocessing import Queue
from typing import List

import grpc

from hatchet_sdk.clients.admin import new_admin
from hatchet_sdk.clients.dispatcher import (
    ActionListenerImpl,
    GetActionListenerRequest,
    new_dispatcher,
)
from hatchet_sdk.contracts.dispatcher_pb2 import ActionType
from hatchet_sdk.loader import ClientConfig
from hatchet_sdk.logger import logger


@dataclass
class WorkerActionListenerProcess:
    name: str
    actions: List[str]
    max_runs: int
    config: ClientConfig
    action_queue: Queue
    handle_kill: bool = True
    debug: bool = False

    killing: bool = field(init=False, default=False)
    listener: ActionListenerImpl = field(init=False, default=None)

    def __post_init__(self):
        if self.debug:
            logger.setLevel(logging.DEBUG)
        self.killing = self.handle_kill

        # TODO handle kill signal

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
        except grpc.RpcError as rpc_error:
            logger.error(f"could not start action listener: {rpc_error}")

        return await self.start_loop()

    async def start_loop(self):
        async for action in self.listener:
            match action.action_type:
                case ActionType.START_STEP_RUN:
                    logger.debug(f"rx: start step run: {action.step_run_id}/{action.action_id}")
                case ActionType.CANCEL_STEP_RUN:
                    logger.debug(f"rx: cancel step run: {action.step_run_id}")
                case ActionType.START_GET_GROUP_KEY:
                    logger.debug(f"rx: start group key: {action.get_group_key_run_id}")
                case _:
                    logger.error(
                        f"rx: unknown action type ({action.action_type}): {action.action_type}"
                    )
            self.action_queue.put(action)


def worker_action_listener_process(*args, **kwargs):
    asyncio.run(WorkerActionListenerProcess(*args, **kwargs).start())
