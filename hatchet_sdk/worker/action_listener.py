import asyncio
from dataclasses import dataclass, field
import logging
import sys
from typing import List
import grpc
from hatchet_sdk.contracts.dispatcher_pb2 import ActionType
from hatchet_sdk.logger import logger
from hatchet_sdk.clients.admin import new_admin
from hatchet_sdk.clients.dispatcher import ActionListenerImpl, GetActionListenerRequest, new_dispatcher
from hatchet_sdk.loader import ClientConfig

# logger = logging.getLogger("hatchet.listener")
# logger.setLevel(logging.ERROR)
# handler = logging.StreamHandler(sys.stdout)
# formatter = logging.Formatter("[%(levelname)s] 🪓 -- %(asctime)s (listener) - %(message)s")
# handler.setFormatter(formatter)
# logger.addHandler(handler)

@dataclass
class WorkerActionProcess:
    name: str
    actions: List[str]
    max_runs: int
    config: ClientConfig
    handle_kill: bool = True
    debug: bool = False
    
    killing: bool = field(init=False, default=False)
    listener: ActionListenerImpl = field(init=False, default=None)

    def __post_init__(self):
        if self.debug:
            logger.setLevel(logging.DEBUG)
        self.killing = self.handle_kill

    async def start(self):
        logger.debug(f'starting action listener: {self.name}')

        try:
            # We need to initialize a new admin and dispatcher client *after* we've started the event loop,
            # otherwise the grpc.aio methods will use a different event loop and we'll get a bunch of errors.
            self.dispatcher_client = new_dispatcher(self.config)
            self.admin_client = new_admin(self.config)
            # self.workflow_run_event_listener = new_listener(self.config)
            # self.client.workflow_listener = PooledWorkflowRunListener(self.config)

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

            logger.debug(f"acquired listener: {self.listener.worker_id}")
        except grpc.RpcError as rpc_error:
            logger.error(f"could not start action listener: {rpc_error}")

        return await self.start_loop()


    async def start_loop(self):
        async for action in self.listener:
            match action.action_type:
                case ActionType.START_STEP_RUN:
                    logger.debug(f"start step run: {action.step_run_id}")
                    # await self.handle_start_step_run(action)
                case ActionType.CANCEL_STEP_RUN:
                    logger.debug(f"cancel step run: {action.step_run_id}")
                    # await self.handle_cancel_action(action.step_run_id)
                case ActionType.START_GET_GROUP_KEY:
                    logger.debug(f"start group key: {action.group_key}")
                    # await self.handle_start_group_key_run(action)
                case _:
                    logger.error(f"unknown action type: {action.action_type}")


def worker_process(*args, **kwargs):
    asyncio.run(WorkerActionProcess(*args, **kwargs).start())