import asyncio
from dataclasses import dataclass, field
import logging
from multiprocessing import Queue
from typing import List
from hatchet_sdk.logger import logger
from hatchet_sdk.loader import ClientConfig

@dataclass
class WorkerActionRunner:
    name: str
    actions: List[str]
    max_runs: int
    config: ClientConfig
    action_queue: Queue
    handle_kill: bool = True
    debug: bool = False

    killing: bool = field(init=False, default=False)

    def __post_init__(self):
        if self.debug:
            logger.setLevel(logging.DEBUG)
        self.killing = self.handle_kill

        asyncio.run(self.start())

    async def start(self):
        logger.debug(f'starting action runner: {self.name}')
        return await self.start_loop()

    async def start_loop(self):
        while True:
            action = await self.get_action()
            logger.info(f"Received action: {action}")

    async def get_action(self):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.action_queue.get)


    async def wait_for_tasks(self):
        return
        # wait for all futures to finish
        for taskId in list(self.tasks.keys()):
            try:
                logger.info(f"Waiting for task {taskId} to finish...")

                if taskId in self.tasks:
                    await self.tasks.get(taskId)
            except Exception as e:
                pass