import asyncio
from dataclasses import dataclass, field
import logging
from multiprocessing import Queue
import signal
import sys
from typing import List
from hatchet_sdk.logger import logger
from hatchet_sdk.loader import ClientConfig
from hatchet_sdk.worker.runner.utils.capture_logs import capture_logs
from ...client import new_client, new_client_raw

@dataclass
class WorkerActionRunnerManager:
    name: str
    actions: List[str]
    max_runs: int
    config: ClientConfig
    action_queue: Queue
    handle_kill: bool = True
    debug: bool = False

    loop: asyncio.AbstractEventLoop = field(init=False, default=None)
    
    _start = None

    def __post_init__(self):
        if self.debug:
            logger.setLevel(logging.DEBUG)
        self.killing = self.handle_kill
        self.client = new_client_raw(self.config, self.debug)
        self._start = self.start()

    async def start(self):
        logger.debug(f'starting action runner: {self.name}')
        return await self._start_aio_loop()

    def _start_aio_loop(self, retry_count=1):
        try:
            if self.loop is not None:
                loop = asyncio.get_running_loop()
                self.loop = loop
                created_loop = False
            else:
                self.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)
                created_loop = True
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            created_loop = True

        self.loop.create_task(self._async_start(retry_count))

        # self.loop.add_signal_handler(
        #     signal.SIGINT, lambda: asyncio.create_task(self.exit_gracefully())
        # )
        # self.loop.add_signal_handler(
        #     signal.SIGTERM, lambda: asyncio.create_task(self.exit_gracefully())
        # )
        # self.loop.add_signal_handler(signal.SIGQUIT, lambda: self.exit_forcefully())

        if created_loop:
            self.loop.run_forever()

            if self.handle_kill:
                sys.exit(0)

    async def _async_start(self, retry_count=1):
        await capture_logs(
            self.client.logger,
            self.client.event,
            self._start_action_loop,
        )(retry_count=retry_count)

    async def _start_action_loop(self):
        while True:
            action = await self._get_action()
            logger.info(f"Received action: {action}")

    async def _get_action(self):
        return await self.loop.run_in_executor(None, self.action_queue.get)

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