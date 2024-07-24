import asyncio
from dataclasses import dataclass, field
import logging
from multiprocessing import Queue
import signal
import sys
from typing import Any, Callable, Dict, List
from hatchet_sdk.clients.admin import new_admin
from hatchet_sdk.clients.dispatcher import new_dispatcher
from hatchet_sdk.clients.run_event_listener import new_listener
from hatchet_sdk.clients.workflow_listener import PooledWorkflowRunListener
from hatchet_sdk.logger import logger
from hatchet_sdk.loader import ClientConfig
from hatchet_sdk.client import Client, new_client_raw
from hatchet_sdk.worker.runner.runner import Runner
from hatchet_sdk.worker.runner.utils.capture_logs import capture_logs

@dataclass
class WorkerActionRunLoopManager:
    name: str
    action_registry: Dict[str, Callable[..., Any]]
    max_runs: int
    config: ClientConfig
    action_queue: Queue
    event_queue: Queue
    handle_kill: bool = True
    debug: bool = False

    loop: asyncio.AbstractEventLoop = field(init=False)

    client: Client = None

    runner: Runner = None

    def __post_init__(self):
        if self.debug:
            logger.setLevel(logging.DEBUG)
        self.killing = self.handle_kill
        self.client = new_client_raw(self.config, self.debug)
        self.start()

    def start(self, retry_count=1):
        try:
            loop = asyncio.get_running_loop()
            self.loop = loop
            created_loop = False
            logger.debug("using existing event loop")
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            logger.debug("creating new event loop")
            asyncio.set_event_loop(self.loop)
            created_loop = True

        k = self.loop.create_task(self.async_start(retry_count))

        self.loop.add_signal_handler(
            signal.SIGINT, lambda: asyncio.create_task(self.exit_gracefully())
        )
        self.loop.add_signal_handler(
            signal.SIGTERM, lambda: asyncio.create_task(self.exit_gracefully())
        )
        self.loop.add_signal_handler(signal.SIGQUIT, lambda: self.exit_forcefully())

        if created_loop:
            self.loop.run_forever()

            if self.handle_kill:
                sys.exit(0)

    async def async_start(self, retry_count=1):
        await capture_logs(
            self.client.logger,
            self.client.event,
            self._async_start,
        )(retry_count=retry_count)

    async def _async_start(self, retry_count=1):
        logger.info("starting runner...")
        self.loop = asyncio.get_running_loop()
        k = self.loop.create_task(self._start_action_loop())

    async def _start_action_loop(self):

        self.runner = Runner(
            self.name,
            self.event_queue,
            self.max_runs,
            self.handle_kill,
            self.action_registry,
            self.config,
        )

        print(self.action_registry)

        while True:
            action = await self._get_action()
            logger.debug(f"rx: runtime payload: {action}")
            task = await self.runner.run(action)

    async def _get_action(self):
        return await self.loop.run_in_executor(None, self.action_queue.get)

    async def exit_gracefully(self):
            if self.killing:
                self.exit_forcefully()
                return

            self.killing = True

            logger.info(f"Exiting gracefully...")

            # try:
            #     # self.listener.unregister()
            # except Exception as e:
            #     logger.error(f"Could not unregister worker: {e}")

            try:
                logger.info("Waiting for tasks to finish...")

                # await self.wait_for_tasks()
            except Exception as e:
                logger.error(f"Could not wait for tasks: {e}")

            # Wait for 1 second to allow last calls to flush. These are calls which have been
            # added to the event loop as callbacks to tasks, so we're not aware of them in the
            # task list.
            await asyncio.sleep(1)

            self.loop.stop()

    def exit_forcefully(self):
            self.killing = True

            logger.info("Forcefully exiting hatchet worker...")

            # try:
            #     self.listener.unregister()
            # except Exception as e:
            #     logger.error(f"Could not unregister worker: {e}")

            self.loop.stop()
