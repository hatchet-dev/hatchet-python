import asyncio
import logging
import signal
from dataclasses import dataclass, field
from multiprocessing import Queue
from typing import Any, Callable, Dict

from hatchet_sdk.client import Client, new_client_raw
from hatchet_sdk.clients.dispatcher.action_listener import Action
from hatchet_sdk.context.worker_context import WorkerContext
from hatchet_sdk.loader import ClientConfig
from hatchet_sdk.logger import logger
from hatchet_sdk.worker.runner.runner import Runner
from hatchet_sdk.worker.runner.utils.capture_logs import capture_logs

STOP_LOOP = "STOP_LOOP"


@dataclass
class WorkerActionRunLoopManager:
    name: str
    action_registry: Dict[str, Callable[..., Any]]
    max_runs: int
    config: ClientConfig
    action_queue: Queue
    event_queue: Queue
    loop: asyncio.AbstractEventLoop
    handle_kill: bool = True
    debug: bool = False
    labels: dict[str, str | int] = field(default_factory=dict)

    client: Client = field(init=False, default=None)

    killing: bool = field(init=False, default=False)
    runner: Runner = field(init=False, default=None)

    def __post_init__(self):
        if self.debug:
            logger.setLevel(logging.DEBUG)
        self.client = new_client_raw(self.config, self.debug)
        self.start()

    def start(self, retry_count=1):
        k = self.loop.create_task(self.async_start(retry_count))

        self.loop.add_signal_handler(
            signal.SIGINT, lambda: asyncio.create_task(self.exit_gracefully())
        )
        self.loop.add_signal_handler(
            signal.SIGTERM, lambda: asyncio.create_task(self.exit_gracefully())
        )
        self.loop.add_signal_handler(signal.SIGQUIT, lambda: self.exit_forcefully())

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

    def cleanup(self):
        self.killing = True

        self.action_queue.put(STOP_LOOP)

    async def wait_for_tasks(self):
        if self.runner:
            await self.runner.wait_for_tasks()

    async def _start_action_loop(self):
        self.runner = Runner(
            self.name,
            self.event_queue,
            self.max_runs,
            self.handle_kill,
            self.action_registry,
            self.config,
            self.labels,
        )

        logger.debug(f"'{self.name}' waiting for {list(self.action_registry.keys())}")
        while not self.killing:
            action: Action = await self._get_action()
            if action == STOP_LOOP:
                logger.debug("stopping action runner loop...")
                break

            self.runner.run(action)
        logger.debug("action runner loop stopped")

    async def _get_action(self):
        return await self.loop.run_in_executor(None, self.action_queue.get)

    async def exit_gracefully(self):
        if self.killing:
            return

        logger.info(f"gracefully exiting runner...")

        self.cleanup()

        await self.wait_for_tasks()

        # Wait for 1 second to allow last calls to flush. These are calls which have been
        # added to the event loop as callbacks to tasks, so we're not aware of them in the
        # task list.
        await asyncio.sleep(1)

    def exit_forcefully(self):
        logger.info("forcefully exiting runner...")
        self.cleanup()
