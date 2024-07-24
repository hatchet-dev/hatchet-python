import asyncio
import multiprocessing
import os
import signal
import sys
from dataclasses import dataclass, field
from enum import Enum
from multiprocessing import Process, Queue
from typing import Any, Callable, Dict, Optional

from hatchet_sdk.loader import ClientConfig
from hatchet_sdk.logger import logger
from hatchet_sdk.worker.action_listener import worker_action_listener_process
from hatchet_sdk.worker.runner.run_loop_manager import WorkerActionRunLoopManager

from ..client import Client, new_client_raw
from ..context import Context
from ..workflow import WorkflowMeta


class WorkerStatus(Enum):
    INITIALIZED = 1
    STARTING = 2
    HEALTHY = 3
    UNHEALTHY = 4


@dataclass
class Worker:
    name: str
    config: ClientConfig = field(default_factory=dict)
    max_runs: Optional[int] = None
    debug: bool = False
    handle_kill: bool = True

    client: Client = field(init=False)
    tasks: Dict[str, asyncio.Task] = field(default_factory=dict)
    contexts: Dict[str, Context] = field(default_factory=dict)
    action_registry: Dict[str, Callable[..., Any]] = field(default_factory=dict)
    killing: bool = field(init=False, default=False)
    _status: WorkerStatus = field(init=False, default=WorkerStatus.INITIALIZED)

    action_listener_process: Process = field(init=False, default=None)
    action_runner: WorkerActionRunLoopManager = field(init=False, default=None)

    action_queue: Queue = field(init=False, default_factory=Queue)
    event_queue: Queue = field(init=False, default_factory=Queue)

    def __post_init__(self):
        self.client = new_client_raw(self.config, self.debug)
        self.name = self.client.config.namespace + self.name
        self._setup_signal_handlers()

    def register_workflow(self, workflow: WorkflowMeta):
        namespace = self.client.config.namespace
        self.client.admin.put_workflow(
            workflow.get_name(namespace), workflow.get_create_opts(namespace)
        )

        def create_action_function(action_func):
            def action_function(context):
                return action_func(workflow, context)

            if asyncio.iscoroutinefunction(action_func):
                action_function._is_coroutine = True
            else:
                action_function._is_coroutine = False

            return action_function

        for action_name, action_func in workflow.get_actions(namespace):
            self.action_registry[action_name] = create_action_function(action_func)

    def status(self) -> WorkerStatus:
        # TODO: Implement health check
        if self.listener:
            if self.listener.is_healthy():
                self._status = WorkerStatus.HEALTHY
                return WorkerStatus.HEALTHY
            else:
                self._status = WorkerStatus.UNHEALTHY
                return WorkerStatus.UNHEALTHY

        return self._status

    def async_start(self):
        return self.start()

    ## Start methods
    def start(self):
        main_pid = os.getpid()
        logger.debug(f"worker runtime starting on PID:\t{main_pid}")

        self.action_listener_process = self._start_listener()
        self.action_runner = self._run_action_runner()

        self.action_listener_process.join()

    def _run_action_runner(self):
        # Retrieve the shared queue
        runner = WorkerActionRunLoopManager(
            self.name,
            self.action_registry,
            self.max_runs,
            self.config,
            self.action_queue,
            self.event_queue,
            self.handle_kill,
            self.client.debug,
        )

        return runner

    def _start_listener(self):
        action_list = [str(key) for key in self.action_registry.keys()]

        ctx = multiprocessing.get_context("spawn")
        process = ctx.Process(
            target=worker_action_listener_process,
            args=(
                self.name,
                action_list,
                self.max_runs,
                self.config,
                self.action_queue,
                self.handle_kill,
                self.client.debug,
            ),
        )
        process.start()
        logger.debug(f"action listener starting on PID:\t{process.pid}")

        return process

    ## Cleanup methods
    def _setup_signal_handlers(self):
        signal.signal(signal.SIGTERM, self._handle_exit_signal)
        signal.signal(signal.SIGINT, self._handle_exit_signal)
        signal.signal(signal.SIGQUIT, self._handle_force_quit_signal)

    def _handle_exit_signal(self, signum, frame):
        sig_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
        logger.info(f"received signal {sig_name}...")
        asyncio.run(self.exit_gracefully())

    def _handle_force_quit_signal(self, signum, frame):
        logger.info(f"received SIGQUIT...")
        self.exit_forcefully()

    async def exit_gracefully(self):
        logger.debug(f"gracefully stopping worker: {self.name}")

        if self.action_listener_process:
            self.action_listener_process.terminate()

        if self.action_listener_process:
            self.action_listener_process.join(
                timeout=10
            )  # Wait up to 10 seconds for process to terminate

        # if self.action_runner:
        #     self.action_runner.wait_for_tasks()  # TODO - should we wait for this process to terminate?

        logger.info(f"👋")

    def exit_forcefully(self):
        logger.debug(f"forcefully stopping worker: {self.name}")

        if self.action_listener_process:
            self.action_listener_process.kill()  # Forcefully kill the process

        logger.info(f"👋")
        sys.exit(
            1
        )  # Exit immediately TODO - should we exit with 1 here, there may be other workers to cleanup
