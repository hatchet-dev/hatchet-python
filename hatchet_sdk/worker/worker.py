import asyncio
import multiprocessing
import os
import signal
import sys
from dataclasses import dataclass, field
from enum import Enum
from multiprocessing import Process, Queue
from typing import Any, Callable, Dict, Optional

from hatchet_sdk.client import Client, new_client_raw
from hatchet_sdk.context import Context
from hatchet_sdk.contracts.workflows_pb2 import CreateWorkflowVersionOpts
from hatchet_sdk.loader import ClientConfig
from hatchet_sdk.logger import logger
from hatchet_sdk.v2.callable import HatchetCallable
from hatchet_sdk.worker.action_listener_process import worker_action_listener_process
from hatchet_sdk.worker.runner.run_loop_manager import WorkerActionRunLoopManager
from hatchet_sdk.workflow import WorkflowMeta


class WorkerStatus(Enum):
    INITIALIZED = 1
    STARTING = 2
    HEALTHY = 3
    UNHEALTHY = 4


@dataclass
class WorkerStartOptions:
    loop: asyncio.AbstractEventLoop = field(default=None)


@dataclass
class Worker:
    name: str
    config: ClientConfig = field(default_factory=dict)
    max_runs: Optional[int] = None
    debug: bool = False
    labels: dict[str, str | int] = field(default_factory=dict)
    handle_kill: bool = True

    client: Client = field(init=False)
    tasks: Dict[str, asyncio.Task] = field(default_factory=dict)
    contexts: Dict[str, Context] = field(default_factory=dict)
    action_registry: Dict[str, Callable[..., Any]] = field(default_factory=dict)
    killing: bool = field(init=False, default=False)
    _status: WorkerStatus = field(init=False, default=WorkerStatus.INITIALIZED)

    action_listener_process: Process = field(init=False, default=None)
    action_listener_health_check: asyncio.Task = field(init=False, default=None)
    action_runner: WorkerActionRunLoopManager = field(init=False, default=None)
    ctx = multiprocessing.get_context("spawn")

    action_queue: Queue = field(init=False, default_factory=ctx.Queue)
    event_queue: Queue = field(init=False, default_factory=ctx.Queue)

    loop: asyncio.AbstractEventLoop = field(init=False, default=None)

    def __post_init__(self):
        self.client = new_client_raw(self.config, self.debug)
        self.name = self.client.config.namespace + self.name
        self._setup_signal_handlers()

    def register_function(self, action: str, func: HatchetCallable):
        self.action_registry[action] = func

    def register_workflow_from_opts(self, name: str, opts: CreateWorkflowVersionOpts):
        try:
            self.client.admin.put_workflow(opts.name, opts)
        except Exception as e:
            logger.error(f"failed to register workflow: {opts.name}")
            logger.error(e)
            sys.exit(1)

    def register_workflow(self, workflow: WorkflowMeta):
        namespace = self.client.config.namespace

        try:
            self.client.admin.put_workflow(
                workflow.get_name(namespace), workflow.get_create_opts(namespace)
            )
        except Exception as e:
            logger.error(f"failed to register workflow: {workflow.get_name(namespace)}")
            logger.error(e)
            sys.exit(1)

        def create_action_function(action_func):
            def action_function(context):
                return action_func(workflow, context)

            if asyncio.iscoroutinefunction(action_func):
                action_function.is_coroutine = True
            else:
                action_function.is_coroutine = False

            return action_function

        for action_name, action_func in workflow.get_actions(namespace):
            self.action_registry[action_name] = create_action_function(action_func)

    def status(self) -> WorkerStatus:
        return self._status

    def setup_loop(self, loop: asyncio.AbstractEventLoop = None):
        try:
            loop = loop or asyncio.get_running_loop()
            self.loop = loop
            created_loop = False
            logger.debug("using existing event loop")
            return created_loop
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            logger.debug("creating new event loop")
            asyncio.set_event_loop(self.loop)
            created_loop = True
            return created_loop

    def start(self, options: WorkerStartOptions = WorkerStartOptions()):
        created_loop = self.setup_loop(options.loop)
        f = asyncio.run_coroutine_threadsafe(
            self.async_start(options, _from_start=True), self.loop
        )
        # start the loop and wait until its closed
        if created_loop:
            self.loop.run_forever()

            if self.handle_kill:
                sys.exit(0)
        return f

    ## Start methods
    async def async_start(
        self,
        options: WorkerStartOptions = WorkerStartOptions(),
        _from_start: bool = False,
    ):
        main_pid = os.getpid()
        logger.info(f"------------------------------------------")
        logger.info(f"STARTING HATCHET...")
        logger.debug(f"worker runtime starting on PID: {main_pid}")

        self._status = WorkerStatus.STARTING

        if len(self.action_registry.keys()) == 0:
            logger.error(
                "no actions registered, register workflows or actions before starting worker"
            )
            return

        # non blocking setup
        if not _from_start:
            self.setup_loop(options.loop)

        self.action_listener_process = self._start_listener()
        self.action_runner = self._run_action_runner()
        self.action_listener_health_check = self.loop.create_task(
            self._check_listener_health()
        )

        return await self.action_listener_health_check

    def _run_action_runner(self):
        # Retrieve the shared queue
        runner = WorkerActionRunLoopManager(
            self.name,
            self.action_registry,
            self.max_runs,
            self.config,
            self.action_queue,
            self.event_queue,
            self.loop,
            self.handle_kill,
            self.client.debug,
            self.labels,
        )

        return runner

    def _start_listener(self):
        action_list = [str(key) for key in self.action_registry.keys()]
        try:
            process = self.ctx.Process(
                target=worker_action_listener_process,
                args=(
                    self.name,
                    action_list,
                    self.max_runs,
                    self.config,
                    self.action_queue,
                    self.event_queue,
                    self.handle_kill,
                    self.client.debug,
                ),
            )
            process.start()
            logger.debug(f"action listener starting on PID: {process.pid}")

            return process
        except Exception as e:
            logger.error(f"failed to start action listener: {e}")
            sys.exit(1)

    async def _check_listener_health(self):
        logger.debug("starting action listener health check...")
        try:
            while not self.killing:
                if (
                    self.action_listener_process is None
                    or not self.action_listener_process.is_alive()
                ):
                    logger.debug("child action listener process killed...")
                    self._status = WorkerStatus.UNHEALTHY
                    if not self.killing:
                        self.loop.create_task(self.exit_gracefully())
                    break
                else:
                    self._status = WorkerStatus.HEALTHY
                await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"error checking listener health: {e}")

    ## Cleanup methods
    def _setup_signal_handlers(self):
        signal.signal(signal.SIGTERM, self._handle_exit_signal)
        signal.signal(signal.SIGINT, self._handle_exit_signal)
        signal.signal(signal.SIGQUIT, self._handle_force_quit_signal)

    def _handle_exit_signal(self, signum, frame):
        sig_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
        logger.info(f"received signal {sig_name}...")
        self.loop.create_task(self.exit_gracefully())

    def _handle_force_quit_signal(self, signum, frame):
        logger.info(f"received SIGQUIT...")
        self.exit_forcefully()

    async def close(self):
        logger.info(f"closing worker '{self.name}'...")
        self.killing = True
        # self.action_queue.close()
        # self.event_queue.close()

        if self.action_runner is not None:
            self.action_runner.cleanup()

        await self.action_listener_health_check

    async def exit_gracefully(self):
        logger.debug(f"gracefully stopping worker: {self.name}")

        if self.killing:
            return self.exit_forcefully()

        self.killing = True

        if self.action_listener_process and self.action_listener_process.is_alive():
            self.action_listener_process.kill()  # send SIGTERM to the process

        await self.close()

        if self.loop:
            self.loop.stop()

        logger.info(f"👋")

    def exit_forcefully(self):
        self.killing = True

        logger.debug(f"forcefully stopping worker: {self.name}")

        self.close()

        if self.action_listener_process:
            self.action_listener_process.kill()  # Forcefully kill the process

        logger.info(f"👋")
        sys.exit(
            1
        )  # Exit immediately TODO - should we exit with 1 here, there may be other workers to cleanup


def register_on_worker(callable: HatchetCallable, worker: Worker):
    worker.register_function(callable.get_action_name(), callable)

    if callable.function_on_failure is not None:
        worker.register_function(
            callable.function_on_failure.get_action_name(), callable.function_on_failure
        )

    if callable.function_concurrency is not None:
        worker.register_function(
            callable.function_concurrency.get_action_name(),
            callable.function_concurrency,
        )

    opts = callable.to_workflow_opts()

    worker.register_workflow_from_opts(opts.name, opts)
