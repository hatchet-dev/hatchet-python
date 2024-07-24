import asyncio
import contextvars
import ctypes
import functools
import json
import logging
import multiprocessing
import os
import signal
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from enum import Enum
from io import StringIO
from logging import StreamHandler
from threading import Thread, current_thread
from typing import Any, Callable, Coroutine, Dict

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from hatchet_sdk.clients.admin import new_admin
from hatchet_sdk.clients.events import EventClient
from hatchet_sdk.clients.run_event_listener import new_listener
from hatchet_sdk.clients.workflow_listener import PooledWorkflowRunListener
from hatchet_sdk.contracts.dispatcher_pb2 import (
    GROUP_KEY_EVENT_TYPE_COMPLETED,
    GROUP_KEY_EVENT_TYPE_FAILED,
    GROUP_KEY_EVENT_TYPE_STARTED,
    STEP_EVENT_TYPE_COMPLETED,
    STEP_EVENT_TYPE_FAILED,
    STEP_EVENT_TYPE_STARTED,
    ActionType,
    GroupKeyActionEvent,
    GroupKeyActionEventType,
    StepActionEvent,
    StepActionEventType,
)
from hatchet_sdk.loader import ClientConfig
from hatchet_sdk.logger import logger
from hatchet_sdk.worker.action_listener import WorkerActionProcess, worker_process

from ..client import new_client, new_client_raw
from ..clients.dispatcher import (
    Action,
    ActionListenerImpl,
    GetActionListenerRequest,
    new_dispatcher,
)
from ..context import Context
from ..workflow import WorkflowMeta

wr: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "workflow_run_id", default=None
)
sr: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "step_run_id", default=None
)


def copy_context_vars(ctx_vars, func, *args, **kwargs):
    for var, value in ctx_vars:
        var.set(value)
    return func(*args, **kwargs)


class InjectingFilter(logging.Filter):
    # For some reason, only the InjectingFilter has access to the contextvars method sr.get(),
    # otherwise we would use emit within the CustomLogHandler
    def filter(self, record):
        record.workflow_run_id = wr.get()
        record.step_run_id = sr.get()
        return True


# Custom log handler to process log lines
class CustomLogHandler(StreamHandler):
    def __init__(self, event_client: EventClient, stream=None):
        super().__init__(stream)
        self.logger_thread_pool = ThreadPoolExecutor(max_workers=1)
        self.event_client = event_client

    def _log(self, line: str, step_run_id: str | None):
        try:
            if not step_run_id:
                return

            self.event_client.log(message=line, step_run_id=step_run_id)
        except Exception as e:
            logger.error(f"Error logging: {e}")

    def emit(self, record):
        super().emit(record)

        log_entry = self.format(record)
        self.logger_thread_pool.submit(self._log, log_entry, record.step_run_id)


def capture_logs(
    logger: logging.Logger,
    event_client: EventClient,
    func: Coroutine[Any, Any, Any],
):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        if not logger:
            raise Exception("No logger configured on client")

        log_stream = StringIO()
        custom_handler = CustomLogHandler(event_client, log_stream)
        custom_handler.setLevel(logging.INFO)
        custom_handler.addFilter(InjectingFilter())
        logger.addHandler(custom_handler)

        try:
            result = await func(*args, **kwargs)
        finally:
            custom_handler.flush()
            logger.removeHandler(custom_handler)
            log_stream.close()

        return result

    return wrapper


class WorkerStatus(Enum):
    INITIALIZED = 1
    STARTING = 2
    HEALTHY = 3
    UNHEALTHY = 4


class Worker:
    def __init__(
        self,
        name: str,
        max_runs: int | None = None,
        debug=False,
        handle_kill=True,
        config: ClientConfig = {},
    ):
        # We store the config so we can dynamically create clients for the dispatcher client.
        self.config = config
        self.client = new_client_raw(config, debug)
        self.name = self.client.config.namespace + name
        self.max_runs = max_runs
        self.tasks: Dict[str, asyncio.Task] = {}  # Store run ids and futures
        self.contexts: Dict[str, Context] = {}  # Store run ids and contexts
        self.action_registry: dict[str, Callable[..., Any]] = {}

        self.killing = False
        self.handle_kill = handle_kill

        self._status = WorkerStatus.INITIALIZED

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

    async def exit_gracefully(self):
        logger.debug(f"stopping worker: {self.name}")

        #   TODO
        pass

    def status(self) -> WorkerStatus:
        if self.listener:
            if self.listener.is_healthy():
                self._status = WorkerStatus.HEALTHY
                return WorkerStatus.HEALTHY
            else:
                self._status = WorkerStatus.UNHEALTHY
                return WorkerStatus.UNHEALTHY

        return self._status

    def start(self):
        logger.debug(f"starting worker: {self.name}")

        main_pid = os.getpid()
        logger.debug(f"worker supervisor starting on PID: {main_pid}")

        action_list = [str(key) for key in self.action_registry.keys()]

        process = multiprocessing.Process(
            target=worker_process,
            args=(
                self.name,
                action_list,
                self.max_runs,
                self.config,
                self.handle_kill,
                self.client.debug,
            ),
        )
        process.start()
        logger.debug(f"listener starting on PID: {process.pid}")
        process.join()
        logger.debug(f"listener exited on PID: {process.pid}")
