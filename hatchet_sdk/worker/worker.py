import asyncio
import contextvars
import ctypes
from dataclasses import dataclass, field
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
from typing import Any, Callable, Coroutine, Dict, Optional

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

from ..client import Client, new_client, new_client_raw
from ..clients.dispatcher import (
    Action,
    ActionListenerImpl,
    GetActionListenerRequest,
    new_dispatcher,
)
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

    listener_process: multiprocessing.Process = field(init=False, default=None)


    def __post_init__(self):
        self.client = new_client_raw(self.config, self.debug)
        self.name = self.client.config.namespace + self.name
        self.setup_signal_handlers()



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

    def start(self):
        main_pid = os.getpid()
        logger.debug(f"worker supervisor starting on PID: {main_pid}")

        self.listener_process = self.start_listener()
        self.listener_process.join()

    def start_listener(self):
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

        return process
    
    def setup_signal_handlers(self):
        signal.signal(signal.SIGTERM, self.handle_exit_signal)
        signal.signal(signal.SIGINT, self.handle_exit_signal)
        signal.signal(signal.SIGQUIT, self.handle_force_quit_signal)

    def handle_exit_signal(self, signum, frame):
        sig_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
        logger.info(f"received signal {sig_name}...")
        asyncio.run(self.exit_gracefully())

    def handle_force_quit_signal(self, signum, frame):
        logger.info(f"received SIGQUIT...")
        self.exit_forcefully()

    async def exit_gracefully(self):
        logger.debug(f"gracefully stopping worker: {self.name}")
  
        if self.listener_process:
            self.listener_process.terminate()
            self.listener_process.join(timeout=10)  # Wait up to 10 seconds for process to terminate
        # Add any other cleanup logic

        logger.info(f"👋")

    def exit_forcefully(self):
        logger.debug(f"forcefully stopping worker: {self.name}")

        if self.listener_process:
            self.listener_process.kill()  # Forcefully kill the process
        # Add any other immediate shutdown logic

        logger.info(f"👋")
        sys.exit(1)  # Exit immediately