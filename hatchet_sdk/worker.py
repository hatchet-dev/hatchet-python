import asyncio
import contextvars
import ctypes
import functools
import json
import logging
import random
import signal
import sys
import threading
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
from hatchet_sdk.clients.events import EventClientImpl
from hatchet_sdk.clients.run_event_listener import new_listener
from hatchet_sdk.clients.workflow_listener import PooledWorkflowRunListener
from hatchet_sdk.loader import ClientConfig

from .client import new_client, new_client_raw
from .clients.dispatcher import (
    Action,
    ActionListenerImpl,
    GetActionListenerRequest,
    new_dispatcher,
)
from .context import Context
from .dispatcher_pb2 import (
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
from .logger import logger
from .workflow import WorkflowMeta

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
    def __init__(self, event_client: EventClientImpl, stream=None):
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
    event_client: EventClientImpl,
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
        self.client = new_client_raw(config)
        self.name = self.client.config.namespace + name
        self.max_runs = max_runs
        self.tasks: Dict[str, asyncio.Task] = {}  # Store run ids and futures
        self.contexts: Dict[str, Context] = {}  # Store run ids and contexts
        self.action_registry: dict[str, Callable[..., Any]] = {}
        self.listener: ActionListenerImpl = None

        # The thread pool is used for synchronous functions which need to run concurrently
        self.thread_pool = ThreadPoolExecutor(max_workers=max_runs)
        self.threads: Dict[str, Thread] = {}  # Store run ids and threads

        self.killing = False
        self.handle_kill = handle_kill

        self._status = WorkerStatus.INITIALIZED

    def step_run_callback(self, action: Action):
        def inner_callback(task: asyncio.Task):
            self.cleanup_run_id(action.step_run_id)

            errored = False
            cancelled = task.cancelled()

            # Get the output from the future
            try:
                if not cancelled:
                    output = task.result()
            except Exception as e:
                errored = True

                # This except is coming from the application itself, so we want to send that to the Hatchet instance
                event = self.get_step_action_event(action, STEP_EVENT_TYPE_FAILED)
                event.eventPayload = str(errorWithTraceback(f"{e}", e))

                try:
                    asyncio.create_task(
                        self.dispatcher_client.send_step_action_event(event)
                    )
                except Exception as e:
                    logger.error(f"Could not send action event: {e}")

            if not errored and not cancelled:
                # Create an action event
                try:
                    event = self.get_step_action_finished_event(action, output)
                except Exception as e:
                    logger.error(f"Could not get action finished event: {e}")
                    raise e

                # Send the action event to the dispatcher
                asyncio.create_task(
                    self.dispatcher_client.send_step_action_event(event)
                )

        return inner_callback

    def group_key_run_callback(self, action: Action):
        def inner_callback(task: asyncio.Task):
            self.cleanup_run_id(action.get_group_key_run_id)

            errored = False
            cancelled = task.cancelled()

            # Get the output from the future
            try:
                if not cancelled:
                    output = task.result()
            except Exception as e:
                errored = True

                # This except is coming from the application itself, so we want to send that to the Hatchet instance
                event = self.get_group_key_action_event(
                    action, GROUP_KEY_EVENT_TYPE_FAILED
                )
                event.eventPayload = str(errorWithTraceback(f"{e}", e))

                try:
                    asyncio.create_task(
                        self.dispatcher_client.send_group_key_action_event(event)
                    )
                except Exception as e:
                    logger.error(f"Could not send action event: {e}")

            if not errored and not cancelled:
                # Create an action event
                try:
                    event = self.get_group_key_action_finished_event(action, output)
                except Exception as e:
                    logger.error(f"Could not get action finished event: {e}")
                    raise e

                # Send the action event to the dispatcher
                asyncio.create_task(
                    self.dispatcher_client.send_group_key_action_event(event)
                )

        return inner_callback

    def thread_action_func(self, context, action_func, action: Action):
        if action.step_run_id is not None and action.step_run_id != "":
            self.threads[action.step_run_id] = current_thread()
        elif (
            action.get_group_key_run_id is not None
            and action.get_group_key_run_id != ""
        ):
            self.threads[action.get_group_key_run_id] = current_thread()

        return action_func(context)

    # We wrap all actions in an async func
    async def async_wrapped_action_func(
        self, context: Context, action_func, action: Action, run_id: str
    ):
        wr.set(context.workflow_run_id())
        sr.set(context.step_run_id)

        try:
            if action_func._is_coroutine:
                return await action_func(context)
            else:
                pfunc = functools.partial(
                    # we must copy the context vars to the new thread, as only asyncio natively supports
                    # contextvars
                    copy_context_vars,
                    contextvars.copy_context().items(),
                    self.thread_action_func,
                    context,
                    action_func,
                    action,
                )
                res = await self.loop.run_in_executor(self.thread_pool, pfunc)

                return res
        except Exception as e:
            logger.error(errorWithTraceback(f"Could not execute action: {e}", e))
            raise e
        finally:
            self.cleanup_run_id(run_id)

    def cleanup_run_id(self, run_id: str):
        if run_id in self.tasks:
            del self.tasks[run_id]

        if run_id in self.threads:
            del self.threads[run_id]

        if run_id in self.contexts:
            del self.contexts[run_id]

    async def handle_start_step_run(self, action: Action):
        logger.debug(f"Starting step run {action.step_run_id}")

        action_name = action.action_id
        context = Context(
            action,
            self.dispatcher_client,
            self.admin_client,
            self.client.event,
            self.client.workflow_listener,
            self.workflow_run_event_listener,
            self.client.config.namespace,
        )
        self.contexts[action.step_run_id] = context

        # Find the corresponding action function from the registry
        action_func = self.action_registry.get(action_name)

        if action_func:
            # send an event that the step run has started
            try:
                event = self.get_step_action_event(action, STEP_EVENT_TYPE_STARTED)

                # Send the action event to the dispatcher
                asyncio.create_task(
                    self.dispatcher_client.send_step_action_event(event)
                )
            except Exception as e:
                logger.error(f"Could not send action event: {e}")

            task = self.loop.create_task(
                self.async_wrapped_action_func(
                    context, action_func, action, action.step_run_id
                )
            )

            task.add_done_callback(self.step_run_callback(action))
            self.tasks[action.step_run_id] = task

            try:
                await task
            except Exception as e:
                # do nothing, this should be caught in the callback
                pass

        logger.debug(f"Finished step run {action.step_run_id}")

    async def handle_start_group_key_run(self, action: Action):
        action_name = action.action_id
        context = Context(
            action,
            self.dispatcher_client,
            self.admin_client,
            self.client.event,
            self.client.workflow_listener,
            self.workflow_run_event_listener,
            self.client.config.namespace,
        )
        self.contexts[action.get_group_key_run_id] = context

        # Find the corresponding action function from the registry
        action_func = self.action_registry.get(action_name)

        if action_func:
            # send an event that the group key run has started
            try:
                event = self.get_group_key_action_event(
                    action, GROUP_KEY_EVENT_TYPE_STARTED
                )

                # Send the action event to the dispatcher
                asyncio.create_task(
                    self.dispatcher_client.send_group_key_action_event(event)
                )
            except Exception as e:
                logger.error(f"Could not send action event: {e}")

            task = self.loop.create_task(
                self.async_wrapped_action_func(
                    context, action_func, action, action.get_group_key_run_id
                )
            )

            task.add_done_callback(self.group_key_run_callback(action))
            self.tasks[action.get_group_key_run_id] = task

            try:
                await task
            except Exception as e:
                # do nothing, this should be caught in the callback
                pass

    def force_kill_thread(self, thread):
        """Terminate a python threading.Thread."""
        try:
            if not thread.is_alive():
                return

            logger.info(f"Forcefully terminating thread {thread.ident}")

            exc = ctypes.py_object(SystemExit)
            res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_long(thread.ident), exc
            )
            if res == 0:
                raise ValueError("Invalid thread ID")
            elif res != 1:
                logger.error("PyThreadState_SetAsyncExc failed")

                # Call with exception set to 0 is needed to cleanup properly.
                ctypes.pythonapi.PyThreadState_SetAsyncExc(thread.ident, 0)
                raise SystemError("PyThreadState_SetAsyncExc failed")

            logger.info(f"Successfully terminated thread {thread.ident}")

            # Immediately add a new thread to the thread pool, because we've actually killed a worker
            # in the ThreadPoolExecutor
            self.thread_pool.submit(lambda: None)
        except Exception as e:
            logger.exception(f"Failed to terminate thread: {e}")

    async def handle_cancel_action(self, run_id: str):
        try:
            # call cancel to signal the context to stop
            if run_id in self.contexts:
                context = self.contexts.get(run_id)
                context.cancel()

            await asyncio.sleep(1)

            if run_id in self.tasks:
                future = self.tasks.get(run_id)

                if future:
                    future.cancel()

            # check if thread is still running, if so, print a warning
            if run_id in self.threads:
                logger.warning(
                    f"Thread {self.threads[run_id].ident} with run id {run_id} is still running after cancellation. This could cause the thread pool to get blocked and prevent new tasks from running."
                )
        finally:
            self.cleanup_run_id(run_id)

    def get_step_action_event(
        self, action: Action, event_type: StepActionEventType
    ) -> StepActionEvent:
        eventTimestamp = Timestamp()
        eventTimestamp.GetCurrentTime()

        return StepActionEvent(
            workerId=action.worker_id,
            jobId=action.job_id,
            jobRunId=action.job_run_id,
            stepId=action.step_id,
            stepRunId=action.step_run_id,
            actionId=action.action_id,
            eventTimestamp=eventTimestamp,
            eventType=event_type,
        )

    def get_step_action_finished_event(
        self, action: Action, output: Any
    ) -> StepActionEvent:
        try:
            event = self.get_step_action_event(action, STEP_EVENT_TYPE_COMPLETED)
        except Exception as e:
            logger.error(f"Could not create action finished event: {e}")
            raise e

        output_bytes = ""

        if output is not None:
            output_bytes = json.dumps(output)

        event.eventPayload = output_bytes

        return event

    def get_group_key_action_event(
        self, action: Action, event_type: GroupKeyActionEventType
    ) -> GroupKeyActionEvent:
        eventTimestamp = Timestamp()
        eventTimestamp.GetCurrentTime()

        return GroupKeyActionEvent(
            workerId=action.worker_id,
            workflowRunId=action.workflow_run_id,
            getGroupKeyRunId=action.get_group_key_run_id,
            actionId=action.action_id,
            eventTimestamp=eventTimestamp,
            eventType=event_type,
        )

    def get_group_key_action_finished_event(
        self, action: Action, output: str
    ) -> StepActionEvent:
        try:
            event = self.get_group_key_action_event(
                action, GROUP_KEY_EVENT_TYPE_COMPLETED
            )
        except Exception as e:
            logger.error(f"Could not create action finished event: {e}")
            raise e

        try:
            event.eventPayload = output
        except Exception as e:
            event.eventPayload = ""

        return event

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
        if self.killing:
            self.exit_forcefully()
            return

        self.killing = True

        logger.info(f"Exiting gracefully...")

        try:
            self.listener.unregister()
        except Exception as e:
            logger.error(f"Could not unregister worker: {e}")

        try:
            logger.info("Waiting for tasks to finish...")

            await self.wait_for_tasks()
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

        try:
            self.listener.unregister()
        except Exception as e:
            logger.error(f"Could not unregister worker: {e}")

        self.loop.stop()

    async def wait_for_tasks(self):
        # wait for all futures to finish
        for taskId in list(self.tasks.keys()):
            try:
                logger.info(f"Waiting for task {taskId} to finish...")

                if taskId in self.tasks:
                    await self.tasks.get(taskId)
            except Exception as e:
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

    def start(self, retry_count=1):
        self._status = WorkerStatus.STARTING

        try:
            loop = asyncio.get_running_loop()
            self.loop = loop
            created_loop = False
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            created_loop = True

        self.loop.create_task(self.async_start(retry_count))

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
        logger.info("Starting worker...")
        self.loop = asyncio.get_running_loop()

        try:
            # We need to initialize a new admin and dispatcher client *after* we've started the event loop,
            # otherwise the grpc.aio methods will use a different event loop and we'll get a bunch of errors.
            self.dispatcher_client = new_dispatcher(self.config)
            self.admin_client = new_admin(self.config)
            self.workflow_run_event_listener = new_listener(self.config)
            self.client.workflow_listener = PooledWorkflowRunListener(self.config)

            self.listener: ActionListenerImpl = (
                await self.dispatcher_client.get_action_listener(
                    GetActionListenerRequest(
                        worker_name=self.name,
                        services=["default"],
                        actions=self.action_registry.keys(),
                        max_runs=self.max_runs,
                    )
                )
            )

            # It's important that this iterates async so it doesn't block the event loop. This is
            # what allows self.loop.create_task to work.
            async for action in self.listener:
                if action.action_type == ActionType.START_STEP_RUN:
                    logger.debug(f"Got start step run action: {action.step_run_id}")
                    self.loop.create_task(self.handle_start_step_run(action))
                elif action.action_type == ActionType.CANCEL_STEP_RUN:
                    logger.debug(f"Got cancel step run action: {action.step_run_id}")
                    self.loop.create_task(self.handle_cancel_action(action.step_run_id))
                elif action.action_type == ActionType.START_GET_GROUP_KEY:
                    self.loop.create_task(self.handle_start_group_key_run(action))
                else:
                    logger.error(f"Unknown action type: {action.action_type}")

        except grpc.RpcError as rpc_error:
            logger.error(f"Could not start worker: {rpc_error}")

        if not self.killing:
            logger.info("Could not start worker")


def errorWithTraceback(message: str, e: Exception):
    trace = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    return f"{message}\n{trace}"
