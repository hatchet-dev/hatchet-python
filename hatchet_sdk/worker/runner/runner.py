import asyncio
import contextvars
import ctypes
import functools
import json
import traceback
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from multiprocessing import Queue
from threading import Thread, current_thread
from typing import Any, Callable, Dict

from opentelemetry.trace import StatusCode

from hatchet_sdk.client import new_client_raw
from hatchet_sdk.clients.admin import new_admin
from hatchet_sdk.clients.dispatcher.action_listener import Action
from hatchet_sdk.clients.dispatcher.dispatcher import new_dispatcher
from hatchet_sdk.clients.run_event_listener import new_listener
from hatchet_sdk.clients.workflow_listener import PooledWorkflowRunListener
from hatchet_sdk.context import Context
from hatchet_sdk.context.worker_context import WorkerContext
from hatchet_sdk.contracts.dispatcher_pb2 import (
    GROUP_KEY_EVENT_TYPE_COMPLETED,
    GROUP_KEY_EVENT_TYPE_FAILED,
    GROUP_KEY_EVENT_TYPE_STARTED,
    STEP_EVENT_TYPE_COMPLETED,
    STEP_EVENT_TYPE_FAILED,
    STEP_EVENT_TYPE_STARTED,
    ActionType,
)
from hatchet_sdk.loader import ClientConfig
from hatchet_sdk.logger import logger
from hatchet_sdk.v2.callable import DurableContext
from hatchet_sdk.worker.action_listener_process import ActionEvent
from hatchet_sdk.worker.runner.utils.capture_logs import copy_context_vars, sr, wr
from hatchet_sdk.utils.tracing import parse_carrier_from_metadata, create_tracer


class WorkerStatus(Enum):
    INITIALIZED = 1
    STARTING = 2
    HEALTHY = 3
    UNHEALTHY = 4


class Runner:
    def __init__(
        self,
        name: str,
        event_queue: Queue,
        max_runs: int | None = None,
        handle_kill: bool = True,
        action_registry: dict[str, Callable[..., Any]] = {},
        config: ClientConfig = {},
        labels: dict[str, str | int] = {},
    ):
        # We store the config so we can dynamically create clients for the dispatcher client.
        self.config = config
        self.client = new_client_raw(config)
        self.name = self.client.config.namespace + name
        self.max_runs = max_runs
        self.tasks: Dict[str, asyncio.Task] = {}  # Store run ids and futures
        self.contexts: Dict[str, Context] = {}  # Store run ids and contexts
        self.action_registry: dict[str, Callable[..., Any]] = action_registry

        self.event_queue = event_queue

        # The thread pool is used for synchronous functions which need to run concurrently
        self.thread_pool = ThreadPoolExecutor(max_workers=max_runs)
        self.threads: Dict[str, Thread] = {}  # Store run ids and threads

        self.killing = False
        self.handle_kill = handle_kill

        # We need to initialize a new admin and dispatcher client *after* we've started the event loop,
        # otherwise the grpc.aio methods will use a different event loop and we'll get a bunch of errors.
        self.dispatcher_client = new_dispatcher(self.config)
        self.admin_client = new_admin(self.config)
        self.workflow_run_event_listener = new_listener(self.config)
        self.client.workflow_listener = PooledWorkflowRunListener(self.config)

        self.worker_context = WorkerContext(
            labels=labels, client=new_client_raw(config).dispatcher
        )

        self.otel_tracer = create_tracer(name=__name__, config=config)

    def create_workflow_run_url(self, action: Action) -> str:
        return f"{self.config.server_url}/workflow-runs/{action.workflow_run_id}?tenant={action.tenant_id}"

    def run(self, action: Action):
        ctx = parse_carrier_from_metadata(action.additional_metadata)

        with self.otel_tracer.start_as_current_span(
            "hatchet.worker.runner.runner.Runner.run", context=ctx
        ) as span:
            if self.worker_context.id() is None:
                self.worker_context._worker_id = action.worker_id

            span.set_attributes(action.otel_attributes)
            span.set_attribute("workflow_run_url", self.create_workflow_run_url(action))

            match action.action_type:
                case ActionType.START_STEP_RUN:
                    log = f"run: start step: {action.action_id}/{action.step_run_id}"
                    span.add_event(log)
                    logger.info(log)
                    asyncio.create_task(self.handle_start_step_run(action))
                case ActionType.CANCEL_STEP_RUN:
                    log = f"cancel: step run:  {action.action_id}/{action.step_run_id}"
                    span.add_event(log)
                    logger.info(log)
                    asyncio.create_task(self.handle_cancel_action(action.step_run_id))
                case ActionType.START_GET_GROUP_KEY:
                    log = f"run: get group key:  {action.action_id}/{action.get_group_key_run_id}"
                    span.add_event(log)
                    logger.info(log)
                    asyncio.create_task(self.handle_start_group_key_run(action))
                case _:
                    log = f"unknown action type: {action.action_type}"
                    span.add_event(log)
                    logger.error(log)

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
                self.event_queue.put(
                    ActionEvent(
                        action=action,
                        type=STEP_EVENT_TYPE_FAILED,
                        payload=str(errorWithTraceback(f"{e}", e)),
                    )
                )

                logger.error(
                    f"failed step run: {action.action_id}/{action.step_run_id}"
                )

            if not errored and not cancelled:
                self.event_queue.put(
                    ActionEvent(
                        action=action,
                        type=STEP_EVENT_TYPE_COMPLETED,
                        payload=self.serialize_output(output),
                    )
                )

                logger.info(
                    f"finished step run: {action.action_id}/{action.step_run_id}"
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
                self.event_queue.put(
                    ActionEvent(
                        action=action,
                        type=GROUP_KEY_EVENT_TYPE_FAILED,
                        payload=str(errorWithTraceback(f"{e}", e)),
                    )
                )

                logger.error(
                    f"failed step run: {action.action_id}/{action.step_run_id}"
                )

            if not errored and not cancelled:
                self.event_queue.put(
                    ActionEvent(
                        action=action,
                        type=GROUP_KEY_EVENT_TYPE_COMPLETED,
                        payload=self.serialize_output(output),
                    )
                )

                logger.info(
                    f"finished step run: {action.action_id}/{action.step_run_id}"
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
            if (
                hasattr(action_func, "is_coroutine") and action_func.is_coroutine
            ) or asyncio.iscoroutinefunction(action_func):
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

                loop = asyncio.get_event_loop()
                res = await loop.run_in_executor(self.thread_pool, pfunc)

                return res
        except Exception as e:
            logger.error(
                errorWithTraceback(
                    f"exception raised in action ({action.action_id}, retry={action.retry_count}):\n{e}",
                    e,
                )
            )
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

    def create_context(
        self, action: Action, action_func: Callable[..., Any] | None
    ) -> Context | DurableContext:
        if hasattr(action_func, "durable") and action_func.durable:
            return DurableContext(
                action,
                self.dispatcher_client,
                self.admin_client,
                self.client.event,
                self.client.rest,
                self.client.workflow_listener,
                self.workflow_run_event_listener,
                self.worker_context,
                self.client.config.namespace,
            )

        return Context(
            action,
            self.dispatcher_client,
            self.admin_client,
            self.client.event,
            self.client.rest,
            self.client.workflow_listener,
            self.workflow_run_event_listener,
            self.worker_context,
            self.client.config.namespace,
        )

    async def handle_start_step_run(self, action: Action):
        with self.otel_tracer.start_as_current_span(
            "hatchet.worker.runner.runner.Runner.handle_start_step_run"
        ) as span:
            span.add_event("Starting step run")
            action_name = action.action_id

            # Find the corresponding action function from the registry
            action_func = self.action_registry.get(action_name)

            context = self.create_context(action, action_func)

            self.contexts[action.step_run_id] = context

            if action_func:
                self.event_queue.put(
                    ActionEvent(
                        action=action,
                        type=STEP_EVENT_TYPE_STARTED,
                    )
                )

                loop = asyncio.get_event_loop()
                task = loop.create_task(
                    self.async_wrapped_action_func(
                        context, action_func, action, action.step_run_id
                    )
                )

                task.add_done_callback(self.step_run_callback(action))
                self.tasks[action.step_run_id] = task

                try:
                    await task
                    span.set_status(StatusCode.OK)
                except Exception as e:
                    # do nothing, this should be caught in the callback
                    span.set_status(StatusCode.ERROR)
                    span.record_exception(e)

            span.add_event("Finished step run")

    async def handle_start_group_key_run(self, action: Action):
        with self.otel_tracer.start_as_current_span(
            "hatchet.worker.runner.runner.Runner.handle_start_step_run"
        ) as span:
            span.add_event("Starting group key run")
            action_name = action.action_id
            context = Context(
                action,
                self.dispatcher_client,
                self.admin_client,
                self.client.event,
                self.client.rest,
                self.client.workflow_listener,
                self.workflow_run_event_listener,
                self.worker_context,
                self.client.config.namespace,
            )
            self.contexts[action.get_group_key_run_id] = context

            # Find the corresponding action function from the registry
            action_func = self.action_registry.get(action_name)

            if action_func:
                # send an event that the group key run has started
                self.event_queue.put(
                    ActionEvent(
                        action=action,
                        type=GROUP_KEY_EVENT_TYPE_STARTED,
                    )
                )

                loop = asyncio.get_event_loop()
                task = loop.create_task(
                    self.async_wrapped_action_func(
                        context, action_func, action, action.get_group_key_run_id
                    )
                )

                task.add_done_callback(self.group_key_run_callback(action))
                self.tasks[action.get_group_key_run_id] = task

                try:
                    await task
                    span.set_status(StatusCode.OK)
                except Exception as e:
                    # do nothing, this should be caught in the callback
                    span.set_status(StatusCode.ERROR)
                    span.record_exception(e)

            span.add_event("Finished group key run")

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
        with self.otel_tracer.start_as_current_span(
            "hatchet.worker.runner.runner.Runner.handle_cancel_action"
        ) as span:
            span.add_event(f"Cancelling run id: {run_id}")
            span.set_attribute("run_id", run_id)

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

                span.set_status(StatusCode.OK)
            finally:
                self.cleanup_run_id(run_id)
                span.add_event(f"Finished cancelling run id: {run_id}")

    def serialize_output(self, output: Any) -> str:
        output_bytes = ""
        if output is not None:
            try:
                output_bytes = json.dumps(output)
            except Exception as e:
                logger.error(f"Could not serialize output: {e}")
                output_bytes = str(output)
        return output_bytes

    async def wait_for_tasks(self):
        running = len(self.tasks.keys())
        while running > 0:
            logger.info(f"waiting for {running} tasks to finish...")
            await asyncio.sleep(1)
            running = len(self.tasks.keys())


def errorWithTraceback(message: str, e: Exception):
    trace = "".join(traceback.format_exception(type(e), e, e.__traceback__))
    return f"{message}\n{trace}"
