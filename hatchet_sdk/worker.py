import asyncio
import ctypes
import functools
import json
import signal
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from threading import Thread, current_thread
from typing import Any, Callable, Dict

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from hatchet_sdk.loader import ClientConfig

from .client import new_client
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
        self.client = new_client(config)
        self.name = self.client.config.namespace + name
        self.max_runs = max_runs
        self.tasks: Dict[str, asyncio.Task] = {}  # Store step run ids and futures
        self.contexts: Dict[str, Context] = {}  # Store step run ids and contexts
        self.action_registry: dict[str, Callable[..., Any]] = {}

        # The thread pool is used for synchronous functions which need to run concurrently
        self.thread_pool = ThreadPoolExecutor(max_workers=max_runs)
        self.threads: Dict[str, Thread] = {}  # Store step run ids and threads

        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

        self.killing = False
        self.handle_kill = handle_kill

    async def handle_start_step_run(self, action: Action):
        await asyncio.sleep(0.0)

        action_name = action.action_id
        context = Context(action, self.client)

        self.contexts[action.step_run_id] = context

        # Find the corresponding action function from the registry
        action_func = self.action_registry.get(action_name)

        if action_func:

            def callback(task: asyncio.Task):
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
                        self.dispatcher_client.send_step_action_event(event)
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
                    self.dispatcher_client.send_step_action_event(event)

                # Remove the future from the dictionary
                if action.step_run_id in self.tasks:
                    del self.tasks[action.step_run_id]

            def thread_action_func(context, action_func):
                self.threads[action.step_run_id] = current_thread()
                return action_func(context)

            # We wrap all actions in an async func
            async def async_wrapped_action_func(context):
                try:
                    if action_func._is_coroutine:
                        return await action_func(context)
                    else:

                        pfunc = functools.partial(
                            thread_action_func, context, action_func
                        )
                        res = await self.loop.run_in_executor(self.thread_pool, pfunc)

                        if action.step_run_id in self.threads:
                            # remove the thread id
                            logger.debug(
                                f"Removing step run id {action.step_run_id} from threads"
                            )

                            del self.threads[action.step_run_id]

                        return res
                except Exception as e:
                    logger.error(
                        errorWithTraceback(f"Could not execute action: {e}", e)
                    )
                    raise e
                finally:
                    if action.step_run_id in self.tasks:
                        del self.tasks[action.step_run_id]

            task = self.loop.create_task(async_wrapped_action_func(context))
            task.add_done_callback(callback)
            self.tasks[action.step_run_id] = task

            # send an event that the step run has started
            try:
                event = self.get_step_action_event(action, STEP_EVENT_TYPE_STARTED)
            except Exception as e:
                logger.error(f"Could not create action event: {e}")

            # Send the action event to the dispatcher
            self.dispatcher_client.send_step_action_event(event)

            await task

    async def handle_start_group_key_run(self, action: Action):
        action_name = action.action_id
        context = Context(action, self.client)

        self.contexts[action.get_group_key_run_id] = context

        # Find the corresponding action function from the registry
        action_func = self.action_registry.get(action_name)

        if action_func:

            def callback(task: asyncio.Task):
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
                        self.dispatcher_client.send_group_key_action_event(event)
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
                    self.dispatcher_client.send_group_key_action_event(event)

                # Remove the future from the dictionary
                if action.get_group_key_run_id in self.tasks:
                    del self.tasks[action.get_group_key_run_id]

            def thread_action_func(context, action_func):
                self.threads[action.step_run_id] = current_thread()
                return action_func(context)

            # We wrap all actions in an async func
            async def async_wrapped_action_func(context):
                try:
                    if action_func._is_coroutine:
                        return await action_func(context)
                    else:

                        pfunc = functools.partial(
                            thread_action_func, context, action_func
                        )
                        res = await self.loop.run_in_executor(self.thread_pool, pfunc)

                        if action.step_run_id in self.threads:
                            # remove the thread id
                            logger.debug(
                                f"Removing step run id {action.step_run_id} from threads"
                            )

                            del self.threads[action.step_run_id]

                        return res
                except Exception as e:
                    logger.error(
                        errorWithTraceback(f"Could not execute action: {e}", e)
                    )
                    raise e
                finally:
                    if action.step_run_id in self.tasks:
                        del self.tasks[action.step_run_id]

            task = self.loop.create_task(async_wrapped_action_func(context))
            task.add_done_callback(callback)
            self.tasks[action.get_group_key_run_id] = task

            # send an event that the step run has started
            try:
                event = self.get_group_key_action_event(
                    action, GROUP_KEY_EVENT_TYPE_STARTED
                )
            except Exception as e:
                logger.error(f"Could not create action event: {e}")

            # Send the action event to the dispatcher
            self.dispatcher_client.send_group_key_action_event(event)

            await task

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
        # call cancel to signal the context to stop
        if run_id in self.contexts:
            context = self.contexts.get(run_id)
            context.cancel()

        if run_id in self.tasks:
            future = self.tasks.get(run_id)

            if future:
                future.cancel()

                if run_id in self.tasks:
                    del self.tasks[run_id]

        # grace period of 1 second
        time.sleep(1)

        # check if thread is still running, if so, kill it
        if run_id in self.threads:
            thread = self.threads[run_id]

            if thread:
                self.force_kill_thread(thread)

                if run_id in self.threads:
                    del self.threads[run_id]

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
        self.client.admin.put_workflow(workflow.get_name(), workflow.get_create_opts())

        def create_action_function(action_func):
            def action_function(context):
                return action_func(workflow, context)

            if asyncio.iscoroutinefunction(action_func):
                action_function._is_coroutine = True
            else:
                action_function._is_coroutine = False

            return action_function

        for action_name, action_func in workflow.get_actions():
            self.action_registry[action_name] = create_action_function(action_func)

    def exit_gracefully(self, signum, frame):
        self.killing = True

        logger.info("Gracefully exiting hatchet worker...")

        try:
            self.listener.unregister()
        except Exception as e:
            logger.error(f"Could not unregister worker: {e}")

        # cancel all futures
        # for future in self.tasks.values():
        #     try:
        #         future.result()
        #     except Exception as e:
        #         logger.error(f"Could not wait for future: {e}")

        if self.handle_kill:
            logger.info("Exiting...")
            sys.exit(0)

    def start(self, retry_count=1):
        try:
            loop = asyncio.get_running_loop()
            self.loop = loop
            created_loop = False
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            created_loop = True

        self.loop.create_task(self.async_start(retry_count))

        if created_loop:
            self.loop.run_forever()

    async def async_start(self, retry_count=1):
        logger.info("Starting worker...")
        self.loop = asyncio.get_running_loop()

        try:
            # We need to initialize a new dispatcher *after* we've started the event loop, otherwise
            # the grpc.aio methods will use a different event loop and we'll get a bunch of errors.
            self.dispatcher_client = new_dispatcher(self.config)

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
                    self.loop.create_task(self.handle_start_step_run(action))
                elif action.action_type == ActionType.CANCEL_STEP_RUN:
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
