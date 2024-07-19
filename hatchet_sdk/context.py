import inspect
import json
import traceback
from concurrent.futures import Future, ThreadPoolExecutor

from hatchet_sdk.clients.events import EventClientImpl
from hatchet_sdk.clients.run_event_listener import (
    RunEventListener,
    RunEventListenerClient,
)
from hatchet_sdk.clients.workflow_listener import PooledWorkflowRunListener
from hatchet_sdk.workflow_run import WorkflowRunRef

from .clients.admin import (
    AdminClientImpl,
    ChildTriggerWorkflowOptions,
    ScheduleTriggerWorkflowOptions,
    TriggerWorkflowOptions,
)
from .clients.dispatcher import Action, DispatcherClientImpl
from .dispatcher_pb2 import OverridesData
from .logger import logger

DEFAULT_WORKFLOW_POLLING_INTERVAL = 5  # Seconds


def get_caller_file_path():
    caller_frame = inspect.stack()[2]

    return caller_frame.filename


class BaseContext:
    def _prepare_workflow_options(
        self, key: str = None, options: ChildTriggerWorkflowOptions = None
    ):
        workflow_run_id = self.action.workflow_run_id
        step_run_id = self.action.step_run_id

        trigger_options: TriggerWorkflowOptions = {
            "parent_id": workflow_run_id,
            "parent_step_run_id": step_run_id,
            "child_key": key,
            "child_index": self.spawn_index,
            "additional_metadata": options["additional_metadata"] if options else None,
        }

        self.spawn_index += 1
        return trigger_options


class ContextAioImpl(BaseContext):
    def __init__(
        self,
        action: Action,
        dispatcher_client: DispatcherClientImpl,
        admin_client: AdminClientImpl,
        event_client: EventClientImpl,
        workflow_listener: PooledWorkflowRunListener,
        workflow_run_event_listener: RunEventListenerClient,
        namespace: str = "",
    ):
        self.action = action
        self.dispatcher_client = dispatcher_client
        self.admin_client = admin_client
        self.event_client = event_client
        self.workflow_listener = workflow_listener
        self.workflow_run_event_listener = workflow_run_event_listener
        self.namespace = namespace
        self.spawn_index = -1

    async def spawn_workflow(
        self,
        workflow_name: str,
        input: dict = {},
        key: str = None,
        options: ChildTriggerWorkflowOptions = None,
    ) -> WorkflowRunRef:
        trigger_options = self._prepare_workflow_options(key, options)

        return await self.admin_client.aio.run_workflow(
            workflow_name, input, trigger_options
        )


class Context(BaseContext):
    spawn_index = -1

    def __init__(
        self,
        action: Action,
        dispatcher_client: DispatcherClientImpl,
        admin_client: AdminClientImpl,
        event_client: EventClientImpl,
        workflow_listener: PooledWorkflowRunListener,
        workflow_run_event_listener: RunEventListenerClient,
        namespace: str = "",
    ):
        self.aio = ContextAioImpl(
            action,
            dispatcher_client,
            admin_client,
            event_client,
            workflow_listener,
            workflow_run_event_listener,
            namespace,
        )

        # Check the type of action.action_payload before attempting to load it as JSON
        if isinstance(action.action_payload, (str, bytes, bytearray)):
            try:
                self.data = json.loads(action.action_payload)
            except Exception as e:
                logger.error(f"Error parsing action payload: {e}")
                # Assign an empty dictionary if parsing fails
                self.data = {}
        else:
            # Directly assign the payload to self.data if it's already a dict
            self.data = (
                action.action_payload if isinstance(action.action_payload, dict) else {}
            )

        self.action = action
        # FIXME: stepRunId is a legacy field, we should remove it
        self.stepRunId = action.step_run_id

        self.step_run_id = action.step_run_id
        self.exit_flag = False
        self.dispatcher_client = dispatcher_client
        self.admin_client = admin_client
        self.event_client = event_client
        self.workflow_listener = workflow_listener
        self.workflow_run_event_listener = workflow_run_event_listener
        self.namespace = namespace

        # FIXME: this limits the number of concurrent log requests to 1, which means we can do about
        # 100 log lines per second but this depends on network.
        self.logger_thread_pool = ThreadPoolExecutor(max_workers=1)
        self.stream_event_thread_pool = ThreadPoolExecutor(max_workers=1)

        # store each key in the overrides field in a lookup table
        # overrides_data is a dictionary of key-value pairs
        self.overrides_data = self.data.get("overrides", {})

        if action.get_group_key_run_id != "":
            self.input = self.data
        else:
            self.input = self.data.get("input", {})

    def step_output(self, step: str):
        try:
            return self.data["parents"][step]
        except KeyError:
            raise ValueError(f"Step output for '{step}' not found")

    def triggered_by_event(self) -> bool:
        return self.data.get("triggered_by", "") == "event"

    def workflow_input(self):
        return self.input

    def workflow_run_id(self):
        return self.action.workflow_run_id

    def cancel(self):
        logger.info("Cancelling step...")
        self.exit_flag = True

    # done returns true if the context has been cancelled
    def done(self):
        return self.exit_flag

    def playground(self, name: str, default: str = None):
        # if the key exists in the overrides_data field, return the value
        if name in self.overrides_data:
            return self.overrides_data[name]

        caller_file = get_caller_file_path()

        self.dispatcher_client.put_overrides_data(
            OverridesData(
                stepRunId=self.stepRunId,
                path=name,
                value=json.dumps(default),
                callerFilename=caller_file,
            )
        )

        return default

    def spawn_workflow(
        self,
        workflow_name: str,
        input: dict = {},
        key: str = None,
        options: ChildTriggerWorkflowOptions = None,
    ):
        trigger_options = self._prepare_workflow_options(key, options)

        return self.admin_client.run_workflow(workflow_name, input, trigger_options)

    def _log(self, line: str) -> (bool, Exception):  # type: ignore
        try:
            self.event_client.log(message=line, step_run_id=self.stepRunId)
            return True, None
        except Exception as e:
            # we don't want to raise an exception here, as it will kill the log thread
            return False, e

    def log(self, line, raise_on_error: bool = False):
        if self.stepRunId == "":
            return

        if not isinstance(line, str):
            line = json.dumps(line)

        future: Future = self.logger_thread_pool.submit(self._log, line)

        def handle_result(future: Future):
            success, exception = future.result()
            if not success and exception:
                if raise_on_error:
                    raise exception
                else:
                    thread_trace = "".join(
                        traceback.format_exception(
                            type(exception), exception, exception.__traceback__
                        )
                    )
                    call_site_trace = "".join(traceback.format_stack())
                    logger.error(
                        f"Error in log thread: {exception}\n{thread_trace}\nCalled from:\n{call_site_trace}"
                    )

        future.add_done_callback(handle_result)

    def release_slot(self):
        return self.dispatcher_client.release_slot(self.stepRunId)

    def _put_stream(self, data: str | bytes):
        try:
            self.event_client.stream(data=data, step_run_id=self.stepRunId)
        except Exception as e:
            logger.error(f"Error putting stream event: {e}")

    def put_stream(self, data: str | bytes):
        if self.stepRunId == "":
            return

        self.stream_event_thread_pool.submit(self._put_stream, data)

    def refresh_timeout(self, increment_by: str):
        try:
            return self.dispatcher_client.refresh_timeout(
                step_run_id=self.stepRunId, increment_by=increment_by
            )
        except Exception as e:
            logger.error(f"Error refreshing timeout: {e}")

    def retry_count(self):
        return self.action.retry_count
