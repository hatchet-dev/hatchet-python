import inspect
import json
import traceback
from concurrent.futures import Future, ThreadPoolExecutor

from tenacity import retry, stop_after_attempt, wait_exponential_jitter

from hatchet_sdk.clients.events import EventClient
from hatchet_sdk.clients.rest_client import RestApi
from hatchet_sdk.clients.run_event_listener import RunEventListenerClient
from hatchet_sdk.clients.workflow_listener import PooledWorkflowRunListener
from hatchet_sdk.context.worker_context import WorkerContext
from hatchet_sdk.contracts.dispatcher_pb2 import OverridesData
from hatchet_sdk.workflow_run import WorkflowRunRef

from ..clients.admin import (
    AdminClient,
    ChildTriggerWorkflowOptions,
    TriggerWorkflowOptions,
)
from ..clients.dispatcher.dispatcher import Action, DispatcherClient
from ..logger import logger

DEFAULT_WORKFLOW_POLLING_INTERVAL = 5  # Seconds


def get_caller_file_path():
    caller_frame = inspect.stack()[2]

    return caller_frame.filename


class BaseContext:
    def _prepare_workflow_options(
        self,
        key: str = None,
        options: ChildTriggerWorkflowOptions | None = None,
        worker_id: str = None,
    ):
        workflow_run_id = self.action.workflow_run_id
        step_run_id = self.action.step_run_id

        desired_worker_id = None
        if options is not None and "sticky" in options and options["sticky"] == True:
            desired_worker_id = worker_id

        meta = None
        if options is not None and "additional_metadata" in options:
            meta = options["additional_metadata"]

        trigger_options: TriggerWorkflowOptions = {
            "parent_id": workflow_run_id,
            "parent_step_run_id": step_run_id,
            "child_key": key,
            "child_index": self.spawn_index,
            "additional_metadata": meta,
            "desired_worker_id": desired_worker_id,
        }

        self.spawn_index += 1
        return trigger_options


class ContextAioImpl(BaseContext):
    def __init__(
        self,
        action: Action,
        dispatcher_client: DispatcherClient,
        admin_client: AdminClient,
        event_client: EventClient,
        rest_client: RestApi,
        workflow_listener: PooledWorkflowRunListener,
        workflow_run_event_listener: RunEventListenerClient,
        worker: WorkerContext,
        namespace: str = "",
    ):
        self.action = action
        self.dispatcher_client = dispatcher_client
        self.admin_client = admin_client
        self.event_client = event_client
        self.rest_client = rest_client
        self.workflow_listener = workflow_listener
        self.workflow_run_event_listener = workflow_run_event_listener
        self.namespace = namespace
        self.spawn_index = -1
        self.worker = worker

    @retry(
        wait=wait_exponential_jitter(),
        stop=stop_after_attempt(5),
    )
    async def spawn_workflow(
        self,
        workflow_name: str,
        input: dict = {},
        key: str = None,
        options: ChildTriggerWorkflowOptions = None,
    ) -> WorkflowRunRef:
        worker_id = self.worker.id()
        # if (
        #     options is not None
        #     and "sticky" in options
        #     and options["sticky"] == True
        #     and not self.worker.has_workflow(workflow_name)
        # ):
        #     raise Exception(
        #         f"cannot run with sticky: workflow {workflow_name} is not registered on the worker"
        #     )

        trigger_options = self._prepare_workflow_options(key, options, worker_id)

        return await self.admin_client.aio.run_workflow(
            workflow_name, input, trigger_options
        )


class Context(BaseContext):
    spawn_index = -1

    worker: WorkerContext

    def __init__(
        self,
        action: Action,
        dispatcher_client: DispatcherClient,
        admin_client: AdminClient,
        event_client: EventClient,
        rest_client: RestApi,
        workflow_listener: PooledWorkflowRunListener,
        workflow_run_event_listener: RunEventListenerClient,
        worker: WorkerContext,
        namespace: str = "",
    ):
        self.worker = worker

        self.aio = ContextAioImpl(
            action,
            dispatcher_client,
            admin_client,
            event_client,
            rest_client,
            workflow_listener,
            workflow_run_event_listener,
            worker,
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
        self.rest_client = rest_client
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
        logger.debug("cancelling step...")
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
            try:
                line = json.dumps(line)
            except Exception:
                line = str(line)

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

    def additional_metadata(self):
        return self.action.additional_metadata

    def child_index(self):
        return self.action.child_workflow_index

    def child_key(self):
        return self.action.child_workflow_key

    def parent_workflow_run_id(self):
        return self.action.parent_workflow_run_id

    def fetch_run_failures(self):
        data = self.rest_client.workflow_run_get(self.action.workflow_run_id)
        other_job_runs = [
            run for run in data.job_runs if run.job_id != self.action.job_id
        ]
        # TODO: Parse Step Runs using a Pydantic Model rather than a hand crafted dictionary
        failed_step_runs = [
            {
                "step_id": step_run.step_id,
                "step_run_action_name": step_run.step.action,
                "error": step_run.error,
            }
            for job_run in other_job_runs
            if job_run.step_runs
            for step_run in job_run.step_runs
            if step_run.error
        ]

        return failed_step_runs
