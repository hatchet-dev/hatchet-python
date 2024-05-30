import asyncio
import inspect
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from multiprocessing import Event

from aiostream.stream import merge

from hatchet_sdk.clients.events import EventClientImpl
from hatchet_sdk.clients.rest.models.workflow_run_status import WorkflowRunStatus
from hatchet_sdk.clients.workflow_listener import PooledWorkflowRunListener
from hatchet_sdk.dispatcher_pb2_grpc import DispatcherStub
from hatchet_sdk.workflows_pb2_grpc import WorkflowServiceStub

from .client import ClientImpl
from .clients.admin import AdminClientImpl, ScheduleTriggerWorkflowOptions
from .clients.dispatcher import Action, DispatcherClientImpl
from .clients.listener import HatchetListener, StepRunEvent, WorkflowRunEventType
from .dispatcher_pb2 import OverridesData
from .logger import logger

DEFAULT_WORKFLOW_POLLING_INTERVAL = 5  # Seconds


def get_caller_file_path():
    caller_frame = inspect.stack()[2]

    return caller_frame.filename


class WorkflowRunRef:
    workflow_run_id: str
    client: ClientImpl

    def __init__(
        self,
        workflow_run_id: str,
        workflow_listener: PooledWorkflowRunListener,
    ):
        self.workflow_run_id = workflow_run_id
        self.workflow_listener = workflow_listener

    def stream(self):
        return self.workflow_listener.subscribe(self.workflow_run_id)

    async def result(self):
        return await self.workflow_listener.result(self.workflow_run_id)


class Context:
    spawn_index = -1

    def __init__(
        self,
        action: Action,
        dispatcher_client: DispatcherClientImpl,
        admin_client: AdminClientImpl,
        event_client: EventClientImpl,
        workflow_listener: PooledWorkflowRunListener,
    ):
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
        self.stepRunId = action.step_run_id
        self.exit_flag = False
        self.dispatcher_client = dispatcher_client
        self.admin_client = admin_client
        self.event_client = event_client
        self.workflow_listener = workflow_listener

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

    async def spawn_workflow(
        self, workflow_name: str, input: dict = {}, key: str = None
    ):
        workflow_run_id = self.action.workflow_run_id
        step_run_id = self.action.step_run_id

        options: ScheduleTriggerWorkflowOptions = {
            "parent_id": workflow_run_id,
            "parent_step_run_id": step_run_id,
            "child_key": key,
            "child_index": self.spawn_index,
        }

        self.spawn_index += 1

        child_workflow_run_id = await self.admin_client.run_workflow(
            workflow_name, input, options
        )

        return WorkflowRunRef(child_workflow_run_id, self.workflow_listener)

    def _log(self, line: str):
        try:
            self.event_client.log(message=line, step_run_id=self.stepRunId)
        except Exception as e:
            logger.error(f"Error logging: {e}")

    def log(self, line: str):
        if self.stepRunId == "":
            return

        self.logger_thread_pool.submit(self._log, line)

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
