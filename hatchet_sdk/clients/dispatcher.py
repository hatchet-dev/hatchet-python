# relative imports
import asyncio
import json
import random
import threading
import time
from typing import Any, AsyncGenerator, List

import grpc
from grpc._cython import cygrpc

from hatchet_sdk.clients.event_ts import Event_ts, read_with_interrupt
from hatchet_sdk.connection import new_conn

from ..dispatcher_pb2 import (
    ActionEventResponse,
    ActionType,
    AssignedAction,
    GroupKeyActionEvent,
    HeartbeatRequest,
    OverridesData,
    RefreshTimeoutRequest,
    ReleaseSlotRequest,
    StepActionEvent,
    WorkerListenRequest,
    WorkerRegisterRequest,
    WorkerRegisterResponse,
    WorkerUnsubscribeRequest,
)
from ..dispatcher_pb2_grpc import DispatcherStub
from ..loader import ClientConfig
from ..logger import logger
from ..metadata import get_metadata
from .events import proto_timestamp_now


def new_dispatcher(config: ClientConfig):
    return DispatcherClientImpl(config=config)


class DispatcherClient:
    def get_action_listener(self, ctx, req):
        raise NotImplementedError

    async def send_step_action_event(self, ctx, in_):
        raise NotImplementedError


DEFAULT_ACTION_LISTENER_RETRY_INTERVAL = 5  # seconds
DEFAULT_ACTION_LISTENER_RETRY_COUNT = 15
DEFAULT_ACTION_TIMEOUT = 600  # seconds
DEFAULT_REGISTER_TIMEOUT = 30


class GetActionListenerRequest:
    def __init__(
        self,
        worker_name: str,
        services: List[str],
        actions: List[str],
        max_runs: int | None = None,
    ):
        self.worker_name = worker_name
        self.services = services
        self.actions = actions
        self.max_runs = max_runs


class Action:
    def __init__(
        self,
        worker_id: str,
        tenant_id: str,
        workflow_run_id: str,
        get_group_key_run_id: str,
        job_id: str,
        job_name: str,
        job_run_id: str,
        step_id: str,
        step_run_id: str,
        action_id: str,
        action_payload: str,
        action_type: ActionType,
        retry_count: int,
    ):
        self.worker_id = worker_id
        self.workflow_run_id = workflow_run_id
        self.get_group_key_run_id = get_group_key_run_id
        self.tenant_id = tenant_id
        self.job_id = job_id
        self.job_name = job_name
        self.job_run_id = job_run_id
        self.step_id = step_id
        self.step_run_id = step_run_id
        self.action_id = action_id
        self.action_payload = action_payload
        self.action_type = action_type
        self.retry_count = retry_count


class WorkerActionListener:
    def actions(self, ctx, err_ch):
        raise NotImplementedError

    def unregister(self):
        raise NotImplementedError


START_STEP_RUN = 0
CANCEL_STEP_RUN = 1
START_GET_GROUP_KEY = 2


async def exp_backoff_sleep(attempt: int, max_sleep_time: float = 5):
    base_time = 0.1  # starting sleep time in seconds (100 milliseconds)
    jitter = random.uniform(0, base_time)  # add random jitter
    sleep_time = min(base_time * (2**attempt) + jitter, max_sleep_time)
    await asyncio.sleep(sleep_time)


class ActionListenerImpl(WorkerActionListener):
    config: ClientConfig

    def __init__(
        self,
        config: ClientConfig,
        worker_id,
    ):
        self.config = config
        self.client = DispatcherStub(new_conn(config))
        self.aio_client = DispatcherStub(new_conn(config, True))
        self.token = config.token
        self.worker_id = worker_id
        self.retries = 0
        self.last_connection_attempt = 0
        self.last_heartbeat_succeeded = True  # start in a healthy state
        self.heartbeat_thread: threading.Thread = None
        self.run_heartbeat = True
        self.listen_strategy = "v2"
        self.stop_signal = False
        self.logger = logger

    def is_healthy(self):
        return self.last_heartbeat_succeeded

    def heartbeat(self):
        # send a heartbeat every 4 seconds
        while True:
            if not self.run_heartbeat:
                break

            try:
                self.client.Heartbeat(
                    HeartbeatRequest(
                        workerId=self.worker_id,
                        heartbeatAt=proto_timestamp_now(),
                    ),
                    timeout=5,
                    metadata=get_metadata(self.token),
                )

                self.last_heartbeat_succeeded = True
            except grpc.RpcError as e:
                # we don't reraise the error here, as we don't want to stop the heartbeat thread
                logger.error(f"Failed to send heartbeat: {e}")

                self.last_heartbeat_succeeded = False

                if self.interrupt is not None:
                    self.interrupt.set()

                if e.code() == grpc.StatusCode.UNIMPLEMENTED:
                    break

            time.sleep(4)

    def start_heartbeater(self):
        if self.heartbeat_thread is not None:
            return

        # create a new thread to send heartbeats
        heartbeat_thread = threading.Thread(target=self.heartbeat)
        heartbeat_thread.start()

        self.heartbeat_thread = heartbeat_thread

    def __aiter__(self):
        return self._generator()

    async def _generator(self) -> AsyncGenerator[Action, None]:
        listener: Any = None

        while True:
            if self.stop_signal:
                break

            if listener is not None:
                listener.cancel()

            listener = await self.get_listen_client()

            try:
                while True:
                    self.interrupt = Event_ts()
                    t = asyncio.create_task(
                        read_with_interrupt(listener, self.interrupt)
                    )
                    await self.interrupt.wait()

                    if not t.done():
                        # print a warning
                        logger.warning(
                            "Interrupted read_with_interrupt task of action listener"
                        )

                        t.cancel()
                        listener.cancel()
                        break

                    assigned_action = t.result()

                    if assigned_action is cygrpc.EOF:
                        self.retries = self.retries + 1
                        break

                    self.retries = 0
                    assigned_action: AssignedAction

                    # Process the received action
                    action_type = self.map_action_type(assigned_action.actionType)

                    if (
                        assigned_action.actionPayload is None
                        or assigned_action.actionPayload == ""
                    ):
                        action_payload = None
                    else:
                        action_payload = self.parse_action_payload(
                            assigned_action.actionPayload
                        )

                    action = Action(
                        tenant_id=assigned_action.tenantId,
                        worker_id=self.worker_id,
                        workflow_run_id=assigned_action.workflowRunId,
                        get_group_key_run_id=assigned_action.getGroupKeyRunId,
                        job_id=assigned_action.jobId,
                        job_name=assigned_action.jobName,
                        job_run_id=assigned_action.jobRunId,
                        step_id=assigned_action.stepId,
                        step_run_id=assigned_action.stepRunId,
                        action_id=assigned_action.actionId,
                        action_payload=action_payload,
                        action_type=action_type,
                        retry_count=assigned_action.retryCount,
                    )

                    yield action
            except grpc.RpcError as e:
                # Handle different types of errors
                if e.code() == grpc.StatusCode.CANCELLED:
                    # Context cancelled, unsubscribe and close
                    self.logger.debug("Context cancelled, closing listener")
                elif e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                    logger.info("Deadline exceeded, retrying subscription")
                elif (
                    self.listen_strategy == "v2"
                    and e.code() == grpc.StatusCode.UNIMPLEMENTED
                ):
                    # ListenV2 is not available, fallback to Listen
                    self.listen_strategy = "v1"
                    self.run_heartbeat = False
                    logger.info("ListenV2 not available, falling back to Listen")
                else:
                    # Unknown error, report and break
                    logger.error(f"Failed to receive message: {e}")

                    self.retries = self.retries + 1

    def parse_action_payload(self, payload: str):
        try:
            payload_data = json.loads(payload)
        except json.JSONDecodeError as e:
            raise ValueError(f"Error decoding payload: {e}")
        return payload_data

    def map_action_type(self, action_type):
        if action_type == ActionType.START_STEP_RUN:
            return START_STEP_RUN
        elif action_type == ActionType.CANCEL_STEP_RUN:
            return CANCEL_STEP_RUN
        elif action_type == ActionType.START_GET_GROUP_KEY:
            return START_GET_GROUP_KEY
        else:
            # self.logger.error(f"Unknown action type: {action_type}")
            return None

    async def get_listen_client(self):
        current_time = int(time.time())

        if (
            current_time - self.last_connection_attempt
            > DEFAULT_ACTION_LISTENER_RETRY_INTERVAL
        ):
            self.retries = 0

        if self.retries > DEFAULT_ACTION_LISTENER_RETRY_COUNT:
            raise Exception(
                f"Could not subscribe to the worker after {DEFAULT_ACTION_LISTENER_RETRY_COUNT} retries"
            )
        elif self.retries >= 1:
            # logger.info
            # if we are retrying, we wait for a bit. this should eventually be replaced with exp backoff + jitter
            await exp_backoff_sleep(
                self.retries, DEFAULT_ACTION_LISTENER_RETRY_INTERVAL
            )

            logger.info(
                f"Could not connect to Hatchet, retrying... {self.retries}/{DEFAULT_ACTION_LISTENER_RETRY_COUNT}"
            )

        self.aio_client = DispatcherStub(new_conn(self.config, True))

        if self.listen_strategy == "v2":
            listener = self.aio_client.ListenV2(
                WorkerListenRequest(workerId=self.worker_id),
                timeout=self.config.listener_v2_timeout,
                metadata=get_metadata(self.token),
            )

            self.start_heartbeater()
        else:
            # if ListenV2 is not available, fallback to Listen
            listener = self.aio_client.Listen(
                WorkerListenRequest(workerId=self.worker_id),
                timeout=DEFAULT_ACTION_TIMEOUT,
                metadata=get_metadata(self.token),
            )

        self.last_connection_attempt = current_time

        logger.info("Established listener.")
        return listener

    def unregister(self):
        self.run_heartbeat = False
        self.stop_signal = True

        try:
            self.client.Unsubscribe(
                WorkerUnsubscribeRequest(workerId=self.worker_id),
                timeout=5,
                metadata=get_metadata(self.token),
            )
        except grpc.RpcError as e:
            raise Exception(f"Failed to unsubscribe: {e}")


class DispatcherClientImpl(DispatcherClient):
    config: ClientConfig

    def __init__(self, config: ClientConfig):
        conn = new_conn(config)
        self.client = DispatcherStub(conn)

        aio_conn = new_conn(config, True)
        self.aio_client = DispatcherStub(aio_conn)
        self.token = config.token
        self.config = config
        # self.logger = logger
        # self.validator = validator

    async def get_action_listener(
        self, req: GetActionListenerRequest
    ) -> ActionListenerImpl:
        # Register the worker
        response: WorkerRegisterResponse = await self.aio_client.Register(
            WorkerRegisterRequest(
                workerName=req.worker_name,
                actions=req.actions,
                services=req.services,
                maxRuns=req.max_runs,
            ),
            timeout=DEFAULT_REGISTER_TIMEOUT,
            metadata=get_metadata(self.token),
        )

        return ActionListenerImpl(self.config, response.workerId)

    async def send_step_action_event(self, in_: StepActionEvent):
        await self.aio_client.SendStepActionEvent(
            in_,
            metadata=get_metadata(self.token),
        )

    async def send_group_key_action_event(self, in_: GroupKeyActionEvent):
        await self.aio_client.SendGroupKeyActionEvent(
            in_,
            metadata=get_metadata(self.token),
        )

    def put_overrides_data(self, data: OverridesData):
        response: ActionEventResponse = self.client.PutOverridesData(
            data,
            metadata=get_metadata(self.token),
        )

        return response

    def release_slot(self, step_run_id: str):
        self.client.ReleaseSlot(
            ReleaseSlotRequest(stepRunId=step_run_id),
            timeout=DEFAULT_REGISTER_TIMEOUT,
            metadata=get_metadata(self.token),
        )

    def refresh_timeout(self, step_run_id: str, increment_by: str):
        self.client.RefreshTimeout(
            RefreshTimeoutRequest(
                stepRunId=step_run_id,
                incrementTimeoutBy=increment_by,
            ),
            timeout=DEFAULT_REGISTER_TIMEOUT,
            metadata=get_metadata(self.token),
        )
