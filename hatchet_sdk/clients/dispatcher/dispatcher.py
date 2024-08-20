from google.protobuf.timestamp_pb2 import Timestamp

from hatchet_sdk.clients.dispatcher.action_listener import (
    Action,
    ActionListener,
    GetActionListenerRequest,
)
from hatchet_sdk.connection import new_conn
from hatchet_sdk.contracts.dispatcher_pb2 import (
    ActionEventResponse,
    GroupKeyActionEvent,
    GroupKeyActionEventType,
    OverridesData,
    RefreshTimeoutRequest,
    ReleaseSlotRequest,
    StepActionEvent,
    StepActionEventType,
    UpsertWorkerLabelsRequest,
    WorkerLabels,
    WorkerRegisterRequest,
    WorkerRegisterResponse,
)
from hatchet_sdk.contracts.dispatcher_pb2_grpc import DispatcherStub

from ...loader import ClientConfig
from ...metadata import get_metadata

DEFAULT_REGISTER_TIMEOUT = 30


def new_dispatcher(config: ClientConfig):
    return DispatcherClient(config=config)


class DispatcherClient:
    config: ClientConfig

    def __init__(self, config: ClientConfig):
        conn = new_conn(config)
        self.client = DispatcherStub(conn)

        aio_conn = new_conn(config, True)
        self.aio_client = DispatcherStub(aio_conn)
        self.token = config.token
        self.config = config

    async def get_action_listener(
        self, req: GetActionListenerRequest
    ) -> ActionListener:
        # Register the worker
        response: WorkerRegisterResponse = await self.aio_client.Register(
            WorkerRegisterRequest(
                workerName=req.worker_name,
                actions=req.actions,
                services=req.services,
                maxRuns=req.max_runs,
                labels=req.labels,
            ),
            timeout=DEFAULT_REGISTER_TIMEOUT,
            metadata=get_metadata(self.token),
        )

        return ActionListener(self.config, response.workerId)

    async def send_step_action_event(
        self, action: Action, event_type: StepActionEventType, payload: str
    ):
        eventTimestamp = Timestamp()
        eventTimestamp.GetCurrentTime()

        event = StepActionEvent(
            workerId=action.worker_id,
            jobId=action.job_id,
            jobRunId=action.job_run_id,
            stepId=action.step_id,
            stepRunId=action.step_run_id,
            actionId=action.action_id,
            eventTimestamp=eventTimestamp,
            eventType=event_type,
            eventPayload=payload,
        )

        return await self.aio_client.SendStepActionEvent(
            event,
            metadata=get_metadata(self.token),
        )

    async def send_group_key_action_event(
        self, action: Action, event_type: GroupKeyActionEventType, payload: str
    ):
        eventTimestamp = Timestamp()
        eventTimestamp.GetCurrentTime()

        event = GroupKeyActionEvent(
            workerId=action.worker_id,
            workflowRunId=action.workflow_run_id,
            getGroupKeyRunId=action.get_group_key_run_id,
            actionId=action.action_id,
            eventTimestamp=eventTimestamp,
            eventType=event_type,
            eventPayload=payload,
        )

        return await self.aio_client.SendGroupKeyActionEvent(
            event,
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

    def upsert_worker_labels(self, worker_id: str, labels: dict[str, str | int]):
        worker_labels = {}

        for key, value in labels.items():
            if isinstance(value, int):
                worker_labels[key] = WorkerLabels(intValue=value)
            else:
                worker_labels[key] = WorkerLabels(strValue=str(value))

        self.client.UpsertWorkerLabels(
            UpsertWorkerLabelsRequest(workerId=worker_id, labels=worker_labels),
            timeout=DEFAULT_REGISTER_TIMEOUT,
            metadata=get_metadata(self.token),
        )
