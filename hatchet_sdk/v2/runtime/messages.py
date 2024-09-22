from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional

from google.protobuf.json_format import ParseDict

from hatchet_sdk.contracts.dispatcher_pb2 import (
    GROUP_KEY_EVENT_TYPE_COMPLETED,
    GROUP_KEY_EVENT_TYPE_FAILED,
    GROUP_KEY_EVENT_TYPE_STARTED,
    STEP_EVENT_TYPE_COMPLETED,
    STEP_EVENT_TYPE_FAILED,
    STEP_EVENT_TYPE_STARTED,
    ActionType,
    AssignedAction,
    GroupKeyActionEventType,
    StepActionEvent,
    StepActionEventType,
    SubscribeToWorkflowRunsRequest,
    WorkflowRunEvent,
    WorkflowRunEventType,
)
from hatchet_sdk.worker.action_listener_process import Action


class MessageKind(Enum):
    UNKNOWN = 0
    ACTION = 1
    STEP_EVENT = 2
    WORKFLOW_RUN_EVENT = 3
    SUBSCRIBE_TO_WORKFLOW_RUN = 4
    WORKER_ID = 5


@dataclass
class Message:
    """The runtime IPC message format. Note that it has to be trivially pickle-able."""

    _action: Optional[Dict] = None
    _step_event: Optional[Dict] = None
    _workflow_run_event: Optional[Dict] = None
    _subscribe_to_workflow_run: Optional[Dict] = None

    worker_id: Optional[str] = None

    @property
    def kind(self) -> MessageKind:
        if self._action is not None:
            return MessageKind.ACTION
        if self._step_event is not None:
            return MessageKind.STEP_EVENT
        if self._workflow_run_event is not None:
            return MessageKind.WORKFLOW_RUN_EVENT
        if self._subscribe_to_workflow_run is not None:
            return MessageKind.SUBSCRIBE_TO_WORKFLOW_RUN
        if self.worker_id:
            return MessageKind.WORKER_ID
        return MessageKind.UNKNOWN

    @property
    def action(self) -> AssignedAction:
        assert self._action is not None
        ret = AssignedAction()
        return ParseDict(self._action, ret)

    @property
    def step_event(self) -> StepActionEvent:
        assert self._step_event is not None
        ret = StepActionEvent()
        return ParseDict(self._step_event, ret)

    @property
    def workflow_run_event(self) -> WorkflowRunEvent:
        assert self._workflow_run_event is not None
        ret = WorkflowRunEvent()
        return ParseDict(self._workflow_run_event, ret)

    @property
    def subscribe_to_workflow_run(self) -> SubscribeToWorkflowRunsRequest:
        assert self._subscribe_to_workflow_run is not None
        ret = SubscribeToWorkflowRunsRequest()
        return ParseDict(self._subscribe_to_workflow_run, ret)
