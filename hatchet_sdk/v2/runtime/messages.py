from dataclasses import dataclass
from enum import Enum
from typing import Any

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
    StepActionEventType,
    WorkflowRunEventType,
    StepActionEvent,
)
from hatchet_sdk.worker.action_listener_process import Action
from google.protobuf.json_format import ParseDict
from typing import Optional, Dict


class MessageKind(Enum):
    UNKNOWN = 0
    ACTION = 1
    STEP_EVENT = 2


@dataclass
class Message:
    """The runtime IPC message format. Note that it has to be trivially pickle-able."""

    _action: Optional[Dict] = None
    _step_event: Optional[Dict] = None

    @property
    def kind(self) -> MessageKind:
        if self._action is not None:
            return MessageKind.ACTION
        if self._step_event is not None:
            return MessageKind.STEP_EVENT
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
