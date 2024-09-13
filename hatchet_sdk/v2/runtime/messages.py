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
)
from hatchet_sdk.worker.action_listener_process import Action


class MessageType(Enum):
    UNKNOWN = 0
    ACTION_RUN = 1
    ACTION_CANCEL = 2
    EVENT_STARTED = 3
    EVENT_FINISHED = 4


@dataclass
class Message:
    action: AssignedAction
    type: MessageType
    payload: Any = None
