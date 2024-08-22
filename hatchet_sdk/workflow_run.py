import asyncio
from typing import Coroutine, Generic, TypeVar

from hatchet_sdk.clients.run_event_listener import (
    RunEventListener,
    RunEventListenerClient,
)
from hatchet_sdk.clients.workflow_listener import PooledWorkflowRunListener
from hatchet_sdk.utils.aio_utils import EventLoopThread, get_active_event_loop


class WorkflowRunRef:
    workflow_run_id: str

    def __init__(
        self,
        workflow_run_id: str,
        workflow_listener: PooledWorkflowRunListener,
        workflow_run_event_listener: RunEventListenerClient,
    ):
        self.workflow_run_id = workflow_run_id
        self.workflow_listener = workflow_listener
        self.workflow_run_event_listener = workflow_run_event_listener

    def __str__(self):
        return self.workflow_run_id

    def stream(self) -> RunEventListener:
        return self.workflow_run_event_listener.stream(self.workflow_run_id)

    def result(self) -> Coroutine:
        return self.workflow_listener.result(self.workflow_run_id)

    def sync_result(self) -> dict:
        loop = get_active_event_loop()
        if loop is None:
            with EventLoopThread() as loop:
                coro = self.workflow_listener.result(self.workflow_run_id)
                future = asyncio.run_coroutine_threadsafe(coro, loop)
                return future.result()
        else:
            coro = self.workflow_listener.result(self.workflow_run_id)
            future = asyncio.run_coroutine_threadsafe(coro, loop)
            return future.result()


T = TypeVar("T")


class RunRef(WorkflowRunRef, Generic[T]):
    async def result(self) -> T:
        res = await self.workflow_listener.result(self.workflow_run_id)

        if len(res) == 1:
            return list(res.values())[0]

        return res
