import asyncio
from threading import Thread
from typing import Generic, TypeVar

from hatchet_sdk.clients.run_event_listener import (
    RunEventListener,
    RunEventListenerClient,
)
from hatchet_sdk.clients.workflow_listener import PooledWorkflowRunListener


class EventLoopThread:
    """A class that manages an asyncio event loop running in a separate thread."""

    def __init__(self):
        """
        Initializes the EventLoopThread by creating an event loop
        and setting up a thread to run the loop.
        """
        self.loop = asyncio.new_event_loop()
        self.thread = Thread(target=self.run_loop_in_thread, args=(self.loop,))

    def __enter__(self) -> asyncio.AbstractEventLoop:
        """
        Starts the thread running the event loop when entering the context.

        Returns:
            asyncio.AbstractEventLoop: The event loop running in the separate thread.
        """
        self.thread.start()
        return self.loop

    def __exit__(self) -> None:
        """
        Stops the event loop and joins the thread when exiting the context.
        """
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join()

    def run_loop_in_thread(self, loop: asyncio.AbstractEventLoop) -> None:
        """
        Sets the event loop for the current thread and runs it forever.

        Args:
            loop (asyncio.AbstractEventLoop): The event loop to run.
        """
        asyncio.set_event_loop(loop)
        loop.run_forever()


def get_or_create_event_loop():
    try:
        return asyncio.get_event_loop()
    except RuntimeError as e:
        if str(e).startswith("There is no current event loop in thread"):
            return None
        else:
            raise e


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

    def result(self):
        loop = get_or_create_event_loop()
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
