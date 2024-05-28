import asyncio
import json
from collections.abc import AsyncIterator
from typing import AsyncGenerator

import grpc

from hatchet_sdk.connection import new_conn

from ..dispatcher_pb2 import SubscribeToWorkflowRunsRequest, WorkflowRunEvent
from ..dispatcher_pb2_grpc import DispatcherStub
from ..loader import ClientConfig
from ..metadata import get_metadata

DEFAULT_WORKFLOW_LISTENER_RETRY_INTERVAL = 5  # seconds
DEFAULT_WORKFLOW_LISTENER_RETRY_COUNT = 5


class PooledWorkflowRunListener:
    requests: asyncio.Queue[SubscribeToWorkflowRunsRequest] = asyncio.Queue()

    listener: AsyncGenerator[WorkflowRunEvent, None] = None

    events: dict[str, asyncio.Queue[WorkflowRunEvent]] = {}

    def __init__(self, token: str, config: ClientConfig):
        conn = new_conn(config, True)
        self.client = DispatcherStub(conn)
        self.stop_signal = False
        self.token = token
        self.config = config

    def abort(self):
        self.stop_signal = True
        self.requests.put_nowait(False)

    async def _init_producer(self):
        if not self.listener:
            self.listener = await self._retry_subscribe()

            async for workflow_event in self.listener:
                if workflow_event.workflowRunId in self.events:
                    self.events[workflow_event.workflowRunId].put_nowait(workflow_event)

    async def _request(self) -> AsyncIterator[SubscribeToWorkflowRunsRequest]:
        while True:
            request = await self.requests.get()

            if request is False or self.stop_signal:
                self.stop_signal = True
                break

            yield request
            self.requests.task_done()

    async def subscribe(self, workflow_run_id: str):
        self.events[workflow_run_id] = asyncio.Queue()

        asyncio.create_task(self._init_producer())

        await self.requests.put(
            SubscribeToWorkflowRunsRequest(
                workflowRunId=workflow_run_id,
            )
        )

        while True:
            event = await self.events[workflow_run_id].get()
            if event.workflowRunId == workflow_run_id:
                yield event
                break  # FIXME this should only break on terminal events... but we're not broadcasting event types

        del self.events[workflow_run_id]

    async def result(self, workflow_run_id: str):
        async for event in self.subscribe(workflow_run_id):
            errors = []

            if event.results:
                errors = [result.error for result in event.results if result.error]

            if errors:
                raise Exception(f"Child Errors: {errors}")

            results = {
                result.stepReadableId: json.loads(result.output)
                for result in event.results
                if result.output
            }

            return results

    async def _retry_subscribe(self):
        retries = 0

        while retries < DEFAULT_WORKFLOW_LISTENER_RETRY_COUNT:
            try:
                if retries > 0:
                    await asyncio.sleep(DEFAULT_WORKFLOW_LISTENER_RETRY_INTERVAL)

                listener = self.client.SubscribeToWorkflowRuns(
                    self._request(),
                    metadata=get_metadata(self.token),
                )

                return listener
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    retries = retries + 1
                else:
                    raise ValueError(f"gRPC error: {e}")
