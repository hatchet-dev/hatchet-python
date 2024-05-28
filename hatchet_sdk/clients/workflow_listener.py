import asyncio
import json
from typing import AsyncGenerator
from collections.abc import AsyncIterator

import grpc

from hatchet_sdk.connection import new_conn

from ..dispatcher_pb2 import (
    SubscribeToWorkflowRunsRequest,
    WorkflowRunEvent
)
from ..dispatcher_pb2_grpc import DispatcherStub
from ..loader import ClientConfig
from ..metadata import get_metadata

DEFAULT_WORKFLOW_LISTENER_RETRY_INTERVAL = 5  # seconds
DEFAULT_WORKFLOW_LISTENER_RETRY_COUNT = 5

class PooledWorkflowRunListener:
    requests: asyncio.Queue[SubscribeToWorkflowRunsRequest] = asyncio.Queue()

    listener = None

    def __init__(self, token: str, config: ClientConfig):
        conn = new_conn(config, True)
        self.client = DispatcherStub(conn)
        self.stop_signal = False
        self.token = token
        self.config = config

    def abort(self):
        self.stop_signal = True
        self.requests.put_nowait(False)

    async def _generator(self) -> AsyncGenerator[WorkflowRunEvent, None]: #FIXME WorkflowRunEventType?
        if not self.listener:
            self.listener = await self._retry_subscribe()

        async for workflow_event in self.listener:
            yield workflow_event

    async def _request(self) -> AsyncIterator[SubscribeToWorkflowRunsRequest]:
        while True:
            request = await self.requests.get() # FIXME done in the signal

            if request is False or self.stop_signal:
                self.stop_signal = True
                break

            yield request
            self.requests.task_done()

    async def subscribe(self, workflow_run_id: str):
        # FIXME try? except?
        listener = self._generator()

        self.requests.put_nowait(
            SubscribeToWorkflowRunsRequest(
                workflowRunId=workflow_run_id,
            )
        )

        # TODO track subscription

        async for event in listener:
            yield event

        # TODO remove subscription

    async def result(self, workflow_run_id: str):
        async for event in self.subscribe(workflow_run_id):
            errors = []

            if event.results:
                errors = [result.error for result in event.results if result.error]

            if errors:
                raise Exception(f"Child Errors: {errors}")
            
            results = {result.stepReadableId: json.loads(result.output) for result in event.results if result.output}

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
