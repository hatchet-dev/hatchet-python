import asyncio
import json
import time
from collections.abc import AsyncIterator
from typing import AsyncGenerator

import grpc

from hatchet_sdk.clients.event_ts import Event_ts, read_with_interrupt
from hatchet_sdk.connection import new_conn

from ..dispatcher_pb2 import SubscribeToWorkflowRunsRequest, WorkflowRunEvent
from ..dispatcher_pb2_grpc import DispatcherStub
from ..loader import ClientConfig
from ..logger import logger
from ..metadata import get_metadata

DEFAULT_WORKFLOW_LISTENER_RETRY_INTERVAL = 3  # seconds
DEFAULT_WORKFLOW_LISTENER_RETRY_COUNT = 5
DEFAULT_WORKFLOW_LISTENER_INTERRUPT_INTERVAL = 1800  # 30 minutes


class _Subscription:
    def __init__(self, id: int, workflow_run_id: str):
        self.id = id
        self.workflow_run_id = workflow_run_id
        self.queue: asyncio.Queue[WorkflowRunEvent | None] = asyncio.Queue()

    async def __aiter__(self):
        return self

    async def __anext__(self) -> WorkflowRunEvent:
        return await self.queue.get()

    async def get(self) -> WorkflowRunEvent:
        event = await self.queue.get()

        if event is None:
            raise StopAsyncIteration

        return event

    async def put(self, item: WorkflowRunEvent):
        await self.queue.put(item)

    async def close(self):
        await self.queue.put(None)


class PooledWorkflowRunListener:
    # list of all active subscriptions, mapping from a subscription id to a workflow run id
    subscriptionsToWorkflows: dict[int, str] = {}

    # list of workflow run ids mapped to an array of subscription ids
    workflowsToSubscriptions: dict[str, list[int]] = {}

    subscription_counter: int = 0
    subscription_counter_lock: asyncio.Lock = asyncio.Lock()

    requests: asyncio.Queue[SubscribeToWorkflowRunsRequest] = asyncio.Queue()

    listener: AsyncGenerator[WorkflowRunEvent, None] = None

    # events have keys of the format workflow_run_id + subscription_id
    events: dict[int, _Subscription] = {}

    interrupter: asyncio.Task = None

    def __init__(self, config: ClientConfig):
        conn = new_conn(config, True)
        self.client = DispatcherStub(conn)
        self.stop_signal = False
        self.token = config.token
        self.config = config

    def abort(self):
        self.stop_signal = True
        self.requests.put_nowait(False)

    async def _interrupter(self):
        """
        _interrupter runs in a separate thread and interrupts the listener according to a configurable duration.
        """
        await asyncio.sleep(DEFAULT_WORKFLOW_LISTENER_INTERRUPT_INTERVAL)

        if self.interrupt is not None:
            self.interrupt.set()

    async def _init_producer(self):
        try:
            if not self.listener:
                while True:
                    try:
                        self.listener = await self._retry_subscribe()

                        logger.debug(f"Workflow run listener connected.")

                        # spawn an interrupter task
                        if self.interrupter is not None and not self.interrupter.done():
                            self.interrupter.cancel()

                        self.interrupter = asyncio.create_task(self._interrupter())

                        while True:
                            self.interrupt = Event_ts()
                            t = asyncio.create_task(
                                read_with_interrupt(self.listener, self.interrupt)
                            )
                            await self.interrupt.wait()

                            if not t.done():
                                # print a warning
                                logger.warning(
                                    "Interrupted read_with_interrupt task of workflow run listener"
                                )

                                t.cancel()
                                self.listener.cancel()
                                await asyncio.sleep(
                                    DEFAULT_WORKFLOW_LISTENER_RETRY_INTERVAL
                                )
                                break

                            workflow_event: WorkflowRunEvent = t.result()

                            # get a list of subscriptions for this workflow
                            subscriptions = self.workflowsToSubscriptions.get(
                                workflow_event.workflowRunId, []
                            )

                            for subscription_id in subscriptions:
                                await self.events[subscription_id].put(workflow_event)

                    except grpc.RpcError as e:
                        logger.error(f"grpc error in workflow run listener: {e}")
                        await asyncio.sleep(DEFAULT_WORKFLOW_LISTENER_RETRY_INTERVAL)
                        continue

        except Exception as e:
            logger.error(f"Error in workflow run listener: {e}")

            self.listener = None

            # close all subscriptions
            for subscription_id in self.events:
                await self.events[subscription_id].close()

            raise e

    async def _request(self) -> AsyncIterator[SubscribeToWorkflowRunsRequest]:
        # replay all existing subscriptions
        workflow_run_set = set(self.subscriptionsToWorkflows.values())

        for workflow_run_id in workflow_run_set:
            yield SubscribeToWorkflowRunsRequest(
                workflowRunId=workflow_run_id,
            )

        while True:
            request = await self.requests.get()

            if request is False or self.stop_signal:
                self.stop_signal = True
                break

            yield request
            self.requests.task_done()

    def cleanup_subscription(self, subscription_id: int, init_producer: asyncio.Task):
        workflow_run_id = self.subscriptionsToWorkflows[subscription_id]

        if workflow_run_id in self.workflowsToSubscriptions:
            self.workflowsToSubscriptions[workflow_run_id].remove(subscription_id)

        del self.subscriptionsToWorkflows[subscription_id]
        del self.events[subscription_id]

        if len(self.events) == 0:
            if self.interrupter is not None and not self.interrupter.done():
                self.interrupter.cancel()
            self.interrupter = None

            if not init_producer.done():
                init_producer.cancel()

    async def subscribe(self, workflow_run_id: str):
        init_producer: asyncio.Task = None
        try:
            # create a new subscription id, place a mutex on the counter
            await self.subscription_counter_lock.acquire()
            self.subscription_counter += 1
            subscription_id = self.subscription_counter
            self.subscription_counter_lock.release()

            self.subscriptionsToWorkflows[subscription_id] = workflow_run_id

            if workflow_run_id not in self.workflowsToSubscriptions:
                self.workflowsToSubscriptions[workflow_run_id] = [subscription_id]
            else:
                self.workflowsToSubscriptions[workflow_run_id].append(subscription_id)

            self.events[subscription_id] = _Subscription(
                subscription_id, workflow_run_id
            )

            init_producer = asyncio.create_task(self._init_producer())

            await self.requests.put(
                SubscribeToWorkflowRunsRequest(
                    workflowRunId=workflow_run_id,
                )
            )

            event = await self.events[subscription_id].get()

            return event
        except asyncio.CancelledError:
            raise
        finally:
            self.cleanup_subscription(subscription_id, init_producer)

    async def result(self, workflow_run_id: str):
        event = await self.subscribe(workflow_run_id)

        errors = []

        if event.results:
            errors = [result.error for result in event.results if result.error]

        if errors:
            raise Exception(f"Workflow Errors: {errors}")

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
