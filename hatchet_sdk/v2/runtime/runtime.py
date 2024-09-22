import asyncio
import multiprocessing as mp
import os
import queue
import sys
import threading
from concurrent.futures import ProcessPoolExecutor
from contextlib import suppress

from loguru import logger

import hatchet_sdk.loader as loader
import hatchet_sdk.v2.hatchet as hatchet
import hatchet_sdk.v2.runtime.future as future
import hatchet_sdk.v2.runtime.messages as messages
import hatchet_sdk.v2.runtime.runner as runner
import hatchet_sdk.v2.runtime.utils as utils
import hatchet_sdk.v2.runtime.worker as worker
from hatchet_sdk.contracts.dispatcher_pb2 import (
    SubscribeToWorkflowRunsRequest,
    WorkflowRunEvent,
)


class Runtime:
    def __init__(self, client: "hatchet.Hatchet", options: "worker.WorkerOptions"):
        logger.trace("init runtime")

        self.client = client
        self.process_pool = ProcessPoolExecutor()

        self.to_worker: mp.Queue["messages.Message"] = mp.Queue()
        self.from_worker: mp.Queue["messages.Message"] = mp.Queue()

        self.to_runner = queue.Queue()
        self.to_wfr_futures = queue.Queue()

        self.worker = worker.WorkerProcess(
            config=client.config,
            inbound=self.to_worker,
            outbound=self.from_worker,
            options=options,
        )
        self.worker_id = None

        self.runner = runner.RunnerLoop(
            client=client, inbound=self.to_runner, outbound=self.to_worker
        )
        self.wfr_futures = future.WorkflowRunFutures(
            # pool=client.executor,
            broker=future.RequestResponseBroker(
                inbound=self.to_wfr_futures,
                outbound=self.to_worker,
                req_key=lambda msg: msg.subscribe_to_workflow_run.workflowRunId,
                resp_key=lambda msg: msg.workflow_run_event.workflowRunId,
                executor=client.executor,
            ),
        )

        self.loop_task = None

    async def loop(self):
        async for msg in utils.QueueAgen(self.from_worker):
            match msg.kind:
                case messages.MessageKind.ACTION:
                    await asyncio.to_thread(self.to_runner.put, msg)
                case messages.MessageKind.WORKFLOW_RUN_EVENT:
                    await asyncio.to_thread(self.to_wfr_futures.put, msg)
                case messages.MessageKind.WORKER_ID:
                    self.runner.worker_id = msg.worker_id
                    self.worker_id = msg.worker_id
                case _:
                    raise NotImplementedError

    async def start(self):
        logger.trace("starting runtime on {}", threading.get_ident())
        self.runner.start()
        self.wfr_futures.start()
        self.worker.start()

        self.client.executor.submit(asyncio.run, self.loop())

        while self.worker_id is None:
            await asyncio.sleep(1)
        logger.debug("runtime started")
        return self.worker_id

    async def shutdown(self):
        logger.trace("shutting down runtime")

        self.worker.shutdown()
        # await self.worker.shutdown()
        self.from_worker.close()
        self.from_worker.join_thread()

        await self.runner.shutdown()
        self.to_worker.close()
        self.to_worker.join_thread()

        await self.wfr_futures.shutdown()

        self.loop_task.cancel()
        with suppress(asyncio.CancelledError):
            await self.loop_task

        logger.debug("bye")
