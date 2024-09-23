import asyncio
import multiprocessing as mp
import queue
import threading
from concurrent.futures import CancelledError
from contextlib import suppress

from loguru import logger

import hatchet_sdk.v2.hatchet as hatchet
import hatchet_sdk.v2.runtime.future as future
import hatchet_sdk.v2.runtime.messages as messages
import hatchet_sdk.v2.runtime.runner as runner
import hatchet_sdk.v2.runtime.utils as utils
import hatchet_sdk.v2.runtime.worker as worker


class Runtime:
    """The Hatchet runtime.

    The runtime is managine the runner on the main process, the run event listener on the main process,
    and the worker on the sidecar process, together with the queues among them. A Hatchet client should
    only contain one Runtime object. The behavior will be undefined if there are multiple Runtime per
    Hatchet client.
    """

    # TODO: rename WorkerOptions to RuntimeOptions.
    def __init__(self, *, client: "hatchet.Hatchet", options: "worker.WorkerOptions"):
        logger.trace("init runtime")

        self._client = client
        self._executor = client.executor

        # the main queues between the sidecar process and the main process
        self._to_worker: mp.Queue["messages.Message"] = mp.Queue()
        self._from_worker: mp.Queue["messages.Message"] = mp.Queue()

        # the queue to the runner on the main process
        self._to_runner = queue.Queue()

        # the queue to the workflow run event listener on the main process
        self._to_wfr_futures = queue.Queue()

        # the worker on the sidecar process
        self._worker = worker.WorkerProcess(
            config=client.config,
            inbound=self._to_worker,
            outbound=self._from_worker,
            options=options,
        )
        self.worker_id = None

        # the runner on the main process
        self._runner = runner.RunnerLoop(
            reg=client.registry,
            inbound=self._to_runner,
            outbound=self._to_worker,
        )

        # the workflow run event listener on the main process
        self._wfr_futures = future.WorkflowRunFutures(
            executor=self._executor,
            broker=future.RequestResponseBroker(
                inbound=self._to_wfr_futures,
                outbound=self._to_worker,
                req_key=lambda msg: msg.subscribe_to_workflow_run.workflowRunId,
                resp_key=lambda msg: msg.workflow_run_event.workflowRunId,
                executor=self._executor,
            ),
        )

        # the shutdown signal
        self._shutdown = threading.Event()
        self._loop_task = None

    async def _loop(self):
        async for msg in utils.QueueAgen(self._from_worker):
            match msg.kind:
                case messages.MessageKind.ACTION:
                    await asyncio.to_thread(self._to_runner.put, msg)
                case messages.MessageKind.WORKFLOW_RUN_EVENT:
                    await asyncio.to_thread(self._to_wfr_futures.put, msg)
                case messages.MessageKind.WORKER_ID:
                    self._runner.worker_id = msg.worker_id
                    self.worker_id = msg.worker_id
                case _:
                    raise NotImplementedError
            if self._shutdown.is_set():
                break

        logger.trace("bye: runtime")

    async def start(self):
        logger.debug("starting runtime")

        # NOTE: the order matters, we should start things in topological order
        self._runner.start()
        self._wfr_futures.start()

        # schedule the runtime on a separate thread
        self._loop_task = self._executor.submit(asyncio.run, self._loop())

        self._worker.start()
        while self.worker_id is None:
            await asyncio.sleep(1)

        logger.debug("runtime started")
        return self.worker_id

    async def shutdown(self):
        logger.trace("shutting down runtime")

        # NOTE: the order matters, we should shut things down in topological order
        self._worker.shutdown()
        self._from_worker.close()
        self._from_worker.join_thread()

        await self._runner.shutdown()
        self._to_worker.close()
        self._to_worker.join_thread()

        await self._wfr_futures.shutdown()

        self._shutdown.set()

        if self._loop_task is not None:
            with suppress(CancelledError):
                self._loop_task.result(timeout=10)

        logger.debug("bye: runtime")
