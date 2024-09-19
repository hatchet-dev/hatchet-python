import asyncio
import multiprocessing as mp

from loguru import logger

import hatchet_sdk.v2.hatchet as hatchet
import hatchet_sdk.v2.runtime.runner as runner
import hatchet_sdk.v2.runtime.worker as worker


class Runtime:
    def __init__(self, client: "hatchet.Hatchet", options: "worker.WorkerOptions"):
        logger.trace("init runtime")
        self.events = mp.Queue()
        self.actions = mp.Queue()
        self.worker = worker.Worker(
            client=client, inbound=self.events, outbound=self.actions, options=options
        )
        self.runner = runner.BaseRunnerLoop(
            client=client, inbound=self.actions, outbound=self.events
        )

    async def start(self):
        logger.trace("starting runtime")
        self.runner.start()
        await self.worker.start()
        self.runner.worker_id = self.worker.id
        logger.debug("runtime started")
        return self.worker.id

    async def shutdown(self):
        logger.trace("shutting down runtime")
        await self.worker.shutdown()
        self.actions.close()
        self.actions.join_thread()

        await self.runner.shutdown()
        self.events.close()
        self.events.join_thread()
        logger.debug("bye")
