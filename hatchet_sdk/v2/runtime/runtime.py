import hatchet_sdk.v2.hatchet as hatchet
import hatchet_sdk.v2.runtime.worker as worker
import hatchet_sdk.v2.runtime.runner as runner
import multiprocessing as mp
import asyncio


class Runtime:

    def __init__(self, client: "hatchet.Hatchet", options: "worker.WorkerOptions"):
        self.events = mp.Queue()
        self.actions = mp.Queue()
        self.worker = worker.Worker(
            client=client, inbound=self.events, outbound=self.actions, options=options
        )
        self.runner = runner.BaseRunnerLoop(
            client=client, inbound=self.actions, outbound=self.events
        )

    async def start(self):
        self.runner.start()
        await self.worker.start()
        self.runner.worker_id = self.worker.id
        return self.worker.id

    async def shutdown(self):
        await self.worker.shutdown()
        await self.runner.shutdown()
        self.actions.close()
        self.events.close()
