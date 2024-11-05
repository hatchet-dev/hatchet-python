import asyncio
import time

from dotenv import load_dotenv

from hatchet_sdk import Context, Hatchet

load_dotenv()

hatchet = Hatchet(debug=True)


async def check_blocking_workers(context: Context):
    start_time = time.time()
    context.log("Starting blocking worker check")

    await asyncio.sleep(30)

    end_time = time.time()
    elapsed_time = end_time - start_time

    if 25 <= elapsed_time <= 35:
        context.log(f"Check completed in {elapsed_time:.2f} seconds")
    else:
        raise ValueError(
            f"Blockage detected: Task took {elapsed_time:.2f} seconds, expected 25-35"
            " seconds"
        )


@hatchet.workflow(on_crons=["*/5 * * * *"])
class CheckBlockingWorkersWorkflow:
    @hatchet.step(timeout="1m")
    async def iter_1(self, context: Context):
        await check_blocking_workers(context)

    @hatchet.step(parents=["iter_1"], timeout="1m")
    async def iter_2(self, context):
        await check_blocking_workers(context)

    @hatchet.step(parents=["iter_2"], timeout="1m")
    async def iter_3(self, context):
        await check_blocking_workers(context)

def main():
    workflow = CheckBlockingWorkersWorkflow()
    worker = hatchet.worker("test-worker", max_runs=1)
    worker.register_workflow(workflow)
    worker.start()

if __name__ == "__main__":
    main()
