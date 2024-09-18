import asyncio
from contextlib import suppress

from dotenv import load_dotenv

from hatchet_sdk import Context, Hatchet

load_dotenv()

hatchet = Hatchet(debug=True)


@hatchet.workflow()
class MyWorkflow:
    @hatchet.step()
    def step1(self, context: Context):
        pass


async def async_main():
    worker = None
    try:
        workflow = MyWorkflow()
        worker = hatchet.worker("test-worker", max_runs=1)
        worker.register_workflow(workflow)
        worker.start()
        while True:
            await asyncio.sleep(1)
    finally:
        await worker.exit_gracefully()


def main():
    with suppress(KeyboardInterrupt):
        asyncio.run(async_main())


if __name__ == "__main__":
    main()
