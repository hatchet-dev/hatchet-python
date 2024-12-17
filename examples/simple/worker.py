import time

from dotenv import load_dotenv

from hatchet_sdk import Context, Hatchet
from .test_logger import Foo

load_dotenv()

hatchet = Hatchet(debug=True)


@hatchet.workflow(on_events=["user:create"])
class MyWorkflow:
    @hatchet.step(timeout="11s", retries=3)
    async def step1(self, context: Context):
        foo = Foo()

        await foo.run("abc")

        return {
            "step1": "step1",
        }


def main():
    workflow = MyWorkflow()
    worker = hatchet.worker("test-worker", max_runs=1)
    worker.register_workflow(workflow)
    worker.start()


if __name__ == "__main__":
    main()
