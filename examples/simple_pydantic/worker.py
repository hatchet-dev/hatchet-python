import time

from dotenv import load_dotenv
from pydantic import BaseModel

from hatchet_sdk import Context, Hatchet

load_dotenv()

hatchet = Hatchet(debug=True)


class WorkflowPayload(BaseModel):
    """Example Pydantic model."""

    step1: str = "step1"


@hatchet.workflow(on_events=["user:create"])
class MyWorkflow:
    @hatchet.step(timeout="15s", retries=3)
    def step1(self, context: Context):
        print("executed step1")
        time.sleep(5)
        return WorkflowPayload(step1="step1")


def main():
    workflow = MyWorkflow()
    worker = hatchet.worker("test-worker-pydantic", max_runs=1)
    worker.register_workflow(workflow)
    worker.start()


if __name__ == "__main__":
    main()
