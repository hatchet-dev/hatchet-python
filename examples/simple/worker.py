import json
import time

from dotenv import load_dotenv

from hatchet_sdk import Context, CreateWorkflowVersionOpts, Hatchet

load_dotenv()

hatchet = Hatchet(debug=True)


@hatchet.workflow(on_events=["user:create"])
class MyWorkflow:
    @hatchet.step(timeout="9s", retries=3)
    def step1(self, context: Context):
        print("executed step1")
        time.sleep(2)
        raise Exception("test")
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
