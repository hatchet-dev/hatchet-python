import json
import time

from dotenv import load_dotenv

from hatchet_sdk import Context, CreateWorkflowVersionOpts, Hatchet

load_dotenv()

hatchet = Hatchet(debug=True)


@hatchet.workflow(on_events=["user:create"])
class MyWorkflow:
    def __init__(self):
        self.my_value = "test"

    @hatchet.step()
    def step1(self, context: Context):
        test = context.playground("test", "test")
        test2 = context.playground("test2", 100)

        print(test)
        print(test2)
        
        time.sleep(10)

        print("executed step1")

        return {
            "result": "step1",
        }

    @hatchet.step(parents=["step1"], timeout="4s")
    def step2(self, context):
        print("started step2")
        context.sleep(1)
        print("finished step2")

if __name__ == "__main__":
    workflow = MyWorkflow()
    worker = hatchet.worker("test-worker", max_runs=4)
    worker.register_workflow(workflow)

    worker.start()
