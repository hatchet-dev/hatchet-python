import json

from dotenv import load_dotenv

from hatchet_sdk import Context, Hatchet

load_dotenv()

hatchet = Hatchet(debug=True)


@hatchet.workflow(on_events=["user:create"])
class OnFailureWorkflow:
    @hatchet.step()
    def step1(self, context: Context):
        raise Exception("step1 failed")

    @hatchet.on_failure_step()
    def on_failure(self, context):
        print("executed on_failure")
        print(context)


workflow = OnFailureWorkflow()
worker = hatchet.worker("test-worker", max_runs=4)
worker.register_workflow(workflow)

worker.start()
