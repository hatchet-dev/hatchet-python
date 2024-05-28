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
        print("executed step1")
        time.sleep(10)
        pass


workflow = MyWorkflow()
worker = hatchet.worker("test-worker", max_runs=1)
worker.register_workflow(workflow)

# workflow1 = hatchet.client.admin.put_workflow(
#     "workflow-copy-2",
#     MyWorkflow(),
#     overrides=CreateWorkflowVersionOpts(
#         cron_triggers=["* * * * *"],
#         cron_input=json.dumps({"test": "test"}),
#     ),
# )

# print(workflow1)

worker.start()
