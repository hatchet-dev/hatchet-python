import json
import time

from dotenv import load_dotenv

from hatchet_sdk import Context, Hatchet, StickyStrategy
from hatchet_sdk.clients.admin import ChildTriggerWorkflowOptions

load_dotenv()

hatchet = Hatchet(debug=True)


@hatchet.workflow(on_events=["affinity:run"])
class AffinityWorkflow:
    @hatchet.step()
    def step1a(self, context: Context):
        return {"worker": context.worker.id()}

    @hatchet.step(parents=["step1a"])
    async def step2(self, context: Context):

        ref = context.spawn_workflow(
            "MyWorkflow", {}, options={"sticky": True}
        )

        await ref.result()

        return {"worker": context.worker.id()}


worker = hatchet.worker("affinity-worker", max_runs=10, labels={"affinity": "true", "lobsters": 3})
worker.register_workflow(AffinityWorkflow())
worker.start()
