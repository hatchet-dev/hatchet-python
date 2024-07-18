import json
import time

from dotenv import load_dotenv

from hatchet_sdk import Context, Hatchet, WorkerLabelComparator

load_dotenv()

hatchet = Hatchet(debug=True)


@hatchet.workflow(on_events=["affinity:run"])
class AffinityWorkflow:
    @hatchet.step(
        desired_worker_labels={
            "affinity": {
                "value": "true",
                "required": True,
                "weight": 1,
                "comparator": WorkerLabelComparator.GREATER_THAN,
            },
            "lobsters": {
                "value": 2,
                "weight": 1,
                "comparator": WorkerLabelComparator.GREATER_THAN,
            }
        },
    )
    async def step(self, context: Context):
        return {"worker": context.worker.id()}


worker = hatchet.worker(
    "affinity-worker",
    max_runs=10,
    labels={
        "affinity": "true",
        "lobsters": 3,
    },
)
worker.register_workflow(AffinityWorkflow())
worker.start()
