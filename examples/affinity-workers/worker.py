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
            "model": {
                "value": "fancy-ai-model-v2",
                "weight": 10
            },
            "memory": {
                "value": 256,
                "required": True,
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
        "model": "fancy-ai-model-v2",
        "memory": 512,
    },
)
worker.register_workflow(AffinityWorkflow())
worker.start()
