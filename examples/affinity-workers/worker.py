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
            "model": {"value": "fancy-ai-model-v2", "weight": 10},
            "memory": {
                "value": 256,
                "required": True,
                "comparator": WorkerLabelComparator.GREATER_THAN,
            },
        },
    )
    async def step(self, context: Context):
        if context.worker.get_labels().get("model") != "fancy-ai-model-v2":
            context.worker.upsert_labels({"model": "unset"})
            # DO WORK TO EVICT OLD MODEL / LOAD NEW MODEL
            context.worker.upsert_labels({"model": "fancy-ai-model-v2"})

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
