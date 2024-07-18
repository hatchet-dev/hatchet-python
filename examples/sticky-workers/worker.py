import json
import time

from dotenv import load_dotenv

from hatchet_sdk import Context, Hatchet, StickyStrategy

load_dotenv()

hatchet = Hatchet(debug=True)


@hatchet.workflow(
    on_events=["user:create"],
    sticky=StickyStrategy.HARD
)
class StickyWorkflow:
    @hatchet.step()
    def step1a(self, context: Context):
        return {"worker": "sticky-worker"}

    @hatchet.step()
    def step1b(self, context: Context):
        return {"worker": "sticky-worker"}
    
    @hatchet.step(parents=["step1a", "step1b"])
    def step2(self, context: Context):
        return {"worker": "sticky-worker"}

# @hatchet.workflow(on_events=["user:create"])
# class StickyChildWorkflow:

#     @hatchet.step(timeout="2s", retries=3)
#     def step1(self, context: Context):
#         print("executed step1")
#         time.sleep(10)
#         pass

worker = hatchet.worker("sticky-worker", max_runs=10)
worker.register_workflow(StickyWorkflow())
# worker.register_workflow(StickyChildWorkflow())
worker.start()
