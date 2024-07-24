import time

from dotenv import load_dotenv

from hatchet_sdk import Context, Hatchet

load_dotenv()

hatchet = Hatchet(debug=True)


@hatchet.workflow(on_events=["overrides:create"], schedule_timeout="10m")
class OverridesWorkflow:
    def __init__(self):
        self.my_value = "test"

    @hatchet.step(timeout="5s")
    def step1(self, context: Context):
        print(
            "starting step1",
            time.strftime("%H:%M:%S", time.localtime()),
            context.workflow_input(),
        )
        overrideValue = context.playground("prompt", "You are an AI assistant...")
        time.sleep(3)
        # pretty-print time
        print("executed step1", time.strftime("%H:%M:%S", time.localtime()))
        return {
            "step1": overrideValue,
        }

    @hatchet.step()
    def step2(self, context: Context):
        print(
            "starting step2",
            time.strftime("%H:%M:%S", time.localtime()),
            context.workflow_input(),
        )
        time.sleep(5)
        print("executed step2", time.strftime("%H:%M:%S", time.localtime()))
        return {
            "step2": "step2",
        }

    @hatchet.step(parents=["step1", "step2"])
    def step3(self, context: Context):
        print(
            "executed step3",
            time.strftime("%H:%M:%S", time.localtime()),
            context.workflow_input(),
            context.step_output("step1"),
            context.step_output("step2"),
        )
        return {
            "step3": "step3",
        }

    @hatchet.step(parents=["step1", "step3"])
    def step4(self, context: Context):
        print(
            "executed step4",
            time.strftime("%H:%M:%S", time.localtime()),
            context.workflow_input(),
            context.step_output("step1"),
            context.step_output("step3"),
        )
        return {
            "step4": "step4",
        }


workflow = OverridesWorkflow()
worker = hatchet.worker("overrides-worker")
worker.register_workflow(workflow)

worker.start()
