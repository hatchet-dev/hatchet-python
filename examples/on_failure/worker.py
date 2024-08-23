import json

from dotenv import load_dotenv

from hatchet_sdk import Context, Hatchet

load_dotenv()

hatchet = Hatchet(debug=True)


@hatchet.workflow(on_events=["user:create"])
class OnFailureWorkflow:
    @hatchet.step(timeout="1s")
    def step1(self, context: Context):
        raise Exception("step1 failed")

    @hatchet.on_failure_step()
    def on_failure(self, context: Context):
        failures = context.fetch_run_failures()
        print("executed on_failure")
        print(json.dumps(failures, indent=2))
        if len(failures) == 1 and "step1 failed" in failures[0]["error"]:
            return {"status": "success"}
        raise Exception("unexpected failure")


def main():
    workflow = OnFailureWorkflow()
    worker = hatchet.worker("on-failure-worker", max_runs=4)
    worker.register_workflow(workflow)

    worker.start()


if __name__ == "__main__":
    main()
