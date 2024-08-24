#Python
import time

from dotenv import load_dotenv

from hatchet_sdk import Hatchet
from hatchet_sdk.context import Context

load_dotenv()

hatchet = Hatchet(debug=True)


@hatchet.workflow(on_events=["concurrency-test"])
class ConcurrencyDemoWorkflow:

    @hatchet.concurrency(max_runs=5)
    def concurrency(self, context) -> str:
        return "concurrency-key"

    @hatchet.step()
    def step1(self, context: Context):
        input = context.workflow_input()
        time.sleep(3)
        print("executed step1")
        return {"run": input["run"]}


def main():
    workflow = ConcurrencyDemoWorkflow()
    #START setting-concurrency-on-workers
    worker = hatchet.worker("concurrency-demo-worker", max_runs=10)
    #END setting-concurrency-on-workers
    worker.register_workflow(workflow)

    worker.start()


if __name__ == "__main__":
    main()
