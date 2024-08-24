#Python
import time

from dotenv import load_dotenv

from hatchet_sdk import Context, Hatchet

load_dotenv()

hatchet = Hatchet(debug=True)

#START scheduling-timeouts
@hatchet.workflow(on_events=["timeout:create"])
class TimeoutWorkflow:
#END scheduling-timeouts
#START step-timeouts
    @hatchet.step(timeout="4s")
    def step1(self, context: Context):
        time.sleep(5)
        return {"status": "success"}
#END step-timeouts
#START refreshing-timeouts
@hatchet.workflow(on_events=["refresh:create"])
class RefreshTimeoutWorkflow:

    @hatchet.step(timeout="4s")
    def step1(self, context: Context):

        context.refresh_timeout("10s")
        time.sleep(5)

        return {"status": "success"}
#END refreshing-timeouts

def main():
    worker = hatchet.worker("timeout-worker", max_runs=4)
    worker.register_workflow(TimeoutWorkflow())
    worker.register_workflow(RefreshTimeoutWorkflow())

    worker.start()


if __name__ == "__main__":
    main()
