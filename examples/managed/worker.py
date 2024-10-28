import time

from dotenv import load_dotenv

from hatchet_sdk import Context, Hatchet
from hatchet_sdk.clients.cloud_rest.models.managed_worker_region import (
    ManagedWorkerRegion,
)
from hatchet_sdk.compute.configs import Compute

load_dotenv()

hatchet = Hatchet()

# Default compute

default_compute = Compute(
    cpu_kind="shared", cpus=1, memory_mb=1024, region=ManagedWorkerRegion.EWR
)

blocked_compute = Compute(
    pool="blocked-pool",
    cpu_kind="shared",
    cpus=1,
    memory_mb=1024,
    region=ManagedWorkerRegion.EWR,
)

gpu_compute = Compute(
    cpu_kind="gpu", cpus=2, memory_mb=1024, region=ManagedWorkerRegion.EWR
)


@hatchet.workflow(on_events=["user:create"])
class ManagedWorkflow:
    @hatchet.step(timeout="11s", retries=3, compute=default_compute)
    def step1(self, context: Context):
        print("executed step1")
        time.sleep(10)
        # raise Exception("test")
        return {
            "step1": "step1",
        }

    @hatchet.step(timeout="11s", retries=3, compute=gpu_compute)
    def step2(self, context: Context):
        print("executed step2")
        time.sleep(10)
        # raise Exception("test")
        return {
            "step2": "step2",
        }

    @hatchet.step(timeout="11s", retries=3, compute=blocked_compute)
    def step3(self, context: Context):
        print("executed step3")

        return {
            "step3": "step3",
        }

    @hatchet.step(timeout="11s", retries=3, compute=default_compute)
    def step4(self, context: Context):
        print("executed step4")
        time.sleep(10)
        return {
            "step4": "step4",
        }


def main():
    workflow = MyWorkflow()
    worker = hatchet.worker("test-worker", max_runs=1)
    worker.register_workflow(workflow)
    worker.start()


if __name__ == "__main__":
    main()
