from typing import TypedDict

from examples.v2.simple.client import hatchet, DurableContext, Context
from hatchet_sdk.v2.hatchet import ComputeConfig

from .compute_configs import cpu, gpu

class MyResultType(TypedDict):
    my_func: str

# introduce a new ComputeConfig class that defines the compute 
# dockerfile/image config
# do we have hatchet defaults here?


cpu = ComputeConfig(
            # STUBS
            cpu="2",
        )

gpu = ComputeConfig(
            # STUBS
            gpu="h100",
        )
        
@hatchet.function(managed_compute=gpu)
def my_func(context: Context) -> MyResultType:
    return MyResultType(my_func="testing123")


@hatchet.durable(managed_compute=cpu)
async def my_durable_func(context: DurableContext):
    result = await context.run(my_func, {"test": "test"}).result()

    context.log(result)

    return {"my_durable_func": result.get("my_func")}


'''
NOTE -- need to solve how this filters self.func based on 
the running compute config

TODO -- we also need to bind all functions and get set of COMPUTE to 
to bind deployments....

TODO if a hatchet function has compute but .worker is called raise an error

TODO what is the local dev story here?
'''


def entrypoint():
    worker = hatchet.managed_worker(
        "test-worker", 
        max_runs=5,
        default_compute=cpu
    )
    worker.start()


if __name__ == "__main__":
    entrypoint()