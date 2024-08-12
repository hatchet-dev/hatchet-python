from examples.v2.simple.client import hatchet
from examples.v2.simple.functions import my_func, my_durable_func
from hatchet_sdk.v2.hatchet import ComputeConfig
from .compute_configs import cpu

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