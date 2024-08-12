from examples.v2.simple.client import hatchet
from examples.v2.simple.functions import my_durable_func, my_func

def main():
    worker = hatchet.worker("test-worker", max_runs=5)

    # hatchet.admin.run(my_durable_func, {"test": "test"})

    worker.start()


if __name__ == "__main__":
    main()