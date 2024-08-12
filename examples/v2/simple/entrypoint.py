from examples.v2.simple.client import hc


def main():
    worker = hc.worker("test-worker", max_runs=5)

    # hatchet.admin.run(my_durable_func, {"test": "test"})

    worker.start()


if __name__ == "__main__":
    main()