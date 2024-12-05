from hatchet_sdk import Context, Hatchet

hatchet = Hatchet(debug=True)


# ❓ Backoff
@hatchet.workflow()
class BackoffWorkflow:
    # 👀 Backoff configuration
    @hatchet.step(
        retries=10,
        # 👀 Maximum number of seconds to wait between retries
        backoff_max_seconds=60,
        # 👀 Factor to increase the wait time between retries.
        # This sequence will be 2s, 4s, 8s, 16s, 32s, 60s... due to the maxSeconds limit
        backoff_factor=2.0,
    )
    def step1(self, context: Context):
        if context.retry_count() < 3:
            raise Exception("step1 failed")

        return {"status": "success"}


# ‼️


def main():
    workflow = BackoffWorkflow()
    worker = hatchet.worker("backoff-worker", max_runs=4)
    worker.register_workflow(workflow)

    worker.start()


if __name__ == "__main__":
    main()
