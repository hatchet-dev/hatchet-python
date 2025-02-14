from typing import Any

from dotenv import load_dotenv

from hatchet_sdk import Context, Hatchet
from hatchet_sdk.workflow_run import WorkflowRunRef

load_dotenv()

hatchet = Hatchet(debug=True)


@hatchet.workflow(on_events=["parent:create"])
class SyncFanoutParent:
    @hatchet.step(timeout="5m")
    def spawn(self, context: Context) -> dict[str, Any]:
        print("spawning child")

        results: list[WorkflowRunRef] = []

        n = context.workflow_input().get("n", 5)

        for i in range(n):
            results.append(
                (
                    context.spawn_workflow(
                        "SyncFanoutChild",
                        {"a": str(i)},
                        key=f"child{i}",
                        options={"additional_metadata": {"hello": "earth"}},
                    )
                )
            )

        results = [
            r.sync_result()
            for r in results
        ]

        print(f"results {results}")


@hatchet.workflow(on_events=["child:create"])
class SyncFanoutChild:
    @hatchet.step()
    def process(self, context: Context) -> dict[str, str]:
        return {"status": "success " + context.workflow_input()["a"]}


def main() -> None:
    worker = hatchet.worker("sync-fanout-worker", max_runs=40)
    worker.register_workflow(SyncFanoutParent())
    worker.register_workflow(SyncFanoutChild())
    worker.start()


if __name__ == "__main__":
    main()
