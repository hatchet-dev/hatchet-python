from typing import Any

from dotenv import load_dotenv

from hatchet_sdk import Context, Hatchet

load_dotenv()

hatchet = Hatchet(debug=True)


@hatchet.workflow(on_events=["parent:create"])
class SyncFanoutParent:
    @hatchet.step(timeout="5m")
    def spawn(self, context: Context) -> dict[str, Any]:
        print("spawning child")

        context.put_stream("spawning...")
        results = []

        n = context.workflow_input().get("n", 100)

        for i in range(n):
            results.append(
                (
                    context.spawn_workflow(
                        "Child",
                        {"a": str(i)},
                        key=f"child{i}",
                        options={"additional_metadata": {"hello": "earth"}},
                    )
                )
            )


@hatchet.workflow(on_events=["child:create"])
class SyncFanoutChild:
    @hatchet.step()
    def process(self, context: Context) -> dict[str, str]:
        a = context.workflow_input()["a"]
        print(f"child process {a}")
        context.put_stream("child 1...")
        return {"status": "success " + a}

    @hatchet.step()
    def process2(self, context: Context) -> dict[str, str]:
        print("child process2")
        context.put_stream("child 2...")
        return {"status2": "success"}


def main() -> None:
    worker = hatchet.worker("sync-fanout-worker", max_runs=40)
    worker.register_workflow(SyncFanoutParent())
    worker.register_workflow(SyncFanoutChild())
    worker.start()


if __name__ == "__main__":
    main()
