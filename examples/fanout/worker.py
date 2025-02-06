import asyncio
from typing import Any, cast

from hatchet_sdk import BaseWorkflow, ChildTriggerWorkflowOptions, Context, Hatchet

hatchet = Hatchet(debug=True)

parent_wf = hatchet.declare_workflow(on_events=["parent:create"])


class Parent(BaseWorkflow):
    config = parent_wf.config

    @hatchet.step(timeout="5m")
    async def spawn(self, context: Context) -> dict[str, Any]:
        print("spawning child")

        context.put_stream("spawning...")
        results = []

        n = cast(dict[str, Any], context.workflow_input).get("n", 100)

        for i in range(n):
            results.append(
                (
                    await context.aspawn_workflow(
                        "Child",
                        {"a": str(i)},
                        key=f"child{i}",
                        options=ChildTriggerWorkflowOptions(
                            additional_metadata={"hello": "earth"}
                        ),
                    )
                ).result()
            )

        result = await asyncio.gather(*results)
        print(f"results {result}")

        return {"results": result}


child_wf = hatchet.declare_workflow(on_events=["child:create"])


class Child(BaseWorkflow):
    config = child_wf.config

    @hatchet.step()
    def process(self, context: Context) -> dict[str, str]:
        a = cast(dict[str, Any], context.workflow_input)["a"]
        print(f"child process {a}")
        context.put_stream("child 1...")
        return {"status": "success " + a}

    @hatchet.step()
    def process2(self, context: Context) -> dict[str, str]:
        print("child process2")
        context.put_stream("child 2...")
        return {"status2": "success"}


def main() -> None:
    worker = hatchet.worker("fanout-worker", max_runs=40)
    worker.register_workflow(Parent())
    worker.register_workflow(Child())
    worker.start()


if __name__ == "__main__":
    main()
