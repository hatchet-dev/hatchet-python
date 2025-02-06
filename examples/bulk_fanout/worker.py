import asyncio
from typing import Any, cast

from hatchet_sdk import BaseWorkflow, Context, Hatchet
from hatchet_sdk.clients.admin import ChildTriggerWorkflowOptions, ChildWorkflowRunDict

hatchet = Hatchet(debug=True)

bulk_parent_wf = hatchet.declare_workflow(on_events=["parent:create"])


class BulkParent(BaseWorkflow):
    config = bulk_parent_wf.config

    @hatchet.step(timeout="5m")
    async def spawn(self, context: Context) -> dict[str, list[Any]]:
        print("spawning child")

        context.put_stream("spawning...")
        results = []

        n = cast(dict[str, Any], context.workflow_input).get("n", 100)

        child_workflow_runs = [
            ChildWorkflowRunDict(
                workflow_name="BulkChild",
                input={"a": str(i)},
                key=f"child{i}",
                options=ChildTriggerWorkflowOptions(
                    additional_metadata={"hello": "earth"}
                ),
            )
            for i in range(n)
        ]

        if len(child_workflow_runs) == 0:
            return {}

        spawn_results = await context.aspawn_workflows(child_workflow_runs)

        results = await asyncio.gather(
            *[workflowRunRef.result() for workflowRunRef in spawn_results],
            return_exceptions=True,
        )

        print("finished spawning children")

        for result in results:
            if isinstance(result, Exception):
                print(f"An error occurred: {result}")
            else:
                print(result)

        return {"results": results}


bulk_child_wf = hatchet.declare_workflow(on_events=["child:create"])


class BulkChild(BaseWorkflow):
    config = bulk_child_wf.config

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
    worker.register_workflow(BulkParent())
    worker.register_workflow(BulkChild())
    worker.start()


if __name__ == "__main__":
    main()
