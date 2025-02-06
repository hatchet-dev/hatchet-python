import time
from typing import Any, cast

from hatchet_sdk import (
    BaseWorkflow,
    ConcurrencyExpression,
    ConcurrencyLimitStrategy,
    Context,
    Hatchet,
)

hatchet = Hatchet(debug=True)

wf = hatchet.declare_workflow(
    on_events=["concurrency-test"],
    concurrency=ConcurrencyExpression(
        expression="input.group",
        max_runs=5,
        limit_strategy=ConcurrencyLimitStrategy.CANCEL_IN_PROGRESS,
    ),
)


class ConcurrencyDemoWorkflow(BaseWorkflow):

    config = wf.config

    @hatchet.step()
    def step1(self, context: Context) -> dict[str, Any]:
        input = cast(dict[str, Any], context.workflow_input)
        time.sleep(3)
        print("executed step1")
        return {"run": input["run"]}


def main() -> None:
    worker = hatchet.worker("concurrency-demo-worker", max_runs=10)
    worker.register_workflow(ConcurrencyDemoWorkflow())

    worker.start()


if __name__ == "__main__":
    main()
