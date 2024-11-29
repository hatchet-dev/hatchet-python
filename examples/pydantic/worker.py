from typing import cast

from dotenv import load_dotenv
from pydantic import BaseModel

from hatchet_sdk import Context, Hatchet

load_dotenv()

hatchet = Hatchet(debug=False)


@hatchet.workflow(on_events=["parent:create"])
class Parent:
    @hatchet.step(timeout="5m")
    async def spawn(self, context: Context):
        child = await context.aio.spawn_workflow(
            "Child",
            {"a": 1, "b": "10"},
            options={"additional_metadata": {}},
        )

        return await child.result()


class ChildInput(BaseModel):
    a: int
    b: int


class StepResponse(BaseModel):
    status: str


def pretty_print(model: BaseModel, tag: str) -> None:
    print()
    print(tag, model, "\nType:", type(model))  ## This is an instance of `ChildInput`
    print()


@hatchet.workflow(on_events=["child:create"])
class Child:
    @hatchet.step()
    def process(self, context: Context) -> StepResponse:
        input = context.workflow_input(validator=ChildInput)

        pretty_print(input, "Workflow Input:")  ## This is an instance of `ChildInput`

        return StepResponse(status="success")

    @hatchet.step(parents=["process"])
    def process2(self, context: Context) -> StepResponse:
        process_output = cast(StepResponse, context.step_output("process"))

        pretty_print(
            process_output, "Process Output:"
        )  ## This is an instance of `StepResponse`

        return {"status": "step 2 - success"}

    @hatchet.step(parents=["process2"])
    def process3(self, context: Context) -> StepResponse:
        process_2_output = cast(StepResponse, context.step_output("process2"))

        pretty_print(
            process_2_output, "Process 2 Output:"
        )  ## This is an instance of `StepResponse`

        return StepResponse(status="step 3 - success")


def main():
    worker = hatchet.worker("pydantic-worker")
    worker.register_workflow(Parent())
    worker.register_workflow(Child())
    worker.start()


if __name__ == "__main__":
    main()
