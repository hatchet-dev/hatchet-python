from typing import cast

from dotenv import load_dotenv
from pydantic import BaseModel

from hatchet_sdk import Context, Hatchet

load_dotenv()

hatchet = Hatchet(debug=True)


class ParentInput(BaseModel):
    x: str


def pretty_print(tag: str, model: BaseModel) -> None:
    print()
    print(tag, model, "\nType:", type(model))  ## This is an instance of `ChildInput`
    print()


@hatchet.workflow(input_validator=ParentInput)
class Parent:
    @hatchet.step(timeout="5m")
    async def spawn(self, context: Context):
        ## Cast your `workflow_input` to the type of your `input_validator`
        input = cast(ParentInput, context.workflow_input())

        pretty_print("Workflow Input:", input)  ## This is a `ParentInput`

        child = await context.aio.spawn_workflow(
            "Child",
            {"a": 1, "b": "10"},
        )

        return await child.result()


class ChildInput(BaseModel):
    a: int
    b: int


class StepResponse(BaseModel):
    status: str


@hatchet.workflow(input_validator=ChildInput)
class Child:
    @hatchet.step()
    def process(self, context: Context) -> StepResponse:
        input = cast(ChildInput, context.workflow_input())

        pretty_print("Workflow Input:", input)  ## This is a `ChildInput`

        return StepResponse(status="success")

    @hatchet.step(parents=["process"])
    def process2(self, context: Context) -> StepResponse:
        process_output = cast(StepResponse, context.step_output("process"))

        pretty_print("Process Output:", process_output)  ## This is a `StepResponse`

        return {"status": "step 2 - success"}

    @hatchet.step(parents=["process2"])
    def process3(self, context: Context) -> StepResponse:
        process_2_output = cast(StepResponse, context.step_output("process2"))

        pretty_print("Process 2 Output:", process_2_output)  ## This is a `StepResponse`

        return StepResponse(status="step 3 - success")


def main():
    worker = hatchet.worker("pydantic-worker")
    worker.register_workflow(Parent())
    worker.register_workflow(Child())
    worker.start()


if __name__ == "__main__":
    main()
