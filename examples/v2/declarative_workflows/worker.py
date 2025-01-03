from typing import Literal

from examples.v2.declarative_workflows.client import hatchet
from examples.v2.declarative_workflows.workflows import (
    Greeting,
    my_declarative_workflow,
)
from hatchet_sdk import Context


def complete_greeting(greeting: Greeting) -> str:
    match greeting:
        case "Hello":
            return "world!"
        case "Ciao":
            return "mondo!"
        case "Hej":
            return "världen!"


@my_declarative_workflow.declare()
def my_step(ctx: Context) -> dict[Literal["message"], str]:
    greeting = my_declarative_workflow.workflow_input(ctx).greeting

    return {"message": greeting + " " + complete_greeting(greeting)}


if __name__ == "__main__":
    worker = hatchet.worker("my-worker")

    worker.start()
