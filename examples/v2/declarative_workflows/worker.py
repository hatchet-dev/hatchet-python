from collections import Counter
from typing import Literal

from examples.v2.declarative_workflows.client import hatchet
from examples.v2.declarative_workflows.workflows import (
    Greeting,
    Language,
    greet_workflow,
    language_counter_workflow,
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


@greet_workflow.declare()
async def greet(ctx: Context) -> dict[Literal["message"], str]:
    workflow_input = greet_workflow.workflow_input(ctx)
    greeting = workflow_input.greeting

    await language_counter_workflow.spawn(
        context=ctx,
        input=language_counter_workflow.construct_spawn_workflow_input(
            input=workflow_input
        ),
    )

    return {"message": greeting + " " + complete_greeting(greeting)}


## Imagine this is a metric in a monitoring system
counter: Counter[Language] = Counter()


@language_counter_workflow.declare()
async def language_counter(
    ctx: Context,
) -> dict[Language, int]:
    greeting = language_counter_workflow.workflow_input(ctx).greeting

    match greeting:
        case "Hello":
            counter["English"] += 1
        case "Ciao":
            counter["Italian"] += 1
        case "Hej":
            counter["Swedish"] += 1

    return dict(counter)


if __name__ == "__main__":
    worker = hatchet.worker("my-worker")

    worker.start()
