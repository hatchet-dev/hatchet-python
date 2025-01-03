from typing import Literal

from pydantic import BaseModel

from examples.v2.declarative_workflows.client import hatchet
from hatchet_sdk.v2.declarative import DeclarativeWorkflow, DeclarativeWorkflowConfig

Greeting = Literal["Hello", "Ciao", "Hej"]
Language = Literal["English", "Swedish", "Italian"]


class ExampleInput(BaseModel):
    greeting: Greeting


greet_workflow = DeclarativeWorkflow[ExampleInput](
    config=DeclarativeWorkflowConfig(
        name="greet",
        input_validator=ExampleInput,
    ),
    hatchet=hatchet,
)

language_counter_workflow = DeclarativeWorkflow[ExampleInput](
    config=DeclarativeWorkflowConfig(
        name="language_counter",
        input_validator=ExampleInput,
    ),
    hatchet=hatchet,
)
