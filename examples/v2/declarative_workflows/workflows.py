from typing import Literal

from pydantic import BaseModel

from examples.v2.declarative_workflows.client import hatchet

Greeting = Literal["Hello", "Ciao", "Hej"]
Language = Literal["English", "Swedish", "Italian"]


class ExampleInput(BaseModel):
    greeting: Greeting


greet_workflow = hatchet.declare_workflow(input_validator=ExampleInput)
language_counter_workflow = hatchet.declare_workflow(
    input_validator=ExampleInput,
)
