from typing import Literal

from pydantic import BaseModel

from examples.v2.declarative_workflows.client import hatchet
from hatchet_sdk.v2.declarative import DeclarativeWorkflow, DeclarativeWorkflowConfig

Greeting = Literal["Hello", "Ciao", "Hej"]


class ExampleInput(BaseModel):
    greeting: Greeting


my_declarative_workflow = DeclarativeWorkflow[ExampleInput](
    config=DeclarativeWorkflowConfig(
        name="MyDeclarativeWorkflow",
        input_validator=ExampleInput,
    ),
    hatchet=hatchet,
)
