from typing import Any, Type

from pydantic import BaseModel


class WorkflowValidator(BaseModel):
    workflow_input: Type[BaseModel] | None = None
    step_output: Type[BaseModel] | None = None


AdditionalMetadata = dict[str, str]
