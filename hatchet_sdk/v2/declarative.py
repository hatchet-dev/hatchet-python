from typing import Any, Callable, Generic, Type, TypeVar, cast

from pydantic import BaseModel, ConfigDict

from hatchet_sdk.v2.concurrency import ConcurrencyFunction
from hatchet_sdk.v2.hatchet import Context, Hatchet
from hatchet_sdk.workflow_run import WorkflowRunRef

T = TypeVar("T")
TWorkflowInput = TypeVar("TWorkflowInput", bound=BaseModel)


class DeclarativeWorkflowConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    input_validator: Type[BaseModel]
    name: str = ""
    on_events: list[str] | None = None
    on_crons: list[str] | None = None
    version: str = ""
    timeout: str = "60m"
    schedule_timeout: str = "5m"
    concurrency: ConcurrencyFunction | None = None
    default_priority: int | None = None


class DeclarativeWorkflow(Generic[TWorkflowInput]):
    def __init__(
        self, config: DeclarativeWorkflowConfig, hatchet: Hatchet | None = None
    ):
        self.config = config
        self.hatchet = hatchet

    def run_workflow(self, input: TWorkflowInput) -> WorkflowRunRef:
        return self.hatchet.admin.run_workflow(
            workflow_name=self.config.name, input=input.model_dump()
        )

    def declare(self) -> Callable[[Callable[[Context], Any]], Callable[[Context], Any]]:
        return self.hatchet.function(**self.config.model_dump())

    def workflow_input(self, ctx: Context) -> TWorkflowInput:
        return cast(TWorkflowInput, ctx.workflow_input())
