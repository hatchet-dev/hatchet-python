from typing import Any, Callable, Generic, Type, TypeVar, cast

from pydantic import BaseModel, ConfigDict

from hatchet_sdk.clients.admin import ChildTriggerWorkflowOptions
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


class SpawnWorkflowInput(BaseModel):
    workflow_name: str
    input: BaseModel
    key: str | None = None
    options: ChildTriggerWorkflowOptions | None = None


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

    def spawn_workflow(
        self, context: Context, input: SpawnWorkflowInput
    ) -> WorkflowRunRef:
        return context.aio.spawn_workflow(
            workflow_name=input.workflow_name,
            input=input.input.model_dump(),
            key=input.key,
            options=input.options,
        )

    def construct_spawn_workflow_input(
        self, input: TWorkflowInput
    ) -> SpawnWorkflowInput:
        return SpawnWorkflowInput(workflow_name=self.config.name, input=input)

    def declare(self) -> Callable[[Callable[[Context], Any]], Callable[[Context], Any]]:
        return self.hatchet.function(**self.config.model_dump())

    def workflow_input(self, ctx: Context) -> TWorkflowInput:
        return cast(TWorkflowInput, ctx.workflow_input())
