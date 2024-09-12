from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import Awaitable, Callable

# from contextvars import ContextVar, copy_context
from dataclasses import dataclass

# from datetime import timedelta
from typing import (
    Any,
    Dict,
    ForwardRef,
    Generic,
    List,
    Literal,
    Optional,
    ParamSpec,
    TypedDict,
    TypeVar,
    Union,
)

from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel, ConfigDict, Field, computed_field
from pydantic.json_schema import SkipJsonSchema

import hatchet_sdk.v2.hatchet as v2hatchet
from hatchet_sdk.clients.admin import TriggerWorkflowOptions
from hatchet_sdk.context import Context
from hatchet_sdk.context.context import BaseContext, Context, ContextAioImpl
from hatchet_sdk.contracts.workflows_pb2 import (
    CreateStepRateLimit,
    CreateWorkflowJobOpts,
    CreateWorkflowStepOpts,
    CreateWorkflowVersionOpts,
    DesiredWorkerLabels,
    StickyStrategy,
    WorkflowConcurrencyOpts,
    WorkflowKind,
)
from hatchet_sdk.labels import DesiredWorkerLabel
from hatchet_sdk.logger import logger
from hatchet_sdk.rate_limit import RateLimit
from hatchet_sdk.v2.concurrency import ConcurrencyFunction
from hatchet_sdk.v2.runtime import registry
from hatchet_sdk.workflow_run import RunRef

# from typing import TYPE_CHECKING

# if TYPE_CHECKING:
# from hatchet_sdk.v2.hatchet import Hatchet


T = TypeVar("T")
P = ParamSpec("P")


def _sourceloc(fn) -> str:
    try:
        return "{}:{}".format(
            inspect.getsourcefile(fn),
            inspect.getsourcelines(fn)[1],
        )
    except:
        return ""


class HatchetCallableBase(Generic[P, T]):

    def __init__(
        self,
        *,
        func: Callable[P, T],
        name: str,
        namespace: str,
        client: v2hatchet.Hatchet,
        options: Options,
    ):

        self._hatchet = CallableMetadata(
            # TODO: maybe use __qualname__
            name=name.lower() or str(func.__name__).lower(),
            namespace=namespace,
            sourceloc=_sourceloc(func),
            options=options,
            client=client,
            func=func,
            action=f"{namespace}:{name}",
        )
        client.registry.add(key=self._hatchet.action, callable=self)

    def _to_workflow_proto(self) -> CreateWorkflowVersionOpts:
        options = self._hatchet.options

        # if self.function_on_failure is not None:
        #     on_failure_job = CreateWorkflowJobOpts(
        #         name=self.function_name + "-on-failure",
        #         steps=[
        #             self.function_on_failure.to_step(),
        #         ],
        #     )
        # # concurrency: WorkflowConcurrencyOpts | None = None
        # if self.function_concurrency is not None:
        #     self.function_concurrency.set_namespace(self.function_namespace)
        #     concurrency = WorkflowConcurrencyOpts(
        #         action=self.function_concurrency.get_action_name(),
        #         max_runs=self.function_concurrency.max_runs,
        #         limit_strategy=self.function_concurrency.limit_strategy,
        #     )

        workflow = CreateWorkflowVersionOpts(
            name=self._hatchet.name,
            kind=WorkflowKind.DURABLE if options.durable else WorkflowKind.FUNCTION,
            version=options.version,
            event_triggers=options.on_events,
            cron_triggers=options.on_crons,
            schedule_timeout=options.schedule_timeout,
            sticky=options.sticky,
            on_failure_job=(
                options.on_failure._to_job_proto() if options.on_failure else None
            ),
            concurrency=None,  # TODO
            jobs=[
                self._to_job_proto()
            ],  # Note that the failure job is also a HatchetCallable, and it should manage its own name.
            default_priority=options.priority,
        )
        return workflow

    def _to_job_proto(self) -> CreateWorkflowJobOpts:
        job = CreateWorkflowJobOpts(
            name=self._hatchet.name, steps=[self._to_step_proto()]
        )
        return job

    def _to_step_proto(self) -> CreateWorkflowStepOpts:
        options = self._hatchet.options
        step = CreateWorkflowStepOpts(
            readable_id=self._hatchet.name,
            action=self._hatchet.action,
            timeout=options.execution_timeout,
            inputs="{}",  # TODO: not sure that this is, we're defining a step, not running a step
            parents=[],  # this is a single step workflow, always empty
            retries=options.retries,
            rate_limits=options.ratelimits,
            # worker_labels=self.function_desired_worker_labels,
        )
        return step

    def _to_trigger_proto(self) -> Optional[TriggerWorkflowOptions]:
        return None
        ctx = CallableContext.current()
        if not ctx:
            return None
        trigger: TriggerWorkflowOptions = {
            "parent_id": ctx.workflow_run_id,
            "parent_step_run_id": ctx.step_run_id,
        }
        return trigger

    def _debug(self):
        data = {
            "self": repr(self),
            "metadata": self._hatchet._debug(),
            "def_proto": MessageToDict(self._to_workflow_proto()),
            "call_proto": (
                MessageToDict(self._to_trigger_proto())
                if self._to_trigger_proto()
                else None
            ),
        }
        return data

    def _run(self, context: BaseContext):
        raise NotImplementedError


class HatchetCallable(HatchetCallableBase[P, T]):
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        self._hatchet.client.logger.info(f"triggering {self._hatchet.action}")
        input = json.dumps({"args": args, "kwargs": kwargs})
        client = self._hatchet.client
        ref = client.admin.trigger_workflow(
            self._hatchet.name, input=input, options=self._to_trigger_proto()
        )
        self._hatchet.client.logger.info(f"runid: {ref}")
        return None

    def _run(self, context: Context) -> T:
        print(f"running {self.action_name}")
        input = json.loads(context.workflow_input)
        return self.func(*input.args, **input.kwargs)


class HatchetAwaitable(HatchetCallableBase[P, Awaitable[T]]):
    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        print(f"trigering {self.action_name}")
        input = json.dumps({"args": args, "kwargs": kwargs})
        client = self._.options.hatchet
        return await client.admin.run(self._.name, input).result()

    async def _run(self, context: ContextAioImpl) -> T:
        print(f"trigering {self.action_name}")
        input = json.loads(context.workflow_input)
        return await self.func(*input.args, **input.kwargs)


class Options(BaseModel):
    # pydantic configuration
    model_config = ConfigDict(arbitrary_types_allowed=True)

    durable: bool = Field(default=False)
    auto_register: bool = Field(default=True)
    on_failure: Optional[HatchetCallableBase] = Field(default=None, exclude=True)

    # triggering options
    on_events: List[str] = Field(default=[])
    on_crons: List[str] = Field(default=[])

    # metadata
    version: str = Field(default="")

    # timeout
    execution_timeout: str = Field(default="60m", alias="timeout")
    schedule_timeout: str = Field(default="5m")

    # execution
    sticky: Optional[StickyStrategy] = Field(default=None)
    retries: int = Field(default=0, ge=0)
    ratelimits: List[RateLimit] = Field(default=[])
    priority: Optional[int] = Field(default=None, alias="default_priority", ge=1, le=3)
    desired_worker_labels: Dict[str, DesiredWorkerLabel] = Field(default=dict())
    concurrency: Optional[ConcurrencyFunction] = Field(default=None)

    @computed_field
    @property
    def ratelimits_proto(self) -> List[CreateStepRateLimit]:
        return [
            CreateStepRateLimit(key=limit.key, units=limit.units)
            for limit in self.ratelimits
        ]

    @computed_field
    @property
    def desired_worker_labels_proto(self) -> Dict[str, DesiredWorkerLabels]:
        labels = dict()
        for key, d in self.desired_worker_labels.items():
            value = d.get("value", None)
            labels[key] = DesiredWorkerLabels(
                strValue=str(value) if not isinstance(value, int) else None,
                intValue=value if isinstance(value, int) else None,
                required=d.get("required", None),
                weight=d.get("weight", None),
                comparator=d.get("comparator", None),
            )
        return labels


@dataclass
class CallableMetadata:
    func: Callable[P, T]  # the original function

    name: str
    namespace: str
    action: str
    sourceloc: str  # source location of the callable

    options: Options
    client: v2hatchet.Hatchet

    def _debug(self):
        return {
            "func": repr(self.func),
            "name": self.name,
            "namespace": self.namespace,
            "action": self.action,
            "sourceloc": self.sourceloc,
            "client": repr(self.client),
            "options": self.options.model_dump(),
        }


# # Context variable used for propagating hatchet context.
# # The type of the variable is CallableContext.
# _callable_cv = ContextVar("hatchet.callable")


# # The context object to be propagated between parent/child workflows.
# class CallableContext(BaseModel):
#     # pydantic configuration
#     model_config = ConfigDict(arbitrary_types_allowed=True)

#     caller: Optional["HatchetCallable[P,T]"] = None
#     workflow_run_id: str  # caller's workflow run id
#     step_run_id: str  # caller's step run id

#     @staticmethod
#     def cv() -> ContextVar:
#         return _callable_cv

#     @staticmethod
#     def current() -> Optional["CallableContext"]:
#         try:
#             cv: ContextVar = CallableContext.cv()
#             return cv.get()
#         except LookupError:
#             return None


# T = TypeVar("T")


# class TriggerOptions(TypedDict):
#     additional_metadata: Dict[str, str] | None = None
#     sticky: bool | None = None


# class DurableContext(Context):
#     pass


# #     def run(
# #         self,
# #         function: Union[str, HatchetCallable[T]],
# #         input: dict = {},
# #         key: str = None,
# #         options: TriggerOptions = None,
# #     ) -> "RunRef[T]":
# #         worker_id = self.worker.id()

# #         workflow_name = function

# #         if not isinstance(function, str):
# #             workflow_name = function.function_name

# #         # if (
# #         #     options is not None
# #         #     and "sticky" in options
# #         #     and options["sticky"] == True
# #         #     and not self.worker.has_workflow(workflow_name)
# #         # ):
# #         #     raise Exception(
# #         #         f"cannot run with sticky: workflow {workflow_name} is not registered on the worker"
# #         #     )

# #         trigger_options = self._prepare_workflow_options(key, options, worker_id)

# #         return self.admin_client.run(function, input, trigger_options)
