# from __future__ import annotations

import threading
import asyncio
import inspect
import json
from collections.abc import Awaitable, Callable, Iterator
from concurrent.futures.thread import ThreadPoolExecutor

# from contextvars import ContextVar, copy_context
from dataclasses import asdict, dataclass, field

# from datetime import timedelta
from typing import (
    Any,
    Dict,
    ForwardRef,
    Generic,
    Iterable,
    List,
    Literal,
    Optional,
    ParamSpec,
    TypedDict,
    TypeVar,
    Union,
)

from google.protobuf.json_format import MessageToDict

# from hatchet_sdk.logger import logger
from loguru import logger
from pydantic import BaseModel, ConfigDict, Field, computed_field
from pydantic.json_schema import SkipJsonSchema

import hatchet_sdk.v2.hatchet as v2hatchet
import hatchet_sdk.v2.runtime.context as context
import hatchet_sdk.v2.runtime.messages as messages
import hatchet_sdk.v2.runtime.utils as utils
from hatchet_sdk.clients.admin import TriggerWorkflowOptions
from hatchet_sdk.context import Context
from hatchet_sdk.context.context import BaseContext, Context, ContextAioImpl
from hatchet_sdk.contracts.dispatcher_pb2 import (
    AssignedAction,
    StepRunResult,
    SubscribeToWorkflowRunsRequest,
    WorkflowRunEvent,
    WorkflowRunEventType,
)
from hatchet_sdk.contracts.workflows_pb2 import (
    CreateStepRateLimit,
    CreateWorkflowJobOpts,
    CreateWorkflowStepOpts,
    CreateWorkflowVersionOpts,
    DesiredWorkerLabels,
    StickyStrategy,
    TriggerWorkflowRequest,
    TriggerWorkflowResponse,
    WorkflowConcurrencyOpts,
    WorkflowKind,
)
from hatchet_sdk.labels import DesiredWorkerLabel
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


@dataclass
class _CallableInput:
    args: List[Any] = field(default_factory=list)
    kwargs: Dict[str, Any] = field(default_factory=dict)

    def dumps(self):
        return json.dumps(asdict(self))

    @staticmethod
    def loads(s: str):
        # NOTE: AssignedAction.actionPayload looks like the following
        # '{"input": <from self.dumps()>, "parents": {}, "overrides": {}, "user_data": {}, "triggered_by": "manual"}'
        return _CallableInput(**(json.loads(s)["input"]))


@dataclass
class _CallableOutput(Generic[T]):
    output: Optional[T] = None

    def dumps(self):
        return json.dumps(asdict(self))

    @staticmethod
    def loads(s: str):
        return _CallableOutput(**json.loads(s))


class HatchetCallableBase(Generic[P, T]):
    def __init__(
        self,
        *,
        func: Callable[P, T],
        name: str,
        namespace: str,
        client: "v2hatchet.Hatchet",
        options: "Options",
    ):
        # TODO: maybe use __qualname__
        name = name.lower() or func.__name__.lower()
        self._hatchet = CallableMetadata(
            name=name,
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
        # TODO: handle concurrency function and on failure function
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

    def _encode_context(
        self, ctx: "context.BackgroundContext"
    ) -> TriggerWorkflowRequest:
        trigger = TriggerWorkflowRequest(
            additional_metadata=json.dumps(
                {"_hatchet_background_context": ctx.asdict()}
            ),
        )

        # We are not in any valid Hatchet context. This means we're the root.
        if ctx.current is None:
            return trigger

        # Otherwise, the current context is the parent.
        assert ctx.current is not None
        trigger.parent_id = ctx.current.workflow_run_id
        trigger.parent_step_run_id = ctx.current.step_run_id
        trigger.child_index = 0  # TODO: what is this
        return trigger

    def _to_trigger_proto(
        self, ctx: "context.BackgroundContext", inputs: _CallableInput
    ) -> TriggerWorkflowRequest:
        # NOTE: serialization error will be raised as TypeError
        req = TriggerWorkflowRequest(name=self._hatchet.name, input=inputs.dumps())
        req.MergeFrom(self._encode_context(ctx))
        return req

    def _decode_context(
        self, action: AssignedAction
    ) -> Optional["context.BackgroundContext"]:
        if not action.additional_metadata:
            return None

        d: Optional[Dict] = None
        try:
            d = json.loads(action.additional_metadata)
        except json.JSONDecodeError:
            logger.warning("failed to decode additional metadata from assigned action")
            return None

        if "_hatchet_background_context" not in d:
            return None

        ctx = context.BackgroundContext.fromdict(
            client=self._hatchet.client, data=d["_hatchet_background_context"]
        )
        ctx.client = self._hatchet.client
        return ctx

    # def _debug(self):
    #     data = {
    #         "self": repr(self),
    #         "metadata": self._hatchet._debug(),
    #         "def_proto": MessageToDict(self._to_workflow_proto()),
    #         "call_proto": (
    #             MessageToDict(self._ctx_to_trigger_proto())
    #             if self._to_trigger_proto()
    #             else None
    #         ),
    #     }
    #     return data

    def _decode_output(self, result: WorkflowRunEvent):
        steps = list(result.results)
        assert len(steps) == 1
        step = steps[0]
        if step.error:
            raise RuntimeError(step.error)
        else:
            return _CallableOutput.loads(step.output).output

    def _run(self, action: AssignedAction) -> str:
        # actually invokes the function, and serializing the output
        raise NotImplementedError


class HatchetCallable(HatchetCallableBase[P, T]):
    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        ctx = context.ensure_background_context()
        trigger = self._to_trigger_proto(
            ctx, inputs=_CallableInput(args=args, kwargs=kwargs)
        )
        logger.trace(
            "triggering on {}: {}", threading.get_ident(), MessageToDict(trigger)
        )
        client = self._hatchet.client
        ref: TriggerWorkflowResponse = client.admin.client.TriggerWorkflow(
            trigger, metadata=self._hatchet.client._grpc_metadata()
        )
        logger.trace("runid: {}", ref)
        # TODO: look into timeouts for Future.result()

        sub = SubscribeToWorkflowRunsRequest(workflowRunId=ref.workflow_run_id)
        wfre_future = self._hatchet.client._runtime.wfr_futures.submit(sub)

        return utils.MapFuture(
            self._decode_output, wfre_future, self._hatchet.client.executor
        ).result()

    def _run(self, action: AssignedAction) -> str:
        assert action.actionId == self._hatchet.action
        logger.trace("invoking:\n{}", MessageToDict(action))
        ctx = context.ensure_background_context(client=self._hatchet.client)
        assert ctx.current is None

        parent: Optional["context.BackgroundContext"] = self._decode_context(action)
        with context.WithParentContext(parent) as ctx:
            assert ctx.current is None
            ctx.current = context.RunInfo(
                workflow_run_id=action.workflowRunId,
                step_run_id=action.stepRunId,
                name=self._hatchet.name,
                namespace=self._hatchet.namespace,
            )
            if ctx.root is None:
                ctx.root = ctx.current.copy()
            with context.WithContext(ctx):
                inputs = _CallableInput.loads(action.actionPayload)
                output = _CallableOutput(
                    output=self._hatchet.func(*inputs.args, **inputs.kwargs)
                )
                logger.trace("output:\n{}", output)
                return output.dumps()


class HatchetAwaitable(HatchetCallableBase[P, Awaitable[T]]):
    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        print(f"trigering {self.action_name}")
        input = json.dumps({"args": args, "kwargs": kwargs})
        client = self._.options.hatchet
        return await client.admin.run(self._.name, input).result()

    async def _run(self, ctx: ContextAioImpl) -> T:
        print(f"trigering {self.action_name}")
        input = json.loads(ctx.workflow_input)
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

    options: "Options"
    client: "v2hatchet.Hatchet"

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


class HatchetContextBase:
    pass


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
