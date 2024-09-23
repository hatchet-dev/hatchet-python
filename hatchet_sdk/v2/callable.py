import asyncio
import inspect
import json
from collections.abc import Awaitable, Callable
from concurrent.futures import Future
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Generic, List, Optional, ParamSpec, Tuple, TypeVar

from google.protobuf.json_format import MessageToDict
from loguru import logger
from pydantic import BaseModel, ConfigDict, Field, computed_field

import hatchet_sdk.v2.hatchet as hatchet
import hatchet_sdk.v2.runtime.context as context
import hatchet_sdk.v2.runtime.utils as utils
from hatchet_sdk.contracts.dispatcher_pb2 import (
    AssignedAction,
    SubscribeToWorkflowRunsRequest,
    WorkflowRunEvent,
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
    WorkflowKind,
)
from hatchet_sdk.labels import DesiredWorkerLabel
from hatchet_sdk.rate_limit import RateLimit
from hatchet_sdk.v2.concurrency import ConcurrencyFunction

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


# Note: this should be language independent, and useable by Go/Typescript, etc.
@dataclass
class _CallableInput:
    """The input to a Hatchet callable."""

    args: Tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)

    def dumps(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def loads(s: str) -> "_CallableInput":
        # NOTE: AssignedAction.actionPayload looks like the following
        # '{"input": <from self.dumps()>, "parents": {}, "overrides": {}, "user_data": {}, "triggered_by": "manual"}'
        return _CallableInput(**(json.loads(s)["input"]))


# Note: this should be language independent, and usable by Go/Typescript, etc.
@dataclass
class _CallableOutput(Generic[T]):
    """The output of a Hatchet callable."""

    output: T

    def dumps(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def loads(s: str) -> "_CallableOutput[T]":
        ret = _CallableOutput(**json.loads(s))
        return ret


class HatchetCallableBase(Generic[P, T]):
    """Hatchet callable base."""

    def __init__(
        self,
        *,
        func: Callable[P, T],
        name: str,
        namespace: str,
        client: "hatchet.Hatchet",
        options: "Options",
    ):
        # TODO: maybe use __qualname__
        name = name.lower() or func.__name__.lower()

        # hide everything under self._hatchet since the user has access to everything in HatchetCallableBase.
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
            # rate_limits=options.ratelimits, # TODO
            # worker_labels=self.function_desired_worker_labels, # TODO
        )
        return step

    def _encode_context(
        self, ctx: "context.BackgroundContext"
    ) -> TriggerWorkflowRequest:
        """Encode the given context into the trigger protobuf."""
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
        trigger.parent_id = ctx.current.workflow_run_id or ""
        trigger.parent_step_run_id = ctx.current.step_run_id or ""
        trigger.child_index = 0  # TODO: this is no longer needed since the user has full control of how they wanna trigger the children
        return trigger

    def _decode_context(
        self, action: AssignedAction
    ) -> Optional["context.BackgroundContext"]:
        """Reconstruct the background context using the assigned action protobuf."""
        if not action.additional_metadata:
            return None

        d: Optional[Dict] = None
        try:
            d = json.loads(action.additional_metadata)
        except json.JSONDecodeError:
            logger.warning("failed to decode additional metadata from assigned action")
            return None

        assert isinstance(d, Dict)
        if "_hatchet_background_context" not in d:
            return None

        ctx = context.BackgroundContext.fromdict(
            client=self._hatchet.client, data=d["_hatchet_background_context"]
        )
        ctx.client = self._hatchet.client
        return ctx

    def _to_trigger_proto(
        self, ctx: "context.BackgroundContext", inputs: _CallableInput
    ) -> TriggerWorkflowRequest:
        # NOTE: serialization error will be raised as TypeError
        req = TriggerWorkflowRequest(name=self._hatchet.name, input=inputs.dumps())
        req.MergeFrom(self._encode_context(ctx))
        return req

    # TODO: the return type of decode output needs to be casted.
    # For Callable[P, T] the return type is T.
    # For Callable[P, Awaitable[T]], the return type is T.
    def _decode_output(self, result: WorkflowRunEvent):
        """Decode the output from a WorkflowRunEvent.

        Note that the WorkflowRunEvent could be, in the future, encoded from a
        different language, like Typescript or Go.
        """
        steps = list(result.results)
        assert len(steps) == 1  # assumping single step workflows
        step = steps[0]
        if step.error:
            # TODO: find a way to be more precise about the type of exception.
            # right now everything is a RuntimeError.
            raise RuntimeError(step.error)
        else:
            ret = _CallableOutput.loads(step.output).output
            return ret

    def _trigger(self, *args: P.args, **kwargs: P.kwargs) -> TriggerWorkflowResponse:
        ctx = context.ensure_background_context()
        trigger = self._to_trigger_proto(
            ctx, inputs=_CallableInput(args=args, kwargs=kwargs)
        )
        logger.trace("triggering: {}", MessageToDict(trigger))
        client = self._hatchet.client
        ref: TriggerWorkflowResponse = client.admin.client.TriggerWorkflow(
            trigger, metadata=self._hatchet.client._grpc_metadata()
        )
        logger.trace("runid: {}", ref)
        return ref

    def _make_ctx(self, action: AssignedAction) -> "context.BackgroundContext":
        ctx = context.ensure_background_context(client=self._hatchet.client)
        assert ctx.current is None

        parent = self._decode_context(action) or context.BackgroundContext(
            client=self._hatchet.client
        )
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
            return ctx


class HatchetCallable(HatchetCallableBase[P, T]):
    """A Hatchet callable wrapping a non-asyncio free function."""

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Future[T]:
        """Trigger a workflow run and returns the future.

        Note that it is important that we return a Future. We want the user
        to trigger multiple calls and decide when to synchronize. Like,

            concurrent.futures.as_completed(wf1(), wf2(), wf3())
        """
        ref = self._trigger(*args, **kwargs)

        # now setup to wait for the result
        sub = SubscribeToWorkflowRunsRequest(workflowRunId=ref.workflow_run_id)

        # TODO: expose a better interface on the Hatchet client for waiting on results.
        wfre_future = self._hatchet.client.worker()._wfr_futures.submit(sub)

        fut: Future[T] = utils.MapFuture(
            self._decode_output, wfre_future, self._hatchet.client.executor
        )
        return fut

    def _run(self, action: AssignedAction) -> str:
        """Executes the actual code and returns a serialized output."""

        logger.trace("invoking: {}", MessageToDict(action))
        assert action.actionId == self._hatchet.action

        ctx = self._make_ctx(action)
        with context.WithContext(ctx):
            inputs = _CallableInput.loads(action.actionPayload)
            output = _CallableOutput(
                output=self._hatchet.func(*inputs.args, **inputs.kwargs)
            )
            logger.trace("output: {}", output)
            return output.dumps()


class HatchetAwaitable(HatchetCallableBase[P, Awaitable[T]]):
    """A Hatchet callable wrapping an asyncio free function."""

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        ref = self._trigger(*args, **kwargs)

        # now setup to wait for the result
        sub = SubscribeToWorkflowRunsRequest(workflowRunId=ref.workflow_run_id)

        # TODO: expose a better interface on the Hatchet client for waiting on results.
        wfre_future = await self._hatchet.client.worker()._wfr_futures.asubmit(sub)

        return self._decode_output(await wfre_future)

    async def _run(self, action: AssignedAction) -> str:
        logger.trace("invoking: {}", MessageToDict(action))
        assert action.actionId == self._hatchet.action

        ctx = self._make_ctx(action)
        with context.WithContext(ctx):
            inputs = _CallableInput.loads(action.actionPayload)
            output = _CallableOutput(
                output=await self._hatchet.func(*inputs.args, **inputs.kwargs)
            )
            logger.trace("output: {}", output)
            return output.dumps()


class Options(BaseModel):
    """The options for a Hatchet function (aka workflow)."""

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
        # TODO: double check the default values
        labels = dict()
        for key, d in self.desired_worker_labels.items():
            value = d.get("value", None)
            labels[key] = DesiredWorkerLabels(
                strValue=str(value) if not isinstance(value, int) else None,
                intValue=value if isinstance(value, int) else None,
                required=d.get("required") or False,
                weight=d.get("weight") or 0,
                comparator=str(d.get("comparator")) or None,
            )
        return labels


@dataclass
class CallableMetadata(Generic[P, T]):
    """Metadata field for a decorated Hatchet workflow."""

    func: Callable[P, T]  # the original function

    name: str
    namespace: str
    action: str
    sourceloc: str  # source location of the callable

    options: "Options"
    client: "hatchet.Hatchet"

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
