from importlib.metadata import version
from typing import Any, Callable, Collection, Coroutine, Never, cast

from opentelemetry.context import Context
from opentelemetry.instrumentation.instrumentor import (  # type: ignore[attr-defined]
    BaseInstrumentor,
)
from opentelemetry.metrics import MeterProvider, NoOpMeterProvider, get_meter
from opentelemetry.trace import (
    NoOpTracerProvider,
    StatusCode,
    TracerProvider,
    get_tracer,
)
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from wrapt import wrap_function_wrapper  # type: ignore[import-untyped]

import hatchet_sdk
from hatchet_sdk.clients.admin import (
    AdminClient,
    TriggerWorkflowOptions,
    WorkflowRunDict,
)
from hatchet_sdk.clients.dispatcher.action_listener import Action
from hatchet_sdk.clients.events import (
    BulkPushEventWithMetadata,
    EventClient,
    PushEventOptions,
)
from hatchet_sdk.contracts.events_pb2 import Event
from hatchet_sdk.worker.runner.runner import Runner
from hatchet_sdk.workflow_run import WorkflowRunRef

hatchet_sdk_version = version("hatchet-sdk")

InstrumentKwargs = TracerProvider | MeterProvider | None


class HatchetInstrumentor(BaseInstrumentor):  # type: ignore[misc]
    OTEL_TRACEPARENT_KEY = "traceparent"

    def __init__(
        self,
        tracer_provider: TracerProvider,
        meter_provider: MeterProvider = NoOpMeterProvider(),
    ):
        self.tracer_provider = tracer_provider
        self.meter_provider = meter_provider

        super().__init__()

    def create_traceparent(self) -> str | None:
        carrier: dict[str, str] = {}
        TraceContextTextMapPropagator().inject(carrier)

        return carrier.get("traceparent")

    def parse_carrier_from_metadata(
        self, metadata: dict[str, str] | None
    ) -> Context | None:
        if not metadata:
            return None

        traceparent = metadata.get(self.OTEL_TRACEPARENT_KEY)

        if not traceparent:
            return None

        return TraceContextTextMapPropagator().extract(
            {self.OTEL_TRACEPARENT_KEY: traceparent}
        )

    def inject_traceparent_into_metadata(
        self, metadata: dict[str, str], traceparent: str | None
    ) -> dict[str, str]:
        if traceparent:
            metadata[self.OTEL_TRACEPARENT_KEY] = traceparent

        return metadata

    def instrumentation_dependencies(self) -> Collection[str]:
        return tuple()

    def _instrument(self, **kwargs: InstrumentKwargs) -> None:
        self._tracer = get_tracer(__name__, hatchet_sdk_version, self.tracer_provider)
        self._meter = get_meter(__name__, hatchet_sdk_version, self.meter_provider)

        wrap_function_wrapper(
            hatchet_sdk,
            "worker.runner.runner.Runner.handle_start_step_run",
            self._wrap_handle_start_step_run,
        )
        wrap_function_wrapper(
            hatchet_sdk,
            "worker.runner.runner.Runner.handle_start_group_key_run",
            self._wrap_handle_get_group_key_run,
        )
        wrap_function_wrapper(
            hatchet_sdk,
            "worker.runner.runner.Runner.handle_cancel_action",
            self._wrap_handle_cancel_action,
        )

        wrap_function_wrapper(
            hatchet_sdk,
            "clients.events.EventClient.push",
            self._wrap_push_event,
        )

        wrap_function_wrapper(
            hatchet_sdk,
            "clients.events.EventClient.bulk_push",
            self._wrap_bulk_push_event,
        )

        wrap_function_wrapper(
            hatchet_sdk,
            "clients.admin.AdminClient.run_workflow",
            self._wrap_run_workflow,
        )

        wrap_function_wrapper(
            hatchet_sdk,
            "clients.admin.AdminClientAioImpl.run_workflow",
            self._wrap_async_run_workflow,
        )

        wrap_function_wrapper(
            hatchet_sdk,
            "clients.admin.AdminClient.run_workflows",
            self._wrap_run_workflows,
        )

        wrap_function_wrapper(
            hatchet_sdk,
            "clients.admin.AdminClientAioImpl.run_workflows",
            self._wrap_async_run_workflows,
        )

    ## IMPORTANT: Keep these types in sync with the wrapped method's signature
    async def _wrap_handle_start_step_run(
        self,
        wrapped: Callable[[Action], Coroutine[None, None, Exception | None]],
        instance: Runner,
        args: tuple[Action],
        kwargs: Never,
    ) -> Exception | None:
        action = args[0]
        traceparent = self.parse_carrier_from_metadata(action.additional_metadata)

        with self._tracer.start_as_current_span(
            "hatchet.start_step_run",
            attributes=action.otel_attributes,
            context=traceparent,
        ) as span:
            result = await wrapped(*args, **kwargs)

            if isinstance(result, Exception):
                span.set_status(StatusCode.ERROR, str(result))

            return result

    ## IMPORTANT: Keep these types in sync with the wrapped method's signature
    async def _wrap_handle_get_group_key_run(
        self,
        wrapped: Callable[[Action], Coroutine[None, None, Exception | None]],
        instance: Runner,
        args: tuple[Action],
        kwargs: Never,
    ) -> Exception | None:
        action = args[0]

        with self._tracer.start_as_current_span(
            "hatchet.get_group_key_run",
            attributes=action.otel_attributes,
        ) as span:
            result = await wrapped(*args, **kwargs)

            if isinstance(result, Exception):
                span.set_status(StatusCode.ERROR, str(result))

            return result

    ## IMPORTANT: Keep these types in sync with the wrapped method's signature
    async def _wrap_handle_cancel_action(
        self,
        wrapped: Callable[[str], Coroutine[None, None, Exception | None]],
        instance: Runner,
        args: tuple[str],
        kwargs: Never,
    ) -> Exception | None:
        step_run_id = args[0]

        with self._tracer.start_as_current_span(
            "hatchet.cancel_step_run",
            attributes={
                "hatchet.step_run_id": step_run_id,
            },
        ):
            return await wrapped(*args, **kwargs)

    ## IMPORTANT: Keep these types in sync with the wrapped method's signature
    def _wrap_push_event(
        self,
        wrapped: Callable[[str, dict[str, Any], PushEventOptions | None], Event],
        instance: EventClient,
        args: tuple[
            str,
            dict[str, Any],
            PushEventOptions | None,
        ],
        kwargs: dict[str, str | dict[str, Any] | PushEventOptions | None],
    ) -> Event:
        with self._tracer.start_as_current_span(
            "hatchet.push_event",
        ):
            return wrapped(*args, **kwargs)

    ## IMPORTANT: Keep these types in sync with the wrapped method's signature
    def _wrap_bulk_push_event(
        self,
        wrapped: Callable[
            [list[BulkPushEventWithMetadata], PushEventOptions | None], list[Event]
        ],
        instance: EventClient,
        args: tuple[
            list[BulkPushEventWithMetadata],
            PushEventOptions | None,
        ],
        kwargs: dict[str, list[BulkPushEventWithMetadata] | PushEventOptions | None],
    ) -> list[Event]:
        with self._tracer.start_as_current_span(
            "hatchet.bulk_push_event",
        ):
            return wrapped(*args, **kwargs)

    ## IMPORTANT: Keep these types in sync with the wrapped method's signature
    def _wrap_run_workflow(
        self,
        wrapped: Callable[[str, Any, TriggerWorkflowOptions | None], WorkflowRunRef],
        instance: AdminClient,
        args: tuple[str, Any, TriggerWorkflowOptions | None],
        kwargs: dict[str, str | Any | TriggerWorkflowOptions | None],
    ) -> WorkflowRunRef:
        with self._tracer.start_as_current_span(
            "hatchet.run_workflow",
        ):
            return wrapped(*args, **kwargs)

    ## IMPORTANT: Keep these types in sync with the wrapped method's signature
    async def _wrap_async_run_workflow(
        self,
        wrapped: Callable[
            [str, Any, TriggerWorkflowOptions | None],
            Coroutine[None, None, WorkflowRunRef],
        ],
        instance: AdminClient,
        args: tuple[str, Any, TriggerWorkflowOptions | None],
        kwargs: dict[str, str | Any | TriggerWorkflowOptions | None],
    ) -> WorkflowRunRef:
        with self._tracer.start_as_current_span(
            "hatchet.run_workflow",
        ):
            return await wrapped(*args, **kwargs)

    ## IMPORTANT: Keep these types in sync with the wrapped method's signature
    def _wrap_run_workflows(
        self,
        wrapped: Callable[
            [list[WorkflowRunDict], TriggerWorkflowOptions | None], list[WorkflowRunRef]
        ],
        instance: AdminClient,
        args: tuple[
            list[WorkflowRunDict],
            TriggerWorkflowOptions | None,
        ],
        kwargs: dict[str, list[WorkflowRunDict] | TriggerWorkflowOptions | None],
    ) -> list[WorkflowRunRef]:
        with self._tracer.start_as_current_span(
            "hatchet.run_workflows",
        ):
            return wrapped(*args, **kwargs)

    ## IMPORTANT: Keep these types in sync with the wrapped method's signature
    async def _wrap_async_run_workflows(
        self,
        wrapped: Callable[
            [list[WorkflowRunDict], TriggerWorkflowOptions | None],
            Coroutine[None, None, list[WorkflowRunRef]],
        ],
        instance: AdminClient,
        args: tuple[
            list[WorkflowRunDict],
            TriggerWorkflowOptions | None,
        ],
        kwargs: dict[str, list[WorkflowRunDict] | TriggerWorkflowOptions | None],
    ) -> list[WorkflowRunRef]:
        with self._tracer.start_as_current_span(
            "hatchet.run_workflows",
        ):
            return await wrapped(*args, **kwargs)

    def _uninstrument(self, **kwargs: InstrumentKwargs) -> None:
        self.tracer_provider = NoOpTracerProvider()
        self.meter_provider = NoOpMeterProvider()
