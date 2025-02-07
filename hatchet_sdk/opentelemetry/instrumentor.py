from importlib.metadata import version
from typing import Any, Callable, Collection, Coroutine, Never, cast

from opentelemetry.context import Context
from opentelemetry.instrumentation.instrumentor import (  # type: ignore[attr-defined]
    BaseInstrumentor,
)
from opentelemetry.metrics import MeterProvider, get_meter
from opentelemetry.trace import StatusCode, TracerProvider, get_tracer
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from wrapt import wrap_function_wrapper  # type: ignore[import-untyped]

import hatchet_sdk
from hatchet_sdk.clients.dispatcher.action_listener import Action
from hatchet_sdk.clients.events import EventClient, PushEventOptions
from hatchet_sdk.contracts.events_pb2 import Event
from hatchet_sdk.worker.runner.runner import Runner

hatchet_sdk_version = version("hatchet-sdk")

InstrumentKwargs = TracerProvider | MeterProvider | None


class HatchetInstrumentor(BaseInstrumentor):  # type: ignore[misc]
    OTEL_TRACEPARENT_KEY = "traceparent"

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
        tracer_provider = cast(TracerProvider, kwargs.get("tracer_provider"))
        meter_provider = cast(MeterProvider, kwargs.get("meter_provider"))

        self._tracer = get_tracer(__name__, hatchet_sdk_version, tracer_provider)
        self._meter = get_meter(__name__, hatchet_sdk_version, meter_provider)

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

    def _uninstrument(self, **kwargs: InstrumentKwargs) -> None:
        pass
