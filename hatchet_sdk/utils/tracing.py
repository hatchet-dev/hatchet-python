from typing import Collection

from opentelemetry.instrumentation.instrumentor import BaseInstrumentor

_instruments = tuple()
from importlib.metadata import version

from opentelemetry.metrics import get_meter
from opentelemetry.trace import get_tracer
from wrapt import wrap_function_wrapper

import hatchet_sdk

hatchet_sdk_version = version("hatchet-sdk")


class HatchetInstrumentor(BaseInstrumentor):
    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs):
        self._tracer = get_tracer(
            __name__, hatchet_sdk_version, kwargs.get("tracer_provider")
        )
        self._meter = get_meter(
            __name__, hatchet_sdk_version, kwargs.get("meter_provider")
        )

        wrap_function_wrapper(
            hatchet_sdk, "worker.runner.runner.Runner.handle_start_step_run", self._wrap_start_step_run
        )

    def _wrap_start_step_run(self, wrapped, instance, args, kwargs):
        with self._tracer.start_as_current_span("hatchet.start_step_run"):
            return wrapped(*args, **kwargs)

    def _uninstrument(self, **kwargs):
        self._meter = None
        self._tracer = None
