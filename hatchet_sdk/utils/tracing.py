import json

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider, Tracer
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
)
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from typing import Any

from hatchet_sdk.loader import ClientConfig
from functools import cache
from hatchet_sdk.utils.serialization import flatten

@cache
def create_tracer(name: str, config: ClientConfig) -> Tracer:
    ## TODO: Figure out how to specify protocol here
    resource = Resource(
        attributes={SERVICE_NAME: config.otel_service_name or "hatchet.run"}
    )
    processor = BatchSpanProcessor(
        OTLPSpanExporter(
            endpoint=config.otel_exporter_oltp_endpoint,
            headers=config.otel_exporter_oltp_headers,
        ),
    )

    trace_provider = TracerProvider(resource=resource)
    trace_provider.add_span_processor(processor)

    trace.set_tracer_provider(trace_provider)

    return trace.get_tracer(name)

def munge_metadata(meta: dict[str, Any] | None) -> bytes | None:
    span = trace.get_current_span()
    span.set_attributes(flatten(meta, parent_key="", separator="."))

    carrier = {}
    TraceContextTextMapPropagator().inject(carrier)

    meta["__otel_carrier"] = carrier

    return None if meta is None else json.dumps(meta).encode("utf-8")

def parse_carrier_from_metadata(metadata: dict[str, Any] | None) -> Context:
    metadata |= {}

    return (
        TraceContextTextMapPropagator().extract(_ctx)
        if (_ctx := metadata.get("__otel_carrier"))
        else None
    )

