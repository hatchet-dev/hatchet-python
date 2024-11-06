import json
from functools import cache
from typing import Any

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import Tracer, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from hatchet_sdk.loader import ClientConfig

OTEL_CARRIER_KEY = "__otel_carrier"

@cache
def create_tracer(config: ClientConfig) -> Tracer:
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

    return trace.get_tracer(__name__)

def create_carrier() -> dict[str, str]:
    carrier = {}
    TraceContextTextMapPropagator().inject(carrier)

    return carrier

def inject_carrier_into_metadata(metadata: dict[Any, Any], carrier: dict[str, str]) -> dict[Any, Any]:
    metadata[OTEL_CARRIER_KEY] = carrier

    return metadata

def parse_carrier_from_metadata(metadata: dict[str, Any]) -> Context:
    return (
        TraceContextTextMapPropagator().extract(_ctx)
        if (_ctx := metadata.get(OTEL_CARRIER_KEY))
        else None
    )
