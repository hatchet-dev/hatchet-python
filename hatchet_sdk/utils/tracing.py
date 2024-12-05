import json
from functools import cache
from typing import Any

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import NoOpTracerProvider, Tracer
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from hatchet_sdk.loader import ClientConfig

OTEL_CARRIER_KEY = "__otel_carrier"


@cache
def create_tracer(config: ClientConfig) -> Tracer:
    ## TODO: Figure out how to specify protocol here
    resource = Resource(
        attributes={SERVICE_NAME: config.otel_service_name or "hatchet.run"}
    )

    if config.otel_exporter_oltp_endpoint and config.otel_exporter_oltp_headers:
        processor = BatchSpanProcessor(
            OTLPSpanExporter(
                endpoint=config.otel_exporter_oltp_endpoint,
                headers=config.otel_exporter_oltp_headers,
            ),
        )

        ## If tracer provider is already set, we don't need to override it
        if not isinstance(trace.get_tracer_provider(), TracerProvider):
            trace_provider = TracerProvider(resource=resource)
            trace_provider.add_span_processor(processor)
            trace.set_tracer_provider(trace_provider)
    else:
        if not isinstance(trace.get_tracer_provider(), NoOpTracerProvider):
            trace.set_tracer_provider(NoOpTracerProvider())

    return trace.get_tracer(__name__)


def create_carrier() -> dict[str, str]:
    carrier: dict[str, str] = {}
    TraceContextTextMapPropagator().inject(carrier)

    return carrier


def inject_carrier_into_metadata(
    metadata: dict[Any, Any], carrier: dict[str, str]
) -> dict[Any, Any]:
    if carrier:
        metadata[OTEL_CARRIER_KEY] = carrier

    return metadata


def parse_carrier_from_metadata(metadata: dict[str, Any] | None) -> Context | None:
    if not metadata:
        return None

    return (
        TraceContextTextMapPropagator().extract(_ctx)
        if (_ctx := metadata.get(OTEL_CARRIER_KEY))
        else None
    )
