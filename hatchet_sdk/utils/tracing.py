from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
)
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource

from hatchet_sdk.loader import ClientConfig
from functools import cache


class OTelTracingFactory:
    @classmethod
    @cache
    def create_tracer(cls, name: str, config: ClientConfig):
        ## TODO: Figure out how to specify protocol here
        resource = Resource(attributes={SERVICE_NAME: config.otel_service_name})
        processor = BatchSpanProcessor(
            OTLPSpanExporter(
                endpoint=config.otel_exporter_oltp_endpoint,
                headers=config.otel_exporter_oltp_headers
            ),
        )

        trace_provider = TracerProvider(resource=resource)
        trace_provider.add_span_processor(processor)

        trace.set_tracer_provider(trace_provider)

        return trace.get_tracer(name)