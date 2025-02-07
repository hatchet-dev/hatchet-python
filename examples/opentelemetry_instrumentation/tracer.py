import os
from typing import cast

from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from examples.opentelemetry_instrumentation.client import hatchet

resource = Resource(
    attributes={SERVICE_NAME: os.environ["HATCHET_CLIENT_OTEL_SERVICE_NAME"]}
)

headers = dict(
    [
        cast(
            tuple[str, str],
            tuple(os.environ["HATCHET_CLIENT_OTEL_EXPORTER_OTLP_HEADERS"].split("=")),
        )
    ]
)

processor = BatchSpanProcessor(
    OTLPSpanExporter(
        endpoint=os.environ["HATCHET_CLIENT_OTEL_EXPORTER_OTLP_ENDPOINT"],
        headers=headers,
    ),
)

trace_provider = TracerProvider(resource=resource)
trace_provider.add_span_processor(processor)
