from dotenv import load_dotenv
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from hatchet_sdk import Hatchet

load_dotenv()

hatchet = Hatchet(debug=True)

resource = Resource(
    attributes={SERVICE_NAME: hatchet.config.otel_service_name or "hatchet.run"}
)

processor = BatchSpanProcessor(
    OTLPSpanExporter(
        endpoint=hatchet.config.otel_exporter_oltp_endpoint,
        headers=hatchet.config.otel_exporter_oltp_headers,
    ),
)

trace_provider = TracerProvider(resource=resource)
trace_provider.add_span_processor(processor)
