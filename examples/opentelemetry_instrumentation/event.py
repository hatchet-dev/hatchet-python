from dotenv import load_dotenv

from examples.opentelemetry_instrumentation.client import hatchet
from examples.opentelemetry_instrumentation.tracer import trace_provider

with trace_provider.get_tracer(__name__).start_as_current_span("push_event") as span:
    span.add_event("Pushing event")
    event = hatchet.event.push(
        "otel:event",
        {"test": "test"},
        options={"additional_metadata": {"hello": "moon"}},
    )

    span.add_event("Pushed event", attributes={"event_id": event.eventId})
