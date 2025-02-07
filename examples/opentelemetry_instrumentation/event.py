from examples.opentelemetry_instrumentation.client import hatchet
from examples.opentelemetry_instrumentation.tracer import trace_provider
from hatchet_sdk.opentelemetry.instrumentor import HatchetInstrumentor

instrumentor = HatchetInstrumentor()

with trace_provider.get_tracer(__name__).start_as_current_span("push_event") as span:
    span.add_event("Pushing event")

    event = hatchet.event.push(
        "otel:event",
        {"test": "test"},
        options={
            "additional_metadata": instrumentor.inject_traceparent_into_metadata(
                {"hello": "world"}, instrumentor.create_traceparent()
            )
        },
    )

    span.add_event("Pushed event", attributes={"event_id": event.eventId})
