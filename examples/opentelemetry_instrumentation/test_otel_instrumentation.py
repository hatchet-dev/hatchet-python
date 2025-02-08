import json

import pytest
from opentelemetry.trace import NoOpTracerProvider

from hatchet_sdk import Hatchet, Worker
from hatchet_sdk.clients.admin import TriggerWorkflowOptions
from hatchet_sdk.clients.events import PushEventOptions
from hatchet_sdk.opentelemetry.instrumentor import HatchetInstrumentor

trace_provider = NoOpTracerProvider()

instrumentor = HatchetInstrumentor(tracer_provider=trace_provider)
instrumentor.instrument()

tracer = trace_provider.get_tracer(__name__)


def create_additional_metadata() -> dict[str, str]:
    return instrumentor.inject_traceparent_into_metadata(
        {"hello": "world"}, instrumentor.create_traceparent()
    )


def create_push_options() -> PushEventOptions:
    return {"additional_metadata": create_additional_metadata()}


@pytest.mark.parametrize("worker", ["otel"], indirect=True)
def test_push_event(hatchet: Hatchet, worker: Worker) -> None:
    key = "otel:event"
    payload = {"test": "test"}

    with tracer.start_as_current_span("push_event"):
        event = hatchet.event.push(
            event_key=key,
            payload=payload,
            options=create_push_options(),
        )

        assert event.key == key
        assert event.payload == json.dumps(payload)


@pytest.mark.asyncio()
@pytest.mark.parametrize("worker", ["otel"], indirect=True)
async def test_run_workflow(hatchet: Hatchet, worker: Worker) -> None:
    with tracer.start_as_current_span("run_workflow") as span:
        workflow = hatchet.admin.run_workflow(
            "OTelWorkflow",
            {"test": "test"},
            options=TriggerWorkflowOptions(
                additional_metadata=create_additional_metadata()
            ),
        )

        with pytest.raises(Exception, match="Workflow Errors"):
            await workflow.result()
