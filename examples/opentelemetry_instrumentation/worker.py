from dotenv import load_dotenv
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from hatchet_sdk import Context, Hatchet
from hatchet_sdk.utils.tracing import HatchetInstrumentor

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
trace.set_tracer_provider(trace_provider)

HatchetInstrumentor().instrument()


@hatchet.workflow(on_events=["user:create"])
class OTelWorkflow:
    @hatchet.step()
    def step1(self, context: Context) -> dict[str, str]:
        with trace.get_tracer(__name__).start_as_current_span("step1") as span:
            print("executed step1")
            # raise Exception("test")
            return {
                "step1": "step1",
            }

    @hatchet.step()
    def step2(self, context: Context) -> dict[str, str]:
        with trace.get_tracer(__name__).start_as_current_span("step1") as span:
            raise Exception("test error")


def main() -> None:
    worker = hatchet.worker("otel-example-worker", max_runs=1)
    worker.register_workflow(OTelWorkflow())
    worker.start()


if __name__ == "__main__":
    main()
