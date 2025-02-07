from dotenv import load_dotenv
from opentelemetry import trace

from examples.opentelemetry_instrumentation.tracer import trace_provider
from hatchet_sdk import Context, Hatchet
from hatchet_sdk.utils.tracing import HatchetInstrumentor

load_dotenv()

hatchet = Hatchet(debug=True)

HatchetInstrumentor().instrument(
    tracer_provider=trace_provider,
)


@hatchet.workflow(on_events=["otel:event"])
class OTelWorkflow:
    @hatchet.step()
    def step1(self, context: Context) -> dict[str, str]:
        with trace_provider.get_tracer(__name__).start_as_current_span("step1"):
            print("executed step1")
            return {
                "step1": "step1",
            }

    @hatchet.step()
    def step2(self, context: Context) -> None:
        with trace_provider.get_tracer(__name__).start_as_current_span("step2"):
            raise Exception("step 2 failed")


def main() -> None:
    worker = hatchet.worker("otel-example-worker", max_runs=1)
    worker.register_workflow(OTelWorkflow())
    worker.start()


if __name__ == "__main__":
    main()
