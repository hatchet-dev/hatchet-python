import asyncio

from dotenv import load_dotenv

from hatchet_sdk import Context, Hatchet

load_dotenv()

hatchet = Hatchet(debug=True)


@hatchet.workflow(on_events=["parent:create"])
class Parent:
    @hatchet.step(timeout="5m")
    async def spawn(self, context: Context):
        print("spawning child")

        results = []

        for i in range(100):
            results.append(
                (
                    await context.aio.spawn_workflow(
                        "Child", {"a": str(i)}, key=f"child{i}"
                    )
                ).result()
            )

        result = await asyncio.gather(*results)
        print(f"results {result}")

        return {"results": result}


@hatchet.workflow(on_events=["child:create"])
class Child:
    @hatchet.step()
    async def process(self, context: Context):
        a = context.workflow_input()["a"]
        print(f"child process {a}")
        return {"status": "success " + a}

    @hatchet.step()
    async def process2(self, context: Context):
        print("child process2")
        return {"status2": "success"}


def main():
    worker = hatchet.worker("fanout-worker", max_runs=40)
    worker.register_workflow(Parent())
    worker.register_workflow(Child())
    worker.start()


if __name__ == "__main__":
    main()
