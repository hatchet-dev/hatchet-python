import asyncio
from dotenv import load_dotenv

from hatchet_sdk import Context, Hatchet

load_dotenv()

hatchet = Hatchet(debug=True)


@hatchet.workflow(on_events=["parent:create"])
class Parent:
    @hatchet.step(timeout="10s")
    async def spawn(self, context: Context):
        print("spawning child")
        child = context.spawn_workflow("Child", key="child")
        child1 = context.spawn_workflow("Child", key="child1")
        results = [child.result(), child1.result()]

        result = await asyncio.gather(*results)
        print(f"results {result}")


@hatchet.workflow(on_events=["child:create"])
class Child:
    @hatchet.step()
    async def process(self, context: Context):
        print("child process")
        return {"status": "success"}
    
    @hatchet.step()
    async def process2(self, context: Context):
        print("child process2")
        return {"status2": "success"}


async def main():
    worker = hatchet.worker("fanout-worker", max_runs=4)
    worker.register_workflow(Parent())
    worker.register_workflow(Child())
    await worker.async_start()


if __name__ == "__main__":
    asyncio.run(main())
