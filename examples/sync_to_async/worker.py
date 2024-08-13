import asyncio
import os
import time

from dotenv import load_dotenv

from hatchet_sdk import Context, wrap
from hatchet_sdk.v2.hatchet import Hatchet

os.environ["PYTHONASYNCIODEBUG"] = "1"
load_dotenv()

hatchet = Hatchet(debug=True)


@hatchet.function()
async def fanout_sync_async(context: Context) -> dict:
    print("spawning child")

    context.put_stream("spawning...")
    results = []

    n = context.workflow_input().get("n", 10)

    for i in range(n):
        results.append(
            (
                await context.aio.spawn_workflow(
                    "Child",
                    {"a": str(i)},
                    key=f"child{i}",
                    options={"additional_metadata": {"hello": "earth"}},
                )
            ).result()
        )

    result = await asyncio.gather(*results)

    return {"results": result}


@hatchet.workflow(on_events=["child:create"])
class Child:
    def sync_blocking_function(self):
        time.sleep(5)
        return {"type": "sync_blocking"}

    @hatchet.step()
    async def handle_blocking_sync_in_async(self, context: Context):
        wrapped_blocking_function = wrap(self.sync_blocking_function)
        data = await wrapped_blocking_function()  # this should now be async safe!
        return {"blocking_status": "success", "data": data}

    @hatchet.step()
    async def non_blocking_async(self, context: Context):
        await asyncio.sleep(5)
        return {"nonblocking_status": "success"}


def main():
    worker = hatchet.worker("fanout-worker", max_runs=30)
    worker.register_workflow(Child())
    worker.start()


if __name__ == "__main__":
    main()
