from typing import TypedDict

from dotenv import load_dotenv

from hatchet_sdk import Context
from hatchet_sdk.v2.callable import DurableContext
from hatchet_sdk.v2.hatchet import Hatchet

load_dotenv()

hatchet = Hatchet(debug=True)


class MyResultType(TypedDict):
    my_func: str


@hatchet.function()
def my_func(context: Context) -> MyResultType:
    return MyResultType(my_func="testing123")


@hatchet.durable()
async def my_durable_func(context: DurableContext):
    result = await context.run(my_func, {"test": "test"}).result()

    context.log(result)

    return {"my_durable_func": result.get("my_func")}


def main():
    worker = hatchet.worker("test-worker", max_runs=5)

    hatchet.admin.run(my_durable_func, {"test": "test"})

    worker.start()


if __name__ == "__main__":
    main()
