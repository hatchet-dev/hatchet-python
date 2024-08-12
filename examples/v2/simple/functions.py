from typing import TypedDict

from examples.v2.simple.client import hatchet, DurableContext, Context

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


