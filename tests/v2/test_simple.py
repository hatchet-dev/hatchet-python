import asyncio
import pytest


def get_client():
    import dotenv

    from hatchet_sdk.v2.hatchet import Hatchet

    dotenv.load_dotenv()
    return Hatchet(debug=True)


hatchet = get_client()


@hatchet.function()
async def foo(a: int):
    print(f"in foo: a={a}")
    return bar(b=3)


@hatchet.function()
def bar(b: int):
    print(f"in bar: b={b}")
    return b


# def test_trace():
#     import json

#     print(json.dumps(foo._debug(), indent=2))


@pytest.mark.asyncio(scope="session")
async def test_run():
    worker = hatchet.worker("worker", max_runs=5)
    c = foo(a=1)
    worker.start()
    print(await c)
