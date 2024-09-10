def get_client():
    import dotenv

    from hatchet_sdk.v2.hatchet import Hatchet

    dotenv.load_dotenv()
    return Hatchet(debug=True)


hatchet = get_client()


@hatchet.function()
async def foo(a: int):
    return bar(b=3)


@hatchet.function()
def bar(b: int):
    return b


def test_trace():
    import json
    print(json.dumps(foo._debug(), indent=2))
