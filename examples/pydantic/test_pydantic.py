import pytest

from hatchet_sdk import Hatchet


# requires scope module or higher for shared event loop
@pytest.mark.asyncio(scope="session")
@pytest.mark.parametrize("worker", ["pydantic"], indirect=True)
async def test_run(hatchet: Hatchet, worker):
    run = hatchet.admin.run_workflow(
        "Parent",
        {},
    )
    result = await run.result()

    assert len(result["spawn"]) == 3
