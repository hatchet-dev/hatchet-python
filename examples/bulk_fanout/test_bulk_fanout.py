import pytest

from hatchet_sdk import Hatchet


# requires scope module or higher for shared event loop
@pytest.mark.asyncio(scope="session")
@pytest.mark.parametrize("worker", ["bulk_fanout"], indirect=True)
async def test_run(hatchet: Hatchet, worker):
    run = hatchet.admin.run_workflow("BulkParent", {"n": 12})
    result = await run.result()
    assert len(result["spawn"]["results"]) == 12


# requires scope module or higher for shared event loop
@pytest.mark.asyncio(scope="session")
@pytest.mark.parametrize("worker", ["bulk_fanout"], indirect=True)
async def test_run2(hatchet: Hatchet, worker):
    run = hatchet.admin.run_workflow("BulkParent", {"n": 10})
    result = await run.result()
    assert len(result["spawn"]["results"]) == 10
