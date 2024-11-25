import pytest

from hatchet_sdk import Hatchet
from tests.utils import fixture_bg_worker

worker = fixture_bg_worker(["poetry", "run", "bulk_fanout"])


# requires scope module or higher for shared event loop
@pytest.mark.asyncio(scope="session")
async def test_run(hatchet: Hatchet):
    run = hatchet.admin.run_workflow("BulkParent", {"n": 12})
    result = await run.result()
    print(result)
    assert len(result["spawn"]["results"]) == 12


# requires scope module or higher for shared event loop
@pytest.mark.asyncio(scope="session")
async def test_run2(hatchet: Hatchet):
    run = hatchet.admin.run_workflow("BulkParent", {"n": 10})
    result = await run.result()
    assert len(result["spawn"]["results"]) == 10
