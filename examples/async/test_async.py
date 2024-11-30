import pytest

from hatchet_sdk import Hatchet
from tests.utils import fixture_bg_worker

worker = fixture_bg_worker(["poetry", "run", "async"])


# requires scope module or higher for shared event loop
@pytest.mark.asyncio(scope="session")
async def test_run(hatchet: Hatchet, worker):
    run = hatchet.admin.run_workflow("AsyncWorkflow", {})
    result = await run.result()
    assert result["step1"]["test"] == "test"


@pytest.mark.skip(reason="Skipping this test until we can dedicate more time to debug")
@pytest.mark.asyncio(scope="session")
async def test_run_async(aiohatchet: Hatchet, worker):
    run = await aiohatchet.admin.aio.run_workflow("AsyncWorkflow", {})
    result = await run.result()
    assert result["step1"]["test"] == "test"
