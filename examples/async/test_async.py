import pytest

from hatchet_sdk import Hatchet
from tests.utils import fixture_bg_worker
from tests.utils.hatchet_client import hatchet_client_fixture

hatchet = hatchet_client_fixture()
worker = fixture_bg_worker(["poetry", "run", "async"])


# requires scope module or higher for shared event loop
@pytest.mark.asyncio(scope="session")
async def test_run(hatchet: Hatchet):
    run = hatchet.admin.run_workflow("AsyncWorkflow", {})
    result = await run.result()
    assert result["step1"]["test"] == "test"


@pytest.mark.asyncio(scope="session")
async def test_run_async(hatchet: Hatchet):
    run = await hatchet.admin.aio.run_workflow("AsyncWorkflow", {})
    result = await run.result()
    assert result["step1"]["test"] == "test"
