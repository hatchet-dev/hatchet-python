import pytest

from hatchet_sdk import Hatchet
from tests.utils import fixture_bg_worker
from tests.utils.hatchet_client import hatchet_client_fixture

hatchet = hatchet_client_fixture()
worker = fixture_bg_worker(["poetry", "run", "fanout"])


# requires scope module or higher for shared event loop
@pytest.mark.asyncio(scope="session")
async def test_run(hatchet: Hatchet):
    run = hatchet.admin.run_workflow("Parent", {"n": 2})
    result = await run.result()
    assert len(result["spawn"]["results"]) == 2


# requires scope module or higher for shared event loop
@pytest.mark.asyncio(scope="session")
async def test_run2(hatchet: Hatchet):
    run = hatchet.admin.run_workflow("Parent", {"n": 2})
    result = await run.result()
    assert len(result["spawn"]["results"]) == 2
