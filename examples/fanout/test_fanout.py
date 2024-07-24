from hatchet_sdk import Hatchet
import pytest

from tests.utils import background_hatchet_worker



@pytest.fixture
def hatchet():
    return Hatchet(debug=True)

fixture_background_hatchet_worker = background_hatchet_worker(["poetry", "run", "worker"])

# requires scope module or higher for shared event loop
@pytest.mark.asyncio(scope="session")
async def test_run(hatchet):
    run = hatchet.admin.run_workflow("Parent", {"n": 2})
    result = await run.result()
    assert len(result["spawn"]["results"]) == 2


# requires scope module or higher for shared event loop
@pytest.mark.asyncio(scope="session")
async def test_run2(hatchet):
    run = hatchet.admin.run_workflow("Parent", {"n": 2})
    result = await run.result()
    assert len(result["spawn"]["results"]) == 2
