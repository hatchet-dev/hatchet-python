import pytest

from hatchet_sdk import Hatchet
from tests.utils import fixture_bg_worker

worker = fixture_bg_worker(["poetry", "run", "cancellation"])


# requires scope module or higher for shared event loop
@pytest.mark.asyncio(scope="session")
async def test_run(hatchet: Hatchet, worker):
    run = hatchet.admin.run_workflow("CancelWorkflow", {})
    result = await run.result()
    # TODO is this the expected result for a timed out run...
    assert result == {}
