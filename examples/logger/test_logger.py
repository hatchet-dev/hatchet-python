import pytest

from hatchet_sdk import Hatchet
from tests.utils import fixture_bg_worker

worker = fixture_bg_worker(["poetry", "run", "logger"])


# requires scope module or higher for shared event loop
@pytest.mark.asyncio(scope="session")
async def test_run(hatchet: Hatchet):
    run = hatchet.admin.run_workflow("LoggingWorkflow", {})
    result = await run.result()
    assert result["step1"]["status"] == "success"
