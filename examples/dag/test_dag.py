import pytest

from hatchet_sdk import Hatchet
from tests.utils import fixture_bg_worker
from tests.utils.hatchet_client import hatchet_client_fixture

hatchet = hatchet_client_fixture()
worker = fixture_bg_worker(["poetry", "run", "dag"])


# requires scope module or higher for shared event loop
@pytest.mark.asyncio(scope="session")
async def test_run(hatchet: Hatchet):
    run = hatchet.admin.run_workflow("DagWorkflow", {})
    result = await run.result()

    one = result["step1"]["rando"]
    two = result["step2"]["rando"]
    assert result["step3"]["sum"] == one + two
    assert result["step4"]["step4"] == "step4"
