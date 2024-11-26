import pytest
from hatchet_sdk import Hatchet


@pytest.mark.parametrize(
    "worker", ["poetry run python examples/async/worker.py"], indirect=True
)
async def test_run(hatchet: Hatchet, worker):
    run = hatchet.admin.run_workflow("AsyncWorkflow", {})
    result = await run.result()
    assert result["step1"]["test"] == "test"

@pytest.mark.parametrize(
    "worker", ["poetry run python examples/async/worker.py"], indirect=True
)
async def test_run_async(aiohatchet: Hatchet, worker):
    run = await aiohatchet.admin.aio.run_workflow("AsyncWorkflow", {})
    result = await run.result()
    assert result["step1"]["test"] == "test"

