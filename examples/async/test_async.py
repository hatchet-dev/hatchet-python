import pytest

from hatchet_sdk import Hatchet


# requires scope module or higher for shared event loop
@pytest.mark.parametrize(
    "worker", ["poetry run python examples/async/worker.py"], indirect=True
)
@pytest.mark.asyncio(scope="session")
async def test_run(hatchet: Hatchet, worker):
    run = hatchet.admin.run_workflow("AsyncWorkflow", {})
    result = await run.result()
    assert result["step1"]["test"] == "test"


@pytest.mark.parametrize(
    "worker", ["poetry run python examples/async/worker.py"], indirect=True
)
@pytest.mark.asyncio(scope="session")
async def test_run_async(aiohatchet: Hatchet, worker):
    run = await aiohatchet.admin.aio.run_workflow("AsyncWorkflow", {})
    result = await run.result()
    assert result["step1"]["test"] == "test"
