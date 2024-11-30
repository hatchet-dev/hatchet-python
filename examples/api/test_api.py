import pytest

from hatchet_sdk import Hatchet


# requires scope module or higher for shared event loop
## IMPORTANT: Worker needs to be set here to ensure at least one workflow exists
@pytest.mark.parametrize("worker", ["concurrency_limit_rr"], indirect=True)
@pytest.mark.asyncio(scope="session")
async def test_list_workflows(hatchet: Hatchet, worker):
    workflows = hatchet.rest.workflow_list()

    assert len(workflows.rows) != 0


# requires scope module or higher for shared event loop
@pytest.mark.parametrize("worker", ["concurrency_limit_rr"], indirect=True)
@pytest.mark.asyncio(scope="session")
async def test_async_list_workflows(aiohatchet: Hatchet, worker):
    workflows = await aiohatchet.rest.aio.workflow_list()

    assert len(workflows.rows) != 0
