import pytest

from hatchet_sdk import Hatchet
from tests.utils import fixture_bg_worker

worker = fixture_bg_worker(["poetry", "run", "concurrency_limit_rr"])

# requires scope module or higher for shared event loop
@pytest.mark.asyncio(scope="session")
async def test_list_workflows(hatchet: Hatchet, worker):
    workflows = hatchet.rest.workflow_list()

    assert len(workflows.rows) != 0


# requires scope module or higher for shared event loop
@pytest.mark.asyncio(scope="session")
async def test_async_list_workflows(aiohatchet: Hatchet, worker):
    workflows = await aiohatchet.rest.aio.workflow_list()

    assert len(workflows.rows) != 0
