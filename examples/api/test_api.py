import pytest

from hatchet_sdk import Hatchet


# requires scope module or higher for shared event loop
@pytest.mark.asyncio()
async def test_list_workflows(hatchet: Hatchet):
    list = hatchet.rest.workflow_list()

    assert len(list.rows) != 0


# requires scope module or higher for shared event loop
@pytest.mark.asyncio()
async def test_async_list_workflows(aiohatchet: Hatchet):
    list = await aiohatchet.rest.aio.workflow_list()

    assert len(list.rows) != 0
