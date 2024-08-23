import pytest

from hatchet_sdk import Hatchet
from tests.utils.hatchet_client import hatchet_client_fixture

hatchet = hatchet_client_fixture()


# requires scope module or higher for shared event loop
@pytest.mark.asyncio(scope="session")
async def test_list_workflows(hatchet: Hatchet):
    list = hatchet.rest.workflow_list()

    assert len(list.rows) != 0


# requires scope module or higher for shared event loop
@pytest.mark.asyncio(scope="session")
async def test_async_list_workflows(hatchet: Hatchet):
    list = await hatchet.rest.aio.workflow_list()

    assert len(list.rows) != 0
