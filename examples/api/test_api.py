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
    hatchet_async_rest_client = await hatchet.get_async_rest_client()
    list = await hatchet_async_rest_client.workflow_list()

    assert len(list.rows) != 0
