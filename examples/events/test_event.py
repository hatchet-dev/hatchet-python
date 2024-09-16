import pytest

from hatchet_sdk.hatchet import Hatchet
from tests.utils import hatchet_client_fixture

hatchet = hatchet_client_fixture()


# requires scope module or higher for shared event loop
@pytest.mark.asyncio(scope="session")
async def test_event_push(hatchet: Hatchet):
    e = hatchet.event.push("user:create", {"test": "test"})

    assert e.eventId is not None

@pytest.mark.asyncio(scope="session")
async def test_async_event_push(hatchet: Hatchet):
    e = await hatchet.event.async_push("user:create", {"test": "test"})

    assert e.eventId is not None