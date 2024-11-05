import pytest
import pytest_asyncio

from typing import AsyncGenerator

from hatchet_sdk import Hatchet


@pytest_asyncio.fixture(scope="session")
async def aiohatchet() -> AsyncGenerator[Hatchet, None]:
    yield Hatchet(debug=True)

@pytest.fixture(scope="session")
def hatchet() -> Hatchet:
    return Hatchet(debug=True)
