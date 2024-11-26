import os
from typing import AsyncGenerator, Generator, cast

import pytest
import pytest_asyncio
from testcontainers.core.container import DockerContainer
from testcontainers.core.image import DockerImage
from testcontainers.core.waiting_utils import wait_for_logs

from hatchet_sdk import Hatchet

@pytest_asyncio.fixture()
async def aiohatchet() -> AsyncGenerator[Hatchet, None]:
    yield Hatchet(debug=True)


@pytest.fixture()
def hatchet() -> Hatchet:
    return Hatchet(debug=True)

@pytest.fixture()
def worker(request: pytest.FixtureRequest) -> Generator[DockerContainer, None, None]:
    command = cast(str, request.param)

    with DockerImage(path=".", tag="test-container:latest") as image:
        with DockerContainer(str(image)).with_env(
            "HATCHET_CLIENT_TOKEN", os.getenv("HATCHET_CLIENT_TOKEN")
        ).with_env(
            "HATCHET_CLIENT_NAMESPACE", os.getenv("HATCHET_CLIENT_NAMESPACE")
        ).with_command(
            command
        ) as container:
            wait_for_logs(container, "sending heartbeat", timeout=30)

            yield container
