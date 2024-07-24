import asyncio
import time

import pytest

from hatchet_sdk import Hatchet
from tests.utils import fixture_bg_worker
from tests.utils.hatchet_client import hatchet_client_fixture

hatchet = hatchet_client_fixture()
worker = fixture_bg_worker(["poetry", "run", "rate_limit"])


# requires scope module or higher for shared event loop
@pytest.mark.skip(reason="The timing for this test is not reliable")
@pytest.mark.asyncio(scope="session")
async def test_run(hatchet: Hatchet):

    run1 = hatchet.admin.run_workflow("RateLimitWorkflow", {})
    run2 = hatchet.admin.run_workflow("RateLimitWorkflow", {})
    run3 = hatchet.admin.run_workflow("RateLimitWorkflow", {})

    start_time = time.time()

    await asyncio.gather(run1.result(), run2.result(), run3.result())

    end_time = time.time()

    total_time = end_time - start_time

    assert (
        1 <= total_time <= 2
    ), f"Expected runtime to be a bit more than 1 seconds, but it took {total_time:.2f} seconds"
