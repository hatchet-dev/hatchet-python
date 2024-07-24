import subprocess
import time
from hatchet_sdk import Hatchet
import pytest


@pytest.fixture
def hatchet():
    return Hatchet(debug=True)

@pytest.fixture(scope="session", autouse=True)
def fixture_background_hatchet_worker():
    proc = subprocess.Popen(["poetry", "run", "worker"])

    # sleep long enough to make sure we are up and running
    # it would be nice to NOT do this, but we need to ensure the worker is running before we trigger any events
    time.sleep(5)

    yield

    proc.terminate()
    proc.wait()

# requires scope module or higher for shared event loop
@pytest.mark.asyncio(scope="session")
async def test_run(hatchet, ):
    run = hatchet.client.admin.run_workflow("Parent", {"n": 2})
    result = await run.result()
    assert len(result["spawn"]["results"]) == 2


# requires scope module or higher for shared event loop
@pytest.mark.asyncio(scope="session")
async def test_run2(hatchet):
    run = hatchet.client.admin.run_workflow("Parent", {"n": 2})
    result = await run.result()
    assert len(result["spawn"]["results"]) == 2
