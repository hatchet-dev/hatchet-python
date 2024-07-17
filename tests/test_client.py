import asyncio
import pytest

@pytest.fixture
def hatchet():
    from hatchet_sdk import Hatchet
    return Hatchet(debug=True)

def test_client(hatchet):
    assert hatchet

# requires scope module or higher for shared event loop
@pytest.mark.asyncio(scope="session")
async def test_listen(hatchet):
    run = hatchet.client.admin.get_workflow_run("839e089c-6708-4708-ae79-bad47c832580")
    result = await run.result()
    print(result)
    assert result

@pytest.mark.asyncio(scope="session")
async def test_listen2(hatchet):
    run = hatchet.client.admin.get_workflow_run("839e089c-6708-4708-ae79-bad47c832580")
    result = await run.result()
    print(result)
    assert result