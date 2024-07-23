import asyncio

import pytest


@pytest.fixture
def hatchet():
    from hatchet_sdk import Hatchet

    return Hatchet(debug=True)


def test_client(hatchet):
    assert hatchet
