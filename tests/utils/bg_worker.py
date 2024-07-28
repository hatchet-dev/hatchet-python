import subprocess
import time
import pytest

def fixture_bg_worker(command, startup_time=5):
    @pytest.fixture(scope="session", autouse=True)
    def fixture_background_hatchet_worker():
        proc = subprocess.Popen(command)

        # sleep long enough to make sure we are up and running
        time.sleep(startup_time)

        yield

        proc.terminate()
        proc.wait()

    return fixture_background_hatchet_worker