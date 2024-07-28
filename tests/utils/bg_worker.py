import subprocess
import time
from multiprocessing import process

import pytest


def fixture_bg_worker(command, startup_time=5):
    @pytest.fixture(scope="session", autouse=True)
    def fixture_background_hatchet_worker():
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if proc.returncode is not None:
            raise Exception("worker failed %d" % (proc.returncode))

        # sleep long enough to make sure we are up and running
        time.sleep(startup_time)

        yield

        proc.terminate()
        proc.wait()

    return fixture_background_hatchet_worker
