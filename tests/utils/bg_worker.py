import subprocess
import time
import pytest

def fixture_bg_worker(command, startup_time=5):
    @pytest.fixture(scope="session", autouse=True)
    def fixture_background_hatchet_worker(request):
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

        def cleanup():
            proc.terminate()
            proc.wait(timeout=10)

        request.addfinalizer(cleanup)

        time.sleep(startup_time)
        yield

    return fixture_background_hatchet_worker