import logging
import subprocess
import time

import psutil
import pytest


def fixture_bg_worker(command, startup_time=5):
    @pytest.fixture(scope="session", autouse=True)
    def fixture_background_hatchet_worker(request):
        logging.info(f"Starting background worker: {' '.join(command)}")
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        def cleanup():
            logging.info("Cleaning up background worker")
            parent = psutil.Process(proc.pid)
            children = parent.children(recursive=True)
            for child in children:
                child.terminate()
            parent.terminate()

            gone, alive = psutil.wait_procs([parent] + children, timeout=3)
            for p in alive:
                logging.warning(f"Force killing process {p.pid}")
                p.kill()

        request.addfinalizer(cleanup)

        # Check if the process is still running
        if proc.poll() is not None:
            raise Exception(
                f"Worker failed to start with return code {proc.returncode}"
            )

        # Wait for startup
        time.sleep(startup_time)

        # Log stdout and stderr
        def log_output(pipe, log_func):
            for line in iter(pipe.readline, b""):
                log_func(line.decode().strip())

        import threading

        threading.Thread(
            target=log_output, args=(proc.stdout, logging.info), daemon=True
        ).start()
        threading.Thread(
            target=log_output, args=(proc.stderr, logging.error), daemon=True
        ).start()

        return proc

    return fixture_background_hatchet_worker
