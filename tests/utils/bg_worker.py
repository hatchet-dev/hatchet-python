import logging
import subprocess
from typing import Callable
import psutil
import pytest
from io import BytesIO
from threading import Thread


def fixture_bg_worker(command: list[str]):
    @pytest.fixture()
    def worker():
        logging.info(f"Starting background worker: {' '.join(command)}")
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Check if the process is still running
        if proc.poll() is not None:
            raise Exception(f"Worker failed to start with return code {proc.returncode}")

        def wait_for_log_message(pipe: BytesIO) -> None:
            for line in iter(pipe.readline, b""):
                log_line = line.decode().strip()
                logging.info(log_line)  # Log the output
                if "sending heartbeat" in log_line:
                    logging.info(f"Found target log message: {log_line}")
                    break

        Thread(target=wait_for_log_message, args=(proc.stdout,), daemon=True).start()

        def log_output(pipe: BytesIO, log_func: Callable[[str], None]) -> None:
            for line in iter(pipe.readline, b""):
                log_func(line.decode().strip())

        Thread(target=log_output, args=(proc.stdout, logging.info), daemon=True).start()
        Thread(target=log_output, args=(proc.stderr, logging.error), daemon=True).start()

        yield proc

        logging.info("Cleaning up background worker")
        parent = psutil.Process(proc.pid)
        children = parent.children(recursive=True)
        for child in children:
            child.terminate()
        parent.terminate()

        _, alive = psutil.wait_procs([parent] + children, timeout=3)
        for p in alive:
            logging.warning(f"Force killing process {p.pid}")
            p.kill()

    return worker