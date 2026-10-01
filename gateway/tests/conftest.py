import os
import shutil
import socket
import subprocess
import time

import pytest


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def redis_url():
    """Use REDIS_URL if set (CI service container), else start a throwaway redis-server."""
    if os.getenv("REDIS_URL"):
        yield os.environ["REDIS_URL"]
        return
    binary = shutil.which("redis-server")
    if binary is None:
        pytest.skip("needs REDIS_URL or a local redis-server")
    port = _free_port()
    proc = subprocess.Popen([binary, "--port", str(port), "--save", "", "--appendonly", "no"],
                            stdout=subprocess.DEVNULL)
    for _ in range(50):
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.1).close()
            break
        except OSError:
            time.sleep(0.1)
    yield f"redis://127.0.0.1:{port}/0"
    proc.terminate()
    proc.wait()
