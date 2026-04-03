import os

import pytest


@pytest.fixture(scope="session")
def docker_compose_file():
    return os.path.join(os.path.dirname(__file__), "..", "docker-compose.yml")


@pytest.fixture(scope="session")
def valhalla_url(docker_services):
    port = docker_services.port_for("valhalla", 8002)
    url = f"http://localhost:{port}"
    docker_services.wait_until_responsive(
        check=lambda: _is_responsive(url), timeout=30, pause=1
    )
    return url


def _is_responsive(url):
    import requests

    try:
        resp = requests.get(f"{url}/status", timeout=2)
        return resp.status_code == 200
    except requests.ConnectionError:
        return False
