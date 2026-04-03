"""Test pedestrian routing in Copenhagen against a running Valhalla service."""

import requests


def test_status(valhalla_url):
    resp = requests.get(f"{valhalla_url}/status")
    resp.raise_for_status()
    result = resp.json()
    assert "version" in result
    assert "tileset_last_modified" in result


def test_pedestrian_route(valhalla_url):
    resp = requests.post(
        f"{valhalla_url}/route",
        json={
            "locations": [
                {"lat": 55.676, "lon": 12.568},
                {"lat": 55.683, "lon": 12.572},
            ],
            "costing": "pedestrian",
        },
    )
    resp.raise_for_status()
    result = resp.json()

    assert "trip" in result, f"No trip in response: {result}"
    leg = result["trip"]["legs"][0]
    km = leg["summary"]["length"]

    assert km > 0.5, f"Route too short: {km} km"
    assert km < 5.0, f"Route too long: {km} km"
    assert leg["summary"]["time"] > 0
    assert len(leg["maneuvers"]) > 2


def test_reverse_route(valhalla_url):
    resp = requests.post(
        f"{valhalla_url}/route",
        json={
            "locations": [
                {"lat": 55.683, "lon": 12.572},
                {"lat": 55.676, "lon": 12.568},
            ],
            "costing": "pedestrian",
        },
    )
    resp.raise_for_status()
    leg = resp.json()["trip"]["legs"][0]
    assert leg["summary"]["length"] > 0
