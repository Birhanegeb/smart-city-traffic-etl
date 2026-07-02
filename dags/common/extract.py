from __future__ import annotations

import requests
from .config import TOMTOM_API_KEY, TOMTOM_URL


# ---------------------------
# GRID GENERATION (OK in helper)
# ---------------------------
def build_grid(bbox: tuple, steps: int):
    min_lon, min_lat, max_lon, max_lat = bbox
    lon_step = (max_lon - min_lon) / (steps - 1)
    lat_step = (max_lat - min_lat) / (steps - 1)

    points = []
    for i in range(steps):
        for j in range(steps):
            lat = round(min_lat + j * lat_step, 6)
            lon = round(min_lon + i * lon_step, 6)
            points.append((lat, lon))
    return points


# ---------------------------
# API CALL ONLY (BRONZE)
# ---------------------------
def fetch_flow_segment(lat: float, lon: float):
    params = {
        "key": TOMTOM_API_KEY,
        "point": f"{lat},{lon}",
        "unit": "KMPH",
    }
    resp = requests.get(TOMTOM_URL, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()

INCIDENTS_URL = "https://api.tomtom.com/traffic/services/5/incidentDetails"

import urllib.request
import json as json_lib

def fetch_incidents(bbox: tuple):
    min_lon, min_lat, max_lon, max_lat = bbox
    fields = "{incidents{type,geometry{type,coordinates},properties{iconCategory,startTime,endTime,from,to,delay,roadNumbers}}}"
    url = (
        f"{INCIDENTS_URL}"
        f"?key={TOMTOM_API_KEY}"
        f"&bbox={min_lon},{min_lat},{max_lon},{max_lat}"
        f"&fields={fields}"
        f"&language=en-GB"
        f"&timeValidityFilter=present"
    )
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json_lib.loads(resp.read().decode())