import argparse
from collections import defaultdict
import csv
from itertools import tee
import json
import logging
import math
import os
import sys

import folium
import joblib
from shapely.geometry import shape, Point

SEASONS = ["winter", "spring", "summer", "autumn"]


def dump_geojson(data, path, **dump_kwargs):
    with open(path, "w") as f:
        json.dump(data, f, separators=(",", ":"), **dump_kwargs)


def _is_point_in_multigon(point, polygons):
    for polygon in polygons:
        if polygon.contains(point):
            return True
    return False


def _parse_sswi_row(row, karst_polygons):
    try:
        point, lat, lng, _, horizon, season, sswi, _ = row
    except ValueError:
        return
    if point.startswith("#"):
        return

    season = SEASONS[int(season) - 1]
    lat = float(lat)
    lng = float(lng)
    sswi = float(sswi)

    in_karst = _is_point_in_multigon(Point(lng, lat), karst_polygons)
    risk_level = 0
    if sswi < -1.4 and in_karst:
        risk_level = 3
    elif sswi < -1.4 and not in_karst:
        risk_level = 2
    elif sswi < -0.7:
        risk_level = 1

    return (horizon, season, {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [lng, lat],
        },
        "properties": {
            "riskLevel": risk_level,
            "sswi": sswi,
            "inKarst": in_karst,
        }
    })


def compute_risks(sswi_file, karst_file, n_jobs=-1, batch_size="auto", verbose=50, logger=None):
    if logger is not None:
        logger.info("Loading data...")
        sswi_file, sswi_file2 = tee(sswi_file)
        logger.info("Number of points:", sum(1 for line in sswi_file2))

    sswi_reader = csv.reader(sswi_file, delimiter=";")
    karst_polygons = []
    for feature in json.load(karst_file)["features"]:
        karst_polygons.append(shape(feature["geometry"]).buffer(0))

    if logger is not None:
        logger.info("Number of karstic polygons:", len(karst_polygons))
        logger.info("Parsing data...")

    points = joblib.Parallel(
        n_jobs=n_jobs,
        verbose=verbose,
        backend="multiprocessing",
        batch_size=batch_size,
    )(
        joblib.delayed(_parse_sswi_row)(row, karst_polygons)
        for row in sswi_reader
    )

    min_sswi = math.inf
    max_sswi = -math.inf
    features = defaultdict(  # Horizon
        lambda: defaultdict(list)  # Season
    )
    for point in points:
        if point is None:
            continue
        h, s, p = point
        features[h][s].append(p)
        min_sswi = min(p["properties"]["sswi"], min_sswi)
        max_sswi = max(p["properties"]["sswi"], max_sswi)

    geojson = defaultdict(dict)
    for horizon, horizon_data in features.items():
        for season, points in horizon_data.items():
            geojson[horizon][season] = {
                "type": "FeatureCollection",
                "features": points,
            }

    metadata = {
        "sswi": dict(min=min_sswi, max=max_sswi),
        "riskLevels": [
            "",
            "Limitations de tous les prélèvements d'eau",
            "Interdiction d'utiliser l'eau pour des usages non prioritaires",
            "Menaces de pénuries en eau potable"
        ],
    }

    return geojson, metadata


def filter_karst_by_type(f, type_=1, logger=None):
    if logger is not None:
        logger.info("Loading karst data")
    data = json.load(f)

    if logger is not None:
        logger.info("Filtering karst data")
    features = []
    for feature in data["features"]:
        if feature["properties"]["TypeZK"] == type_:
            features.append(feature)

    data["features"] = features
    return data


def display(risk, colorscale, karst=None, map_location=[46.4, 2.5], map_zoom=5):
    m = folium.Map(location=map_location, zoom_start=map_zoom)

    risk_layer = folium.FeatureGroup(name="Risk")
    for p in risk["features"]:
        color = colorscale[p["properties"]["riskLevel"]]
        lng, lat = p["geometry"]["coordinates"]
        folium.CircleMarker(
            location=[lat, lng],
            radius=0.5,
            fill=True,
            fill_color=color,
            color=color,
        ).add_to(risk_layer)
    risk_layer.add_to(m)

    if karst is not None:
        folium.GeoJson(karst, name="Karst").add_to(m)
        folium.LayerControl().add_to(m)

    return m
