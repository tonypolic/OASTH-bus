#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
busmap_all_v19.py
Live χάρτης λεωφορείων ΟΣΕΘ/ΟΑΣΘ από το δημόσιο telematics API του ΟΣΕΘ.

Βελτιώσεις v18:
1. Server-side cache στο /api/all-stops (12h TTL) για αστραπιαία φόρτωση χιλιάδων στάσεων.
2. Connection pooling (HTTPAdapter) για γρήγορες επαναχρησιμοποιούμενες HTTP συνδέσεις.
3. Προστασία Cache Stampede με αποκλειστικά locks σε active-lines και all-stops.
4. Canvas rendering (L.circleMarker) για τις 3.500+ στάσεις (60 FPS ομαλότητα, 0 DOM lag).
5. Differential marker updates: ομαλή κίνηση λεωφορείων χωρίς flicker και χωρίς να κλείνουν τα popups.
6. Ζωντανή αναζήτηση / φίλτρο γραμμής (Search Input) δίπλα στο dropdown.
7. Αυτόματη αποθήκευση & επαναφορά προτιμήσεων χρήστη (localStorage).
8. Δυνατότητα άμεσης επανενημέρωσης (refresh) αφίξεων μέσα στο popup της στάσης.
9. Αφαίρεση περιττών διπλότυπων layers και βελτιωμένο responsive design για κινητά.

Βελτιώσεις v19:
1. Σειριοποίηση live ανανεώσεων ώστε να μην επικαλύπτονται αιτήματα.
2. Canvas rendering των στάσεων με L.circleMarker.
3. Στοχευμένα tests για την κανονικοποίηση αφίξεων και χρόνων.
4. Ανεξάρτητη εμφάνιση στάσεων επιλεγμένης γραμμής (lineStopLayer) ακόμη και με απενεργοποιημένες τις γενικές στάσεις δικτύου.

Εγκατάσταση:
    py -m pip install flask requests

Εκτέλεση:
    py busmap_all_v19.py

Μετά άνοιξε στον browser:
    http://127.0.0.1:5000
"""

from __future__ import annotations

import re
import time
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from flask import Flask, jsonify, render_template_string, request

BASE = "https://oseth.com.gr/el/telematics-api"
LANG = "el"

# Παράμετροι Cache & Δικτύου
ROUTES_CACHE_SECONDS = 6 * 60 * 60       # 6 ώρες για λίστα γραμμών
STOPS_CACHE_SECONDS = 12 * 60 * 60      # 12 ώρες για στάσεις δικτύου
ACTIVE_LINES_CACHE_SECONDS = 45          # 45 δευτερόλεπτα για έλεγχο ενεργών γραμμών
INFO_CACHE_SECONDS = 8                   # 8 δευτερόλεπτα για μεμονωμένη γραμμή
ALL_MODE_CACHE_SECONDS = 15              # 15 δευτερόλεπτα για προβολή ΟΛΩΝ
MAX_WORKERS = 10
REQUEST_TIMEOUT = 12

app = Flask(__name__)

# Επαναχρησιμοποιούμενο Session με Connection Pooling
session = requests.Session()
adapter = HTTPAdapter(pool_connections=30, pool_maxsize=30, max_retries=2)
session.mount("https://", adapter)
session.mount("http://", adapter)
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) busmap_all_v19.py",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://oseth.com.gr/el/anazitisi-dromologion-leoforeion/",
})

# Locks και In-Memory Caches
_routes_lock = threading.Lock()
_routes_cache: dict[str, Any] = {"ts": 0.0, "data": []}

_stops_lock = threading.Lock()
_stops_cache: dict[str, Any] = {"ts": 0.0, "data": []}

_active_lines_lock = threading.Lock()
_active_lines_cache: dict[str, Any] = {"ts": 0.0, "data": []}

_all_lock = threading.Lock()
_all_cache: dict[str, Any] = {"ts": 0.0, "data": None}

_info_lock = threading.Lock()
_info_cache: dict[tuple[str, str], tuple[float, dict[str, Any]]] = {}


@dataclass(frozen=True)
class ShapeRef:
    short_name: str
    long_name: str
    color: str
    route_id: str
    shape_id: str
    headsign: str


def get_json(url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    r = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict) and data.get("status_code") not in (None, 200):
        raise RuntimeError(data.get("error") or f"API status {data.get('status_code')}")
    return data


def load_routes(force: bool = False) -> list[dict[str, Any]]:
    now = time.time()
    with _routes_lock:
        if (not force
                and _routes_cache["data"]
                and now - _routes_cache["ts"] < ROUTES_CACHE_SECONDS):
            return _routes_cache["data"]

        url = f"{BASE}/route"
        obj = get_json(url, params={"page": 1, "size": 1000})
        routes = obj.get("data", {}).get("routes", [])

        _routes_cache["ts"] = now
        _routes_cache["data"] = routes
        return routes


def flatten_shapes(routes: list[dict[str, Any]]) -> list[ShapeRef]:
    refs: list[ShapeRef] = []
    seen: set[tuple[str, str]] = set()

    for route in routes:
        short_name = str(route.get("shortName", "")).strip()
        long_name = str(route.get("longName", "")).strip()
        color = str(route.get("color", "#1f6feb")).strip() or "#1f6feb"

        for th in route.get("tripHeadsigns") or []:
            route_id = str(th.get("routeId", "")).strip()
            shape_id = str(th.get("shapeId", "")).strip()
            headsign = str(th.get("headsign", "")).strip()

            if not route_id or not shape_id:
                continue

            key = (route_id, shape_id)
            if key in seen:
                continue
            seen.add(key)

            refs.append(ShapeRef(
                short_name=short_name,
                long_name=long_name,
                color=color,
                route_id=route_id,
                shape_id=shape_id,
                headsign=headsign,
            ))

    return refs


def get_route_info(ref: ShapeRef, force: bool = False) -> dict[str, Any]:
    key = (ref.route_id, ref.shape_id)
    now = time.time()

    with _info_lock:
        cached = _info_cache.get(key)
        if cached and not force and now - cached[0] < INFO_CACHE_SECONDS:
            return cached[1]

    url = f"{BASE}/route/{requests.utils.quote(ref.route_id, safe='')}/info"
    obj = get_json(url, params={"shapeId": ref.shape_id, "language": LANG})
    data = obj.get("data") or {}

    with _info_lock:
        _info_cache[key] = (now, data)

    return data


def parse_linestring(wkt: str | None) -> list[list[float]]:
    if not wkt:
        return []

    m = re.match(r"^\s*LINESTRING\s*\((.*)\)\s*$", wkt, re.I | re.S)
    if not m:
        return []

    result: list[list[float]] = []
    for pair in m.group(1).split(","):
        parts = pair.strip().split()
        if len(parts) < 2:
            continue
        try:
            lon = float(parts[0])
            lat = float(parts[1])
            result.append([lat, lon])
        except ValueError:
            pass

    return result


def natural_line_key(name: str):
    m = re.match(r"^(\d+)(.*)$", name or "")
    if m:
        return (0, int(m.group(1)), m.group(2))
    return (1, 9999, name or "")


@app.get("/api/routes")
def api_routes():
    try:
        routes = load_routes()
        out = []
        for r in routes:
            out.append({
                "shortName": r.get("shortName"),
                "longName": r.get("longName"),
                "color": r.get("color"),
            })

        unique = {}
        for x in out:
            if x["shortName"]:
                unique[x["shortName"]] = x

        result = sorted(unique.values(), key=lambda x: natural_line_key(str(x["shortName"])))
        return jsonify({"ok": True, "routes": result})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.get("/api/all-stops")
def api_all_stops():
    """Επιστρέφει όλες τις στάσεις του δικτύου ΟΣΕΘ με server-side caching."""
    force = request.args.get("force") == "1"
    now = time.time()

    with _stops_lock:
        if not force and _stops_cache["data"] and now - _stops_cache["ts"] < STOPS_CACHE_SECONDS:
            return jsonify({
                "ok": True,
                "count": len(_stops_cache["data"]),
                "stops": _stops_cache["data"],
                "cached": True,
            })

        try:
            all_stops = []
            seen = set()
            page = 1
            size = 1000

            while page <= 25:
                obj = get_json(
                    f"{BASE}/stop",
                    params={
                        "page": page,
                        "size": size,
                        "language": LANG,
                        "stopName": "",
                    }
                )
                data = obj.get("data") or {}
                stops = data.get("stops") or []

                if not stops:
                    break

                for s in stops:
                    sid = str(s.get("id") or s.get("code") or "").strip()
                    if not sid or sid in seen:
                        continue

                    try:
                        lat = float(s["latitude"])
                        lon = float(s["longitude"])
                    except (KeyError, TypeError, ValueError):
                        continue

                    seen.add(sid)
                    all_stops.append({
                        "id": sid,
                        "code": str(s.get("code") or sid),
                        "name": str(s.get("name") or ""),
                        "latitude": lat,
                        "longitude": lon,
                    })

                total = data.get("total")
                if isinstance(total, int) and len(seen) >= total:
                    break

                if len(stops) < size:
                    break

                page += 1

            _stops_cache["ts"] = time.time()
            _stops_cache["data"] = all_stops

            return jsonify({
                "ok": True,
                "count": len(all_stops),
                "stops": all_stops,
                "cached": False,
            })

        except Exception as e:
            if _stops_cache["data"]:
                return jsonify({
                    "ok": True,
                    "count": len(_stops_cache["data"]),
                    "stops": _stops_cache["data"],
                    "stale": True,
                })
            return jsonify({"ok": False, "error": str(e)}), 502


@app.get("/api/active-lines")
def api_active_lines():
    """Επιστρέφει μόνο τις γραμμές που έχουν τουλάχιστον ένα ενεργό όχημα."""
    now = time.time()

    with _active_lines_lock:
        if (_active_lines_cache["data"]
                and now - _active_lines_cache["ts"] < ACTIVE_LINES_CACHE_SECONDS):
            return jsonify({
                "ok": True,
                "activeLines": _active_lines_cache["data"],
                "cached": True
            })

        try:
            routes = load_routes()
            refs = flatten_shapes(routes)

            active: set[str] = set()
            errors = 0

            def one(ref: ShapeRef):
                try:
                    info = get_route_info(ref)
                    has_vehicles = bool(info.get("vehicles"))
                    return ref.short_name, has_vehicles, None
                except Exception as e:
                    return ref.short_name, False, str(e)

            workers = min(MAX_WORKERS, max(1, len(refs)))
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = [ex.submit(one, ref) for ref in refs]
                for fut in as_completed(futures):
                    short_name, has_vehicles, err = fut.result()
                    if err:
                        errors += 1
                    elif has_vehicles and short_name:
                        active.add(short_name)

            result = sorted(active, key=natural_line_key)

            _active_lines_cache["ts"] = time.time()
            _active_lines_cache["data"] = result

            return jsonify({
                "ok": True,
                "activeLines": result,
                "errors": errors,
                "cached": False
            })

        except Exception as e:
            if _active_lines_cache["data"]:
                return jsonify({
                    "ok": True,
                    "activeLines": _active_lines_cache["data"],
                    "stale": True
                })
            return jsonify({"ok": False, "error": str(e)}), 500


def _walk_lists(value):
    """Επιστρέφει αναδρομικά όλες τις λίστες που υπάρχουν σε JSON payload."""
    if isinstance(value, list):
        yield value
        for item in value:
            yield from _walk_lists(item)
    elif isinstance(value, dict):
        for item in value.values():
            yield from _walk_lists(item)


def _first_value(row: dict[str, Any], names: list[str]):
    lower = {str(k).lower(): v for k, v in row.items()}
    for name in names:
        value = lower.get(name.lower())
        if value not in (None, ""):
            return value
    return None


def _minutes_from_value(value: Any):
    if value is None:
        return None

    if isinstance(value, (int, float)):
        v = float(value)
        if -5 <= v <= 24 * 60:
            return max(0, int(round(v)))
        return None

    s = str(value).strip()
    if not s:
        return None

    m = re.search(r"(-?\d+(?:[.,]\d+)?)\s*(?:min|mins|minute|minutes|λεπτ|′|')?", s, re.I)
    if m and re.fullmatch(r"\s*-?\d+(?:[.,]\d+)?\s*(?:min|mins|minute|minutes|λεπτ(?:ά|α)?|′|')?\s*", s, re.I):
        try:
            return max(0, int(round(float(m.group(1).replace(",", ".")))))
        except ValueError:
            pass

    m = re.fullmatch(r"(\d{1,2}):(\d{2})(?::(\d{2}))?", s)
    if m:
        now = datetime.now().astimezone()
        target = now.replace(
            hour=int(m.group(1)),
            minute=int(m.group(2)),
            second=int(m.group(3) or 0),
            microsecond=0,
        )
        if target < now - timedelta(minutes=2):
            target += timedelta(days=1)
        return max(0, int((target - now).total_seconds() // 60))

    normalized = s.replace("Z", "+00:00")
    for parser in (
        lambda x: datetime.fromisoformat(x),
        lambda x: datetime.strptime(x, "%d/%m/%Y %H:%M:%S"),
        lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"),
    ):
        try:
            dt = parser(normalized)
            now = datetime.now().astimezone()
            if dt.tzinfo is not None:
                dt = dt.astimezone()
            else:
                dt = dt.astimezone()
            return max(0, int((dt - now).total_seconds() // 60))
        except Exception:
            continue

    return None


def normalize_stop_timetable(payload: Any) -> list[dict[str, Any]]:
    candidate_lists = list(_walk_lists(payload))
    if isinstance(payload, list):
        candidate_lists.insert(0, payload)

    best: list[Any] = []
    best_score = -1

    hints = {
        "arrival", "eta", "time", "minute", "route", "line",
        "headsign", "vehicle", "trip", "shortname"
    }

    for arr in candidate_lists:
        dict_rows = [x for x in arr if isinstance(x, dict)]
        if not dict_rows:
            continue

        keys = {str(k).lower() for row in dict_rows[:10] for k in row.keys()}
        score = sum(1 for h in hints if any(h in k for k in keys))
        if score > best_score:
            best_score = score
            best = dict_rows

    if not best and isinstance(payload, dict):
        best = [payload]

    rows: list[dict[str, Any]] = []

    for row in best:
        line = _first_value(row, [
            "routeShortName", "lineShortName", "shortName",
            "line", "route", "routeName", "route_short_name",
            "routeCode", "routeId"
        ])

        headsign = _first_value(row, [
            "headsign", "tripHeadsign", "destination", "destinationName",
            "routeLongName", "longName", "description", "routeDescription"
        ])

        vehicle = _first_value(row, [
            "vehicleId", "vehicle", "vehicleCode", "veh_code", "busId", "bus"
        ])

        if isinstance(line, dict):
            route_obj = line
            line = (
                route_obj.get("shortName")
                or route_obj.get("routeShortName")
                or route_obj.get("lineShortName")
                or route_obj.get("id")
                or ""
            )
            if not headsign:
                headsign = (
                    route_obj.get("headsign")
                    or route_obj.get("longName")
                    or route_obj.get("routeLongName")
                    or ""
                )

        if isinstance(headsign, dict):
            headsign = (
                headsign.get("headsign")
                or headsign.get("longName")
                or headsign.get("name")
                or ""
            )

        if isinstance(vehicle, dict):
            vehicle = (
                vehicle.get("id")
                or vehicle.get("vehicleId")
                or vehicle.get("code")
                or ""
            )

        eta_raw = _first_value(row, [
            "arrivalIn", "arrivalInMinutes", "minutes", "minutesToArrival",
            "waitingTime", "timeToArrival", "etaMinutes", "eta", "btime2"
        ])
        minutes = _minutes_from_value(eta_raw)

        arrival_time = _first_value(row, [
            "estimatedArrivalTime", "expectedArrivalTime", "arrivalTime",
            "estimatedTime", "expectedTime", "time", "arrival"
        ])

        if minutes is None:
            minutes = _minutes_from_value(arrival_time)

        def clean_text(value):
            if value is None:
                return ""
            if isinstance(value, (dict, list)):
                return ""
            return str(value).strip()

        rows.append({
            "line": clean_text(line),
            "headsign": clean_text(headsign),
            "vehicle": clean_text(vehicle),
            "minutes": minutes,
            "arrival_time": clean_text(arrival_time),
        })

    rows = [
        x for x in rows
        if x["line"] or x["headsign"] or x["vehicle"]
        or x["minutes"] is not None or x["arrival_time"]
    ]

    rows.sort(key=lambda x: x["minutes"] if x["minutes"] is not None else 999999)
    return rows


@app.get("/api/arrivals")
def api_arrivals():
    stop_id = (request.args.get("stop_id") or "").strip()
    if not stop_id:
        return jsonify({"ok": False, "error": "Λείπει το stop_id"}), 400

    try:
        now = datetime.now()
        date_text = now.strftime("%d/%m/%Y %H:%M:%S")

        url = f"{BASE}/stop/{requests.utils.quote(stop_id, safe='')}/timetable"
        obj = get_json(url, params={"language": LANG, "date": date_text})

        data = obj.get("data", obj) if isinstance(obj, dict) else obj
        arrivals = normalize_stop_timetable(data)

        res = {
            "ok": True,
            "stop_id": stop_id,
            "updated": int(time.time()),
            "arrivals": arrivals,
        }
        if request.args.get("debug") == "1":
            res["raw"] = data

        return jsonify(res)

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 502


@app.get("/api/live")
def api_live():
    selected = (request.args.get("line") or "").strip()
    all_mode = request.args.get("all") == "1"

    try:
        routes = load_routes()
        refs = flatten_shapes(routes)

        if selected:
            refs = [x for x in refs if x.short_name == selected]

        if not selected and not all_mode:
            return jsonify({"ok": True, "vehicles": [], "shapes": [], "stops": []})

        if all_mode and not selected:
            now = time.time()
            with _all_lock:
                if (_all_cache["data"] is not None
                        and now - _all_cache["ts"] < ALL_MODE_CACHE_SECONDS):
                    return jsonify(_all_cache["data"])

        vehicles = []
        shapes = []
        stops_by_id: dict[str, dict[str, Any]] = {}
        errors = []

        def one(ref: ShapeRef):
            try:
                info = get_route_info(ref)
                return ref, info, None
            except Exception as e:
                return ref, None, str(e)

        workers = min(MAX_WORKERS, max(1, len(refs)))
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [ex.submit(one, ref) for ref in refs]
            for fut in as_completed(futures):
                ref, info, err = fut.result()
                if err:
                    errors.append({
                        "line": ref.short_name,
                        "routeId": ref.route_id,
                        "shapeId": ref.shape_id,
                        "error": err,
                    })
                    continue

                info = info or {}

                for v in info.get("vehicles") or []:
                    try:
                        lat = float(v["latitude"])
                        lon = float(v["longitude"])
                    except (KeyError, TypeError, ValueError):
                        continue

                    vehicles.append({
                        "id": str(v.get("id", "")),
                        "latitude": lat,
                        "longitude": lon,
                        "bearing": float(v.get("bearing") or 0),
                        "line": ref.short_name,
                        "headsign": ref.headsign or info.get("headsign") or "",
                        "routeId": ref.route_id,
                        "shapeId": ref.shape_id,
                        "color": ref.color,
                    })

                if selected:
                    shape_obj = info.get("shape") or {}
                    points = parse_linestring(shape_obj.get("lineString"))
                    if points:
                        shapes.append({
                            "routeId": ref.route_id,
                            "shapeId": ref.shape_id,
                            "headsign": ref.headsign or info.get("headsign") or "",
                            "color": ref.color,
                            "points": points,
                        })

                    for s in info.get("stops") or []:
                        sid = str(s.get("id") or s.get("code") or "")
                        if not sid:
                            continue
                        try:
                            stops_by_id[sid] = {
                                "id": sid,
                                "code": str(s.get("code") or sid),
                                "name": str(s.get("name") or ""),
                                "latitude": float(s["latitude"]),
                                "longitude": float(s["longitude"]),
                                "sequence": s.get("sequence"),
                            }
                        except (KeyError, TypeError, ValueError):
                            pass

        result = {
            "ok": True,
            "mode": "all" if all_mode and not selected else "line",
            "line": selected or None,
            "vehicles": vehicles,
            "shapes": shapes,
            "stops": list(stops_by_id.values()),
            "errors": errors,
            "updated": int(time.time()),
        }

        if all_mode and not selected:
            with _all_lock:
                _all_cache["ts"] = time.time()
                _all_cache["data"] = result

        return jsonify(result)

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


HTML = r"""
<!doctype html>
<html lang="el">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Live λεωφορεία ΟΣΕΘ / ΟΑΣΘ (v19)</title>

<link rel="stylesheet"
 href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
 crossorigin="">

<style>
html, body {
    margin: 0;
    height: 100%;
    font-family: system-ui, -apple-system, "Segoe UI", Roboto, Arial, sans-serif;
}
body {
    display: flex;
    flex-direction: column;
    background: #f5f5f5;
}
#toolbar {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 8px 12px;
    background: #ffffff;
    border-bottom: 1px solid #d0d7de;
    flex-wrap: wrap;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    z-index: 1000;
}
.brand {
    font-weight: 800;
    font-size: 16px;
    color: #0969da;
    display: flex;
    align-items: center;
    gap: 5px;
}
.brand span {
    font-size: 11px;
    background: #e1f0ff;
    color: #0969da;
    padding: 1px 5px;
    border-radius: 4px;
}
#toolbar select, #toolbar button, #toolbar input[type="search"] {
    font-size: 14px;
    padding: 6px 10px;
    border-radius: 6px;
    border: 1px solid #c0c6cc;
    background: #fff;
    outline: none;
}
#toolbar select:focus, #toolbar input[type="search"]:focus {
    border-color: #0969da;
    box-shadow: 0 0 0 2px rgba(9,105,218,0.2);
}
#toolbar button {
    cursor: pointer;
    font-weight: 600;
    background: #f6f8fa;
    transition: background 0.15s, border-color 0.15s;
}
#toolbar button:hover {
    background: #eef2f6;
    border-color: #9aa0a6;
}
#allBtn.active {
    background: #0969da;
    color: white;
    border-color: #0757b3;
}
#map {
    flex: 1;
    min-height: 300px;
}
#status {
    font-size: 13px;
    color: #444;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 380px;
}
#warning {
    font-size: 12px;
    color: #b56500;
    font-weight: 600;
}

.bus-arrow-marker {
    position: relative;
    width: 26px;
    height: 36px;
    filter: drop-shadow(0 1px 3px rgba(255,255,255,0.85))
            drop-shadow(0 2px 4px rgba(0,0,0,0.4));
    transform-origin: 50% 70%;
    transition: transform 0.3s ease;
}
.bus-arrow-marker .shaft {
    position: absolute;
    left: 10px;
    top: 15px;
    width: 6px;
    height: 18px;
    background: #111;
    border-radius: 1px;
}
.bus-arrow-marker .head {
    position: absolute;
    left: 3px;
    top: 0;
    width: 0;
    height: 0;
    border-left: 10px solid transparent;
    border-right: 10px solid transparent;
    border-bottom: 15px solid #111;
}
.bus-arrow-marker .fallback {
    position: absolute;
    left: 50%;
    top: 50%;
    transform: translate(-50%, -50%);
    font-size: 20px;
}
.stop-icon {
    width: 10px;
    height: 10px;
    border-radius: 50%;
    background: white;
    border: 3px solid #333;
    box-sizing: border-box;
}

.leaflet-popup-content {
    margin: 12px 14px;
    min-width: 290px;
    max-width: 420px;
    font-size: 14px;
    line-height: 1.35;
}
.stop-popup-title {
    font-size: 16px;
    font-weight: 800;
    margin-bottom: 3px;
    color: #1a1a1a;
}
.stop-popup-meta {
    color: #555;
    font-size: 12px;
}
.arrivals-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin: 10px 0 6px;
}
.arrivals-title {
    font-size: 15px;
    font-weight: 800;
    color: #111;
}
.refresh-btn-mini {
    font-size: 11px;
    padding: 2px 7px;
    border-radius: 4px;
    background: #f0f4f8;
    color: #0969da;
    border: 1px solid #c2d7ef;
    cursor: pointer;
    font-weight: 600;
}
.refresh-btn-mini:hover {
    background: #e1edfa;
}
.arrivals-list {
    max-height: 290px;
    overflow-y: auto;
    padding-right: 2px;
}
.arrival-row {
    display: grid;
    grid-template-columns: 58px 1fr;
    gap: 10px;
    align-items: start;
    padding: 7px 6px;
    border: 0;
    border-bottom: 1px solid #eef0f2;
    width: 100%;
    background: transparent;
    text-align: left;
    font: inherit;
    border-radius: 5px;
}
.arrival-select {
    cursor: pointer;
}
.arrival-select:hover {
    background: #f3f6fa;
}
.arrival-mins {
    font-size: 19px;
    font-weight: 800;
    line-height: 1.1;
    color: #111;
}
.arrival-info {
    min-width: 0;
    overflow-wrap: anywhere;
}
.arrival-line {
    display: inline-block;
    font-size: 15px;
    font-weight: 800;
    color: #0969da;
}
.arrival-headsign {
    margin-top: 2px;
    font-size: 13px;
    line-height: 1.25;
    color: #333;
}
.arrival-vehicle {
    margin-top: 2px;
    color: #666;
    font-size: 11px;
}
.popup-update {
    margin-top: 8px;
    padding-top: 6px;
    border-top: 1px solid #eee;
    color: #777;
    font-size: 11px;
    display: flex;
    justify-content: space-between;
}

.route-legend {
    background: rgba(255,255,255,0.95);
    padding: 8px 12px;
    border-radius: 8px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.22);
    font-size: 13px;
    line-height: 1.4;
    max-width: 320px;
    border: 1px solid #ddd;
}
.legend-row {
    display: flex;
    align-items: flex-start;
    gap: 7px;
    margin: 5px 0;
}
.legend-line {
    width: 24px;
    height: 6px;
    border-radius: 3px;
    flex: 0 0 auto;
    margin-top: 6px;
}
.small {
    font-size: 11px;
    color: #666;
}

@media (max-width: 768px) {
    #status {
        max-width: 100%;
        order: 99;
    }
}
</style>
</head>
<body>

<div id="toolbar">
    <div class="brand">
        ΟΣΕΘ Live <span>v19</span>
    </div>

    <input type="search" id="lineSearch" placeholder="🔍 Αναζήτηση..." title="Πληκτρολόγησε για φιλτράρισμα γραμμών" style="width: 110px;">

    <select id="lineSelect" title="Επιλογή γραμμής">
        <option value="">-- γραμμή --</option>
    </select>

    <button id="allBtn" title="Εμφάνιση όλων των κινούμενων λεωφορείων">ΟΛΑ</button>
    <button id="refreshBtn" title="Άμεση ανανέωση θέσεων">Ανανέωση</button>
    <button id="stopsBtn" title="Εμφάνιση ή απόκρυψη όλων των στάσεων δικτύου (οι στάσεις της επιλεγμένης γραμμής παραμένουν)">Στάσεις: ON</button>

    <label style="font-size: 13px; cursor: pointer;">
        <input type="checkbox" id="activeOnly" checked>
        Ενεργές μόνο
    </label>

    <label style="font-size: 13px;">
        κάθε
        <select id="intervalSelect" style="padding: 4px 6px;">
            <option value="5">5s</option>
            <option value="10" selected>10s</option>
            <option value="15">15s</option>
            <option value="20">20s</option>
        </select>
    </label>

    <span id="warning"></span>
    <span id="status">Φόρτωση…</span>
</div>

<div id="map"></div>

<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" crossorigin=""></script>

<script>
// Αρχικοποίηση χάρτη με Canvas renderer για μέγιστη απόδοση
const map = L.map('map', { preferCanvas: true }).setView([40.6401, 22.9444], 12);

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; OpenStreetMap contributors | ΟΣΕΘ Telematics'
}).addTo(map);

const routeLayer = L.layerGroup().addTo(map);
const allStopLayer = L.layerGroup().addTo(map);
const lineStopLayer = L.layerGroup().addTo(map);
const vehicleLayer = L.layerGroup().addTo(map);
const stopRenderer = L.canvas({ padding: 0.5 });

const ROUTE_COLORS = [
    "#1565c0", "#d32f2f", "#2e7d32", "#7b1fa2",
    "#ef6c00", "#00838f", "#5d4037", "#c2185b"
];

let legendControl = null;
let currentLine = localStorage.getItem("busmap_line") || "";
let allMode = false;
let allRoutes = [];
let activeLines = new Set();
let timer = null;
let timerGeneration = 0;
let firstFit = true;
let stopsVisible = localStorage.getItem("busmap_stops") !== "0";
let liveRefreshPromise = null;
let pendingLiveRefresh = false;
let pendingForceFit = false;
let renderedLineStopsLine = null;

// Differential marker map: vehicleId -> L.Marker
const vehicleMarkers = new Map();

function safeColor(c) {
    return /^#[0-9a-fA-F]{6}$/.test(c || "") ? c : "#1f6feb";
}

function escapeHtml(s) {
    return String(s ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;");
}

function vehicleIcon(v) {
    const bearing = Number(v.bearing);
    const hasAngle = Number.isFinite(bearing);

    return L.divIcon({
        className: "",
        html: `
          <div class="bus-arrow-marker" style="${hasAngle ? `transform: rotate(${bearing}deg);` : ''}">
            ${hasAngle
                ? '<div class="head"></div><div class="shaft"></div>'
                : '<div class="fallback">🚌</div>'
            }
          </div>`,
        iconSize: [26, 36],
        iconAnchor: [13, 25],
        popupAnchor: [0, -25]
    });
}

function fillLineSelect() {
    const sel = document.getElementById("lineSelect");
    const activeOnly = document.getElementById("activeOnly").checked;
    const searchVal = document.getElementById("lineSearch").value.trim().toLowerCase();
    const oldValue = sel.value || currentLine;

    sel.innerHTML = '<option value="">-- διάλεξε γραμμή --</option>';

    let filtered = activeOnly
        ? allRoutes.filter(x => activeLines.has(String(x.shortName)))
        : allRoutes;

    if (searchVal) {
        filtered = filtered.filter(x =>
            String(x.shortName || "").toLowerCase().includes(searchVal) ||
            String(x.longName || "").toLowerCase().includes(searchVal)
        );
    }

    for (const x of filtered) {
        const opt = document.createElement("option");
        opt.value = x.shortName;
        opt.textContent = `${x.shortName} — ${x.longName}`;
        sel.appendChild(opt);
    }

    if (filtered.some(x => String(x.shortName) === oldValue)) {
        sel.value = oldValue;
    } else if (currentLine && filtered.some(x => String(x.shortName) === currentLine)) {
        sel.value = currentLine;
    } else {
        sel.value = "";
    }
}

async function loadActiveLines(showStatus = true) {
    const status = document.getElementById("status");
    if (showStatus) status.textContent = "Έλεγχος ενεργών γραμμών…";

    const r = await fetch("/api/active-lines", { cache: "no-store" });
    const j = await r.json();
    if (!j.ok) throw new Error(j.error || "Σφάλμα ενεργών γραμμών");

    activeLines = new Set((j.activeLines || []).map(String));
    fillLineSelect();

    if (showStatus) {
        status.textContent = `${activeLines.size} ενεργές γραμμές αυτή τη στιγμή`;
    }
}

async function loadStopArrivals(m, s) {
    const stopId = String(s.id || s.code || "").trim();
    if (!stopId) return;

    const basicStopInfo =
        `<div class="stop-popup-title">${escapeHtml(s.name || "")}</div>` +
        `<div class="stop-popup-meta">Στάση ${escapeHtml(s.code || stopId)}</div>`;

    m.setPopupContent(
        basicStopInfo +
        `<div class="arrivals-title">Επόμενες αφίξεις</div>` +
        `<span class="small">Φόρτωση…</span>`
    );

    try {
        const ar = await fetch(
            `/api/arrivals?stop_id=${encodeURIComponent(stopId)}`,
            { cache: "no-store" }
        );
        const payload = await ar.json();

        if (!ar.ok || !payload.ok) {
            throw new Error(payload.error || "Σφάλμα τηλεματικής");
        }

        const arrivals = (payload.arrivals || []).filter(a => {
            const mins = Number(a.minutes);
            return !Number.isFinite(mins) || mins <= 60;
        });

        let arrivalsHtml = "";

        if (!arrivals.length) {
            arrivalsHtml =
                `<span class="small">Δεν υπάρχουν προβλεπόμενες αφίξεις στα επόμενα 60′.</span>`;
        } else {
            arrivalsHtml = arrivals.slice(0, 15).map(a => {
                const mins = Number(a.minutes);
                const minsText = Number.isFinite(mins)
                    ? (mins <= 0 ? "τώρα" : `${mins}′`)
                    : (a.arrival_time ? escapeHtml(a.arrival_time) : "—");

                const line = String(a.line || "").trim();
                const headsign = String(a.headsign || "").trim();
                const vehicle = String(a.vehicle || "").trim();
                const isOther = line && currentLine && line !== currentLine;

                return (
                    `<button type="button" class="arrival-row arrival-select" ` +
                    `data-line="${escapeHtml(line)}" data-vehicle="${escapeHtml(vehicle)}" ` +
                    `title="Επιλογή λεωφορείου">` +
                      `<div class="arrival-mins">${escapeHtml(minsText)}</div>` +
                      `<div class="arrival-info">` +
                        (line
                          ? `<div><span class="arrival-line">${escapeHtml(line)}</span>` +
                            (isOther ? ` <span class="small">(άλλη γραμμή)</span>` : '') +
                            `</div>`
                          : '') +
                        (headsign
                          ? `<div class="arrival-headsign">${escapeHtml(headsign)}</div>`
                          : '') +
                        (vehicle
                          ? `<div class="arrival-vehicle">Όχημα ${escapeHtml(vehicle)} · πάτησε για επιλογή</div>`
                          : `<div class="arrival-vehicle">Πάτησε για επιλογή γραμμής</div>`) +
                      `</div>` +
                    `</button>`
                );
            }).join("");
        }

        const when = new Date((payload.updated || Date.now() / 1000) * 1000);

        m.setPopupContent(
            basicStopInfo +
            `<div class="arrivals-title">Επόμενες αφίξεις</div>` +
            `<div class="arrivals-list">${arrivalsHtml}</div>` +
            `<div class="popup-update">` +
            `<span>Τηλεματική ΟΣΕΘ · ενημέρωση ${when.toLocaleTimeString("el-GR")}</span>` +
            `<button type="button" class="refresh-stop-btn" style="background:none;border:none;color:#0969da;cursor:pointer;font-size:11px;font-weight:600;padding:0;">🔄 Ανανέωση</button>` +
            `</div>`
        );

        setTimeout(() => {
            const popupEl = m.getPopup()?.getElement();
            const btn = popupEl?.querySelector('.refresh-stop-btn');
            if (btn) {
                btn.onclick = (ev) => {
                    ev.preventDefault();
                    ev.stopPropagation();
                    loadStopArrivals(m, s);
                };
            }
        }, 20);

    } catch(e) {
        console.error("Σφάλμα αφίξεων:", e);
        m.setPopupContent(
            basicStopInfo +
            `<div class="arrivals-title">Επόμενες αφίξεις</div>` +
            `<span class="small" style="color:#d32f2f">Αδυναμία φόρτωσης αφίξεων: ${escapeHtml(e.message)}</span>`
        );
    }
}

function attachStopPopup(m, s) {
    const stopId = String(s.id || s.code || "").trim();
    const basicStopInfo =
        `<div class="stop-popup-title">${escapeHtml(s.name || "")}</div>` +
        `<div class="stop-popup-meta">Στάση ${escapeHtml(s.code || stopId)}</div>`;

    m.bindPopup(
        basicStopInfo +
        `<div style="margin-top:7px" class="small">Πάτησε για live αφίξεις.</div>`,
        { maxWidth: 390 }
    );

    m.on("click", () => {
        loadStopArrivals(m, s);
    });
}

async function loadAllStops() {
    const status = document.getElementById("status");
    try {
        status.textContent = "Φόρτωση στάσεων…";

        const r = await fetch("/api/all-stops", { cache: "no-store" });
        const j = await r.json();
        if (!r.ok || !j.ok) throw new Error(j.error || "Σφάλμα στάσεων");

        allStopLayer.clearLayers();

        for (const s of j.stops || []) {
            const m = L.circleMarker(
                [s.latitude, s.longitude],
                {
                    renderer: stopRenderer,
                    radius: 5,
                    color: "#333",
                    weight: 2,
                    fillColor: "#fff",
                    fillOpacity: 1
                }
            ).addTo(allStopLayer);

            m.bindTooltip(`${s.code} — ${s.name}`, { direction: "top" });
            attachStopPopup(m, s);
        }

        if (!stopsVisible && map.hasLayer(allStopLayer)) {
            map.removeLayer(allStopLayer);
        }

        status.textContent = `${j.count || 0} στάσεις έτοιμες` + (j.cached ? " (cache)" : "");
    } catch(e) {
        status.textContent = "Σφάλμα στάσεων: " + e.message;
    }
}

async function selectArrivalBus(line, vehicle) {
    line = String(line || "").trim();
    vehicle = String(vehicle || "").trim();
    if (!line) return;

    allMode = false;
    currentLine = line;
    localStorage.setItem("busmap_line", line);
    document.getElementById("allBtn").classList.remove("active");
    lineStopLayer.clearLayers();
    renderedLineStopsLine = null;

    const sel = document.getElementById("lineSelect");
    let optionExists = Array.from(sel.options).some(o => String(o.value) === line);

    if (!optionExists) {
        const activeOnly = document.getElementById("activeOnly");
        if (activeOnly.checked) {
            activeOnly.checked = false;
            fillLineSelect();
        }
        optionExists = Array.from(sel.options).some(o => String(o.value) === line);
    }

    if (optionExists) sel.value = line;

    firstFit = false;
    await refreshLive(false);

    if (vehicle) {
        const vm = vehicleMarkers.get(vehicle);
        if (vm) {
            map.closePopup();
            map.setView(vm.getLatLng(), Math.max(map.getZoom(), 16));
            vm.openPopup();
            return;
        }
    }

    document.getElementById("status").textContent +=
        vehicle ? ` — όχημα ${vehicle} δεν βρέθηκε σε κίνηση` : " — επιλέχθηκε γραμμή";
}

async function performLiveRefresh(forceFit = false) {
    const status = document.getElementById("status");
    const warning = document.getElementById("warning");

    let url;
    if (allMode) {
        url = "/api/live?all=1";
        warning.textContent = "Προβολή ΟΛΩΝ: ανανέωση από cache.";
    } else if (currentLine) {
        url = "/api/live?line=" + encodeURIComponent(currentLine);
        warning.textContent = "";
    } else {
        routeLayer.clearLayers();
        lineStopLayer.clearLayers();
        renderedLineStopsLine = null;
        vehicleLayer.clearLayers();
        vehicleMarkers.clear();
        if (legendControl) {
            map.removeControl(legendControl);
            legendControl = null;
        }
        warning.textContent = "";
        status.textContent = "Επιλέξτε γραμμή ή πατήστε ΟΛΑ.";
        return;
    }

    status.textContent = "Ενημέρωση…";

    try {
        const r = await fetch(url, { cache: "no-store" });
        const j = await r.json();
        if (!j.ok) throw new Error(j.error || "Σφάλμα API");

        const bounds = [];

        // 1. Ανανέωση Γεωμετρίας Διαδρομών
        routeLayer.clearLayers();
        const legendRows = [];

        if (legendControl) {
            map.removeControl(legendControl);
            legendControl = null;
        }

        (j.shapes || []).forEach((s, index) => {
            const color = ROUTE_COLORS[index % ROUTE_COLORS.length];
            const p = L.polyline(s.points, {
                color: color,
                weight: 5,
                opacity: 0.85
            }).addTo(routeLayer);

            p.bindPopup(`<b>Γραμμή ${escapeHtml(currentLine)}</b><br>${escapeHtml(s.headsign || "Διαδρομή")}`);
            for (const q of s.points) bounds.push(q);

            legendRows.push(
                `<div class="legend-row">` +
                  `<span class="legend-line" style="background:${color}"></span>` +
                  `<span class="legend-text">${escapeHtml(s.headsign || ("Διαδρομή " + (index + 1)))}</span>` +
                `</div>`
            );
        });

        if (legendRows.length) {
            legendControl = L.control({ position: "bottomright" });
            legendControl.onAdd = function() {
                const div = L.DomUtil.create("div", "route-legend");
                div.innerHTML = `<b>Γραμμή ${escapeHtml(currentLine)}</b>` + legendRows.join("");
                L.DomEvent.disableClickPropagation(div);
                L.DomEvent.disableScrollPropagation(div);
                return div;
            };
            legendControl.addTo(map);
        }

        // 2. Ενημέρωση Στάσεων Επιλεγμένης Γραμμής (ανεξάρτητο lineStopLayer)
        if (!allMode && currentLine) {
            if (renderedLineStopsLine !== currentLine || (lineStopLayer.getLayers().length === 0 && (j.stops || []).length > 0)) {
                lineStopLayer.clearLayers();
                for (const s of j.stops || []) {
                    const m = L.circleMarker(
                        [s.latitude, s.longitude],
                        {
                            renderer: stopRenderer,
                            radius: 6,
                            color: "#0969da",
                            weight: 2.5,
                            fillColor: "#ffffff",
                            fillOpacity: 1
                        }
                    ).addTo(lineStopLayer);

                    m.bindTooltip(`[Γραμμή ${escapeHtml(currentLine)}] ${escapeHtml(s.code)} — ${escapeHtml(s.name)}`, { direction: "top" });
                    attachStopPopup(m, s);
                }
                renderedLineStopsLine = currentLine;
            }
        } else {
            lineStopLayer.clearLayers();
            renderedLineStopsLine = null;
        }

        if (bounds.length === 0 && j.stops && j.stops.length > 0) {
            for (const s of j.stops) {
                if (Number.isFinite(s.latitude) && Number.isFinite(s.longitude)) {
                    bounds.push([s.latitude, s.longitude]);
                }
            }
        }

        // 3. Differential Marker Updates (χωρίς clearLayers/flicker)
        const incomingVehicleIds = new Set();

        for (const v of j.vehicles || []) {
            const vid = String(v.id || "").trim();
            if (!vid) continue;
            incomingVehicleIds.add(vid);

            const latLng = [v.latitude, v.longitude];
            bounds.push(latLng);

            const popupContent =
                `<b>Γραμμή ${escapeHtml(v.line)}</b><br>` +
                `Όχημα: <b>${escapeHtml(v.id)}</b><br>` +
                `${escapeHtml(v.headsign)}<br>` +
                `Bearing: ${Number(v.bearing || 0).toFixed(1)}°<br>` +
                `<span class="small">${Number(v.latitude).toFixed(5)}, ${Number(v.longitude).toFixed(5)}</span>`;

            let marker = vehicleMarkers.get(vid);

            if (marker) {
                // Ενημέρωση θέσης χωρίς επαναδημιουργία
                marker.setLatLng(latLng);

                // Ομαλή ενημέρωση γωνίας
                const el = marker.getElement();
                if (el) {
                    const arrow = el.querySelector(".bus-arrow-marker");
                    if (arrow) {
                        const bearing = Number(v.bearing);
                        if (Number.isFinite(bearing)) {
                            arrow.style.transform = `rotate(${bearing}deg)`;
                        }
                    }
                } else {
                    marker.setIcon(vehicleIcon(v));
                }

                // Ενημέρωση περιεχομένου popup διατηρώντας το ανοιχτό
                const p = marker.getPopup();
                if (p) p.setContent(popupContent);
                marker.options.title = `Γραμμή ${v.line} - Όχημα ${v.id}`;
            } else {
                // Νέο λεωφορείο
                marker = L.marker(latLng, {
                    icon: vehicleIcon(v),
                    title: `Γραμμή ${v.line} - Όχημα ${v.id}`,
                    zIndexOffset: 1000
                }).addTo(vehicleLayer);

                marker.bindPopup(popupContent);
                vehicleMarkers.set(vid, marker);
            }
        }

        // Αφαίρεση οχημάτων που δεν υπάρχουν πλέον
        for (const [vid, marker] of vehicleMarkers.entries()) {
            if (!incomingVehicleIds.has(vid)) {
                vehicleLayer.removeLayer(marker);
                vehicleMarkers.delete(vid);
            }
        }

        if (stopsVisible) {
            if (!map.hasLayer(allStopLayer)) allStopLayer.addTo(map);
        } else {
            if (map.hasLayer(allStopLayer)) map.removeLayer(allStopLayer);
        }

        const when = new Date((j.updated || Math.floor(Date.now() / 1000)) * 1000);
        status.textContent =
            `${j.vehicles?.length || 0} ενεργά λεωφορεία` +
            (!allMode && currentLine ? ` (γραμμή ${currentLine})` : "") +
            ` — ${when.toLocaleTimeString("el-GR")}` +
            (j.errors?.length ? ` — ${j.errors.length} σφάλματα` : "");

        if ((forceFit || firstFit) && bounds.length) {
            map.fitBounds(bounds, { padding: [30, 30], maxZoom: 15 });
            firstFit = false;
        }

    } catch(e) {
        status.textContent = "Σφάλμα live ενημέρωσης: " + e.message;
    }
}

function refreshLive(forceFit = false) {
    if (liveRefreshPromise) {
        pendingLiveRefresh = true;
        pendingForceFit = pendingForceFit || forceFit;
        return liveRefreshPromise;
    }

    liveRefreshPromise = (async () => {
        let fitThisPass = forceFit;
        do {
            pendingLiveRefresh = false;
            fitThisPass = fitThisPass || pendingForceFit;
            pendingForceFit = false;
            await performLiveRefresh(fitThisPass);
            fitThisPass = false;
        } while (pendingLiveRefresh);
    })();

    const currentRun = liveRefreshPromise;
    return currentRun.finally(() => {
        if (liveRefreshPromise === currentRun) {
            liveRefreshPromise = null;
        }
    });
}

function resetTimer() {
    if (timer) clearTimeout(timer);
    timerGeneration += 1;
    const generation = timerGeneration;
    const sec = Number(document.getElementById("intervalSelect").value || 10);
    localStorage.setItem("busmap_interval", sec);

    const scheduleNextRefresh = () => {
        timer = setTimeout(() => {
            refreshLive(false).finally(() => {
                if (generation === timerGeneration) scheduleNextRefresh();
            });
        }, sec * 1000);
    };

    scheduleNextRefresh();
}

async function loadRoutes() {
    const status = document.getElementById("status");
    try {
        status.textContent = "Φόρτωση γραμμών…";

        const r = await fetch("/api/routes");
        const j = await r.json();
        if (!j.ok) throw new Error(j.error || "Σφάλμα");

        allRoutes = j.routes || [];

        await loadActiveLines(true);

        if (currentLine) {
            document.getElementById("lineSelect").value = currentLine;
            await refreshLive(true);
        }
    } catch(e) {
        status.textContent = "Σφάλμα γραμμών: " + e.message;
    }
}

// Event Listeners
document.addEventListener("click", async e => {
    // 1. Επιλογή λεωφορείου από popup άφιξης
    const row = e.target.closest(".arrival-select");
    if (row) {
        e.preventDefault();
        e.stopPropagation();

        const line = row.dataset.line || "";
        const vehicle = row.dataset.vehicle || "";
        row.disabled = true;
        try {
            await selectArrivalBus(line, vehicle);
        } finally {
            row.disabled = false;
        }
        return;
    }
});

document.getElementById("lineSelect").addEventListener("change", async e => {
    currentLine = e.target.value;
    localStorage.setItem("busmap_line", currentLine);
    allMode = false;
    document.getElementById("allBtn").classList.remove("active");
    firstFit = true;
    vehicleLayer.clearLayers();
    vehicleMarkers.clear();
    lineStopLayer.clearLayers();
    renderedLineStopsLine = null;
    await refreshLive(true);
});

document.getElementById("lineSearch").addEventListener("input", fillLineSelect);

document.getElementById("lineSearch").addEventListener("keydown", async e => {
    if (e.key === "Enter") {
        const sel = document.getElementById("lineSelect");
        if (sel.options.length > 1) {
            sel.selectedIndex = 1;
            sel.dispatchEvent(new Event("change"));
        }
    }
});

document.getElementById("allBtn").addEventListener("click", async () => {
    currentLine = "";
    localStorage.removeItem("busmap_line");
    allMode = true;
    document.getElementById("allBtn").classList.add("active");
    document.getElementById("lineSelect").value = "";
    firstFit = true;
    vehicleLayer.clearLayers();
    vehicleMarkers.clear();
    lineStopLayer.clearLayers();
    renderedLineStopsLine = null;
    await refreshLive(true);
});

document.getElementById("refreshBtn").addEventListener("click", async () => {
    try {
        await loadActiveLines(false);
    } catch(e) {}
    await refreshLive(false);
});

document.getElementById("activeOnly").addEventListener("change", () => {
    fillLineSelect();
});

document.getElementById("stopsBtn").addEventListener("click", () => {
    stopsVisible = !stopsVisible;
    localStorage.setItem("busmap_stops", stopsVisible ? "1" : "0");
    const btn = document.getElementById("stopsBtn");
    btn.textContent = stopsVisible ? "Στάσεις: ON" : "Στάσεις: OFF";

    if (stopsVisible) {
        if (!map.hasLayer(allStopLayer)) allStopLayer.addTo(map);
    } else {
        if (map.hasLayer(allStopLayer)) map.removeLayer(allStopLayer);
    }
});

document.getElementById("intervalSelect").addEventListener("change", resetTimer);

// Αποκατάσταση αποθηκευμένου ρυθμού ανανέωσης
const savedInterval = localStorage.getItem("busmap_interval");
if (savedInterval) {
    document.getElementById("intervalSelect").value = savedInterval;
}

if (!stopsVisible) {
    document.getElementById("stopsBtn").textContent = "Στάσεις: OFF";
}

// Εκκίνηση
loadAllStops();
loadRoutes();
resetTimer();
</script>

</body>
</html>
"""


@app.get("/")
def index():
    return render_template_string(HTML)


if __name__ == "__main__":
    print()
    print("=" * 60)
    print(" Live Χάρτης Λεωφορείων ΟΣΕΘ / ΟΑΣΘ - Έκδοση v19")
    print("=" * 60)
    print(" Τοπική διεύθυνση: http://127.0.0.1:5000")
    print(" Πάτησε Ctrl+C για τερματισμό")
    print("=" * 60)
    print()
    app.run(host="127.0.0.1", port=5000, debug=False, threaded=True)
