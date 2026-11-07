# =============================================================================
#  Dana Point PULSE  |  Proprietary and Confidential
#  Copyright (c) 2026 GloCon Solutions LLC. All rights reserved.
#  Created and owned by John Picou, GloCon Solutions LLC, built for Visit Dana Point.
#  This code, its design, and the PULSE report are protected. Copying, sharing,
#  reuse, or redistribution of any part requires written permission.
#  Contact John Picou, GloCon Solutions LLC: john.picou@gloconsolutions.com
#  See NOTICE.md in the repository root.
# =============================================================================
"""
fetch_nws_weather.py
--------------------
Pulls live weather data for Dana Point, CA from the National Weather Service
(weather.gov) public API — no API key required.

  https://www.weather.gov/documentation/services-web-api

Tables written:
  weather_forecast      — 7-day forecast periods
  weather_hourly        — 48-hour hourly forecast
  weather_observations  — Historical observations (past 30 days) from nearest NWS station

Dana Point coordinates: 33.4669°N, 117.6981°W
NWS User-Agent: required per NWS API policy — identifies the calling application.
"""

import re
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
DB   = ROOT / "data" / "analytics.sqlite"

# Dana Point, CA
DANA_POINT_LAT = 33.4669
DANA_POINT_LON = -117.6981

NWS_BASE = "https://api.weather.gov"
# NWS requires a descriptive User-Agent string with contact info
HEADERS = {
    "User-Agent": "VDP-Analytics/1.0 (gloconllc; contact@gloconllc.com)",
    "Accept":     "application/geo+json",
}

INIT_SQL = """
CREATE TABLE IF NOT EXISTS weather_forecast (
    period_number     INTEGER,
    start_time        TEXT    NOT NULL,
    end_time          TEXT,
    is_daytime        INTEGER,
    temp_f            REAL,
    temp_trend        TEXT,
    wind_speed        REAL,
    wind_direction    TEXT,
    short_forecast    TEXT,
    detailed_forecast TEXT,
    fetched_at        TEXT,
    UNIQUE(start_time) ON CONFLICT REPLACE
);

CREATE TABLE IF NOT EXISTS weather_hourly (
    start_time      TEXT    NOT NULL,
    temp_f          REAL,
    wind_speed      REAL,
    wind_direction  TEXT,
    short_forecast  TEXT,
    precip_chance   REAL,
    fetched_at      TEXT,
    UNIQUE(start_time) ON CONFLICT REPLACE
);

CREATE TABLE IF NOT EXISTS weather_observations (
    station_id              TEXT    NOT NULL,
    obs_time                TEXT    NOT NULL,
    temp_f                  REAL,
    dewpoint_f              REAL,
    wind_speed_mph          REAL,
    wind_direction_deg      REAL,
    barometric_pressure_pa  REAL,
    relative_humidity       REAL,
    visibility_m            REAL,
    weather_description     TEXT,
    fetched_at              TEXT,
    UNIQUE(station_id, obs_time) ON CONFLICT IGNORE
);
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _get(url: str, params: dict | None = None) -> dict | None:
    """GET JSON from NWS API. Returns parsed dict or None on any error."""
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=20)
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as exc:
        print(f"[WARN] HTTP {exc.response.status_code} for {url}: {exc}")
        return None
    except requests.RequestException as exc:
        print(f"[WARN] Request failed for {url}: {exc}")
        return None


def _parse_wind_speed(raw: str | None) -> float | None:
    """
    Parse NWS wind speed string to numeric mph (highest value in a range).

    Examples:
      "10 mph"       -> 10.0
      "10 to 15 mph" -> 15.0   (take the higher value)
      "Calm"         -> 0.0
      None           -> None
    """
    if raw is None:
        return None
    raw = raw.strip()
    if raw.lower() in ("calm", "0 mph", ""):
        return 0.0
    nums = re.findall(r"[\d.]+", raw)
    if not nums:
        return None
    return float(nums[-1])


def _c_to_f(celsius: float | None) -> float | None:
    """Convert Celsius to Fahrenheit."""
    if celsius is None:
        return None
    return round(celsius * 9.0 / 5.0 + 32.0, 1)


def _kmh_to_mph(kmh: float | None) -> float | None:
    """Convert km/h to mph."""
    if kmh is None:
        return None
    return round(kmh * 0.621371, 1)


def _qty_value(props: dict, key: str) -> float | None:
    """
    Extract numeric value from an NWS quantity-value object like:
      {"value": 15.3, "unitCode": "wmoUnit:degC"}
    Returns None if missing or null.
    """
    obj = props.get(key)
    if obj is None:
        return None
    if isinstance(obj, dict):
        v = obj.get("value")
    else:
        v = obj
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# NWS API calls
# ---------------------------------------------------------------------------

def fetch_grid_metadata() -> dict | None:
    """
    Call /points/{lat},{lon} to discover:
      - WFO office ID, gridX, gridY
      - Forecast and hourly forecast URLs
      - Observation stations URL
    """
    url = f"{NWS_BASE}/points/{DANA_POINT_LAT},{DANA_POINT_LON}"
    data = _get(url)
    if not data:
        return None
    props = data.get("properties", {})
    return {
        "wfo":                  props.get("gridId"),
        "grid_x":               props.get("gridX"),
        "grid_y":               props.get("gridY"),
        "forecast_url":         props.get("forecast"),
        "forecast_hourly_url":  props.get("forecastHourly"),
        "obs_stations_url":     props.get("observationStations"),
    }


def fetch_7day_forecast(forecast_url: str) -> list[dict]:
    """Fetch 7-day forecast from the grid forecast endpoint."""
    data = _get(forecast_url)
    if not data:
        return []
    periods = data.get("properties", {}).get("periods", [])
    fetched_at = _ts()
    rows = []
    for p in periods:
        rows.append({
            "period_number":    p.get("number"),
            "start_time":       p.get("startTime"),
            "end_time":         p.get("endTime"),
            "is_daytime":       1 if p.get("isDaytime") else 0,
            "temp_f":           p.get("temperature"),          # NWS returns °F directly
            "temp_trend":       p.get("temperatureTrend"),
            "wind_speed":       _parse_wind_speed(p.get("windSpeed")),
            "wind_direction":   p.get("windDirection"),
            "short_forecast":   p.get("shortForecast"),
            "detailed_forecast": p.get("detailedForecast"),
            "fetched_at":       fetched_at,
        })
    return rows


def fetch_hourly_forecast(forecast_hourly_url: str) -> list[dict]:
    """Fetch next 48 hours from the grid hourly forecast endpoint."""
    data = _get(forecast_hourly_url)
    if not data:
        return []
    periods = data.get("properties", {}).get("periods", [])[:48]
    fetched_at = _ts()
    rows = []
    for p in periods:
        # probabilityOfPrecipitation is a quantity-value object or None
        pop_obj = p.get("probabilityOfPrecipitation")
        if isinstance(pop_obj, dict):
            pop = pop_obj.get("value")
        else:
            pop = pop_obj
        try:
            pop = float(pop) if pop is not None else None
        except (TypeError, ValueError):
            pop = None

        rows.append({
            "start_time":     p.get("startTime"),
            "temp_f":         p.get("temperature"),            # already °F
            "wind_speed":     _parse_wind_speed(p.get("windSpeed")),
            "wind_direction": p.get("windDirection"),
            "short_forecast": p.get("shortForecast"),
            "precip_chance":  pop,
            "fetched_at":     fetched_at,
        })
    return rows


def fetch_nearest_station_id(obs_stations_url: str) -> str | None:
    """Return the stationIdentifier of the nearest NWS observation station."""
    data = _get(obs_stations_url)
    if not data:
        return None
    features = data.get("features", [])
    if not features:
        return None
    return features[0].get("properties", {}).get("stationIdentifier")


def fetch_observations(station_id: str) -> list[dict]:
    """
    Fetch raw surface observations for the past 30 days.
    NWS returns temperatures in °C and wind speed in km/h — both are converted.
    """
    start = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    url   = f"{NWS_BASE}/stations/{station_id}/observations"
    data  = _get(url, params={"start": start, "limit": 500})
    if not data:
        return []

    features   = data.get("features", [])
    fetched_at = _ts()
    rows: list[dict] = []

    for feat in features:
        props    = feat.get("properties", {})
        obs_time = props.get("timestamp")
        if not obs_time:
            continue

        # Convert temperature: °C → °F
        temp_f     = _c_to_f(_qty_value(props, "temperature"))
        dewpoint_f = _c_to_f(_qty_value(props, "dewpoint"))

        # Convert wind speed: km/h → mph
        wind_mph = _kmh_to_mph(_qty_value(props, "windSpeed"))

        # Wind direction in degrees (already numeric, no conversion needed)
        wind_deg = _qty_value(props, "windDirection")

        # Barometric pressure in Pascals (store as-is)
        pressure_pa = _qty_value(props, "barometricPressure")

        # Relative humidity as percentage
        humidity = _qty_value(props, "relativeHumidity")

        # Visibility in meters (store as-is)
        visibility = _qty_value(props, "visibility")

        # Present weather description — list of condition objects
        weather_list = props.get("presentWeather") or []
        desc_parts   = [w.get("rawString", "") for w in weather_list if w.get("rawString")]
        weather_desc = ", ".join(desc_parts) if desc_parts else None

        rows.append({
            "station_id":              station_id,
            "obs_time":                obs_time,
            "temp_f":                  temp_f,
            "dewpoint_f":              dewpoint_f,
            "wind_speed_mph":          wind_mph,
            "wind_direction_deg":      wind_deg,
            "barometric_pressure_pa":  pressure_pa,
            "relative_humidity":       humidity,
            "visibility_m":            visibility,
            "weather_description":     weather_desc,
            "fetched_at":              fetched_at,
        })

    return rows


# ---------------------------------------------------------------------------
# DB writes
# ---------------------------------------------------------------------------

def write_forecast(conn: sqlite3.Connection, rows: list[dict]) -> int:
    if not rows:
        return 0
    conn.executemany(
        """INSERT OR REPLACE INTO weather_forecast
           (period_number, start_time, end_time, is_daytime, temp_f, temp_trend,
            wind_speed, wind_direction, short_forecast, detailed_forecast, fetched_at)
           VALUES (:period_number, :start_time, :end_time, :is_daytime, :temp_f, :temp_trend,
                   :wind_speed, :wind_direction, :short_forecast, :detailed_forecast, :fetched_at)""",
        rows,
    )
    conn.commit()
    return len(rows)


def write_hourly(conn: sqlite3.Connection, rows: list[dict]) -> int:
    if not rows:
        return 0
    conn.executemany(
        """INSERT OR REPLACE INTO weather_hourly
           (start_time, temp_f, wind_speed, wind_direction, short_forecast, precip_chance, fetched_at)
           VALUES (:start_time, :temp_f, :wind_speed, :wind_direction,
                   :short_forecast, :precip_chance, :fetched_at)""",
        rows,
    )
    conn.commit()
    return len(rows)


def write_observations(conn: sqlite3.Connection, rows: list[dict]) -> int:
    if not rows:
        return 0
    conn.executemany(
        """INSERT OR IGNORE INTO weather_observations
           (station_id, obs_time, temp_f, dewpoint_f, wind_speed_mph, wind_direction_deg,
            barometric_pressure_pa, relative_humidity, visibility_m,
            weather_description, fetched_at)
           VALUES (:station_id, :obs_time, :temp_f, :dewpoint_f, :wind_speed_mph,
                   :wind_direction_deg, :barometric_pressure_pa, :relative_humidity,
                   :visibility_m, :weather_description, :fetched_at)""",
        rows,
    )
    conn.commit()
    return len(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    print(f"[{_ts()}] fetch_nws_weather.py — NWS weather.gov API (Dana Point, CA)")

    conn = sqlite3.connect(DB, timeout=10)
    conn.executescript(INIT_SQL)
    conn.commit()

    # 1. Discover grid metadata for Dana Point
    print(f"[{_ts()}] Fetching NWS grid metadata ({DANA_POINT_LAT}, {DANA_POINT_LON})")
    meta = fetch_grid_metadata()
    if not meta:
        print("[WARN] fetch_nws_weather: could not fetch grid metadata — skipping all steps")
        conn.close()
        return 0

    print(f"[{_ts()}] Grid resolved: WFO={meta['wfo']}, X={meta['grid_x']}, Y={meta['grid_y']}")

    total = 0

    # 2. 7-day forecast
    try:
        forecast_url = meta.get("forecast_url")
        if forecast_url:
            rows = fetch_7day_forecast(forecast_url)
            n = write_forecast(conn, rows)
            total += n
            print(f"[OK]   weather_forecast: {n} forecast periods written")
        else:
            print("[WARN] No forecast URL returned by NWS /points endpoint")
    except Exception as exc:
        print(f"[WARN] 7-day forecast step failed: {exc}")

    # 3. 48-hour hourly forecast
    try:
        hourly_url = meta.get("forecast_hourly_url")
        if hourly_url:
            rows = fetch_hourly_forecast(hourly_url)
            n = write_hourly(conn, rows)
            total += n
            print(f"[OK]   weather_hourly: {n} hourly periods written")
        else:
            print("[WARN] No hourly forecast URL returned by NWS /points endpoint")
    except Exception as exc:
        print(f"[WARN] Hourly forecast step failed: {exc}")

    # 4. Historical observations from nearest station
    try:
        obs_stations_url = meta.get("obs_stations_url")
        if obs_stations_url:
            station_id = fetch_nearest_station_id(obs_stations_url)
            if station_id:
                print(f"[{_ts()}] Fetching 30-day observations from station: {station_id}")
                rows = fetch_observations(station_id)
                n = write_observations(conn, rows)
                total += n
                print(f"[OK]   weather_observations: {n} observations written (station={station_id})")
            else:
                print("[WARN] No observation station found near Dana Point")
        else:
            print("[WARN] No observation stations URL returned by NWS /points endpoint")
    except Exception as exc:
        print(f"[WARN] Observations step failed: {exc}")

    conn.close()
    return total


if __name__ == "__main__":
    n = main()
    print(f"[DONE] fetch_nws_weather: {n} total rows inserted/updated")
    sys.exit(0)
