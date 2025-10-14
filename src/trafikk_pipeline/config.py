from __future__ import annotations
from datetime import date
import os

def _require_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"Mangler miljøvariabel: {name}")
    return v

API_URL: str = os.environ.get(
    "TRAFIKKDATA_API", "https://trafikkdata-api.atlas.vegvesen.no/graphql"
).rstrip("/")

X_CLIENT: str = os.environ.get("X_CLIENT", "your.contact@company.no")

AZURE_ACCOUNT: str = _require_env("AZURE_STORAGE_ACCOUNT")
AZURE_CONTAINER: str = _require_env("AZURE_CONTAINER")
AZURE_KEY: str = _require_env("AZURE_ACCOUNT_KEY")

FROST_CLIENT_ID: str = (os.environ.get("FROST_CLIENT_ID") or "").strip()

START_DATE: date = date.fromisoformat(os.environ.get("BACKFILL_START", "2025-09-20"))

ENV_FROM: str = (os.environ.get("VOLUME_FROM") or "").strip()
ENV_TO: str = (os.environ.get("VOLUME_TO") or "").strip()
TIME_OFFSET: str = os.environ.get("TIME_OFFSET", "+02:00")

MAX_POINTS: int = int(os.environ.get("MAX_POINTS", "100"))
SLEEP_SECS: float = float(os.environ.get("RATE_LIMIT_SLEEP_SECS", "0.1"))

# Datasjø-stier
BRONZE_POINTS = "bronze/traffic/registration_points"
BRONZE_VOLUMES = "bronze/traffic/volumes"
BRONZE_RAIN = "bronze/weather/rain"

# Filnavn
POINTS_PARQUET = "flat.parquet"
VOLUMES_PARQUET = "flat.parquet"
RAIN_PARQUET = "rain.parquet"

# Bergen-bbox
BERGEN_MIN_LAT, BERGEN_MAX_LAT = 60.15, 60.55
BERGEN_MIN_LON, BERGEN_MAX_LON = 5.05, 5.55

# HTTP headers
HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": "airflow-trafikk-ingest",
    "X-Client": X_CLIENT,
}
