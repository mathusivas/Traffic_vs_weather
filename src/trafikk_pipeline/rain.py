from __future__ import annotations
from datetime import date
import json
import time
import pandas as pd
import requests

from trafikk_pipeline import config
from trafikk_pipeline.clients import upload_parquet, blob_container
from trafikk_pipeline.paths import part_dir

def _frost_daily_precip(lat: float, lon: float, d: date) -> float | None:
    start = f"{d.isoformat()}T00:00:00Z"
    end = f"{d.isoformat()}T23:59:59Z"
    params = {
        "sources": f"nearest(POINT({lon} {lat}))",
        "referencetime": f"{start}/{end}",
        "elements": "precipitation_amount",
        "nearestmaxdistance": "50000",
    }
    r = requests.get(
        "https://frost.met.no/observations/v0.json",
        params=params,
        auth=(config.FROST_CLIENT_ID, ""),
        timeout=30,
        headers={"Accept": "application/json"},
    )
    if r.status_code == 401:
        raise RuntimeError("Frost 401 – sett FROST_CLIENT_ID i Airflow env.")
    if not r.ok:
        return None
    js = r.json()
    tot = 0.0
    any_obs = False
    for st in js.get("data", []):
        for obs in st.get("observations", []):
            if obs.get("elementId") == "precipitation_amount":
                any_obs = True
                try:
                    tot += float(obs.get("value"))
                except (TypeError, ValueError):
                    pass
    if not any_obs:
        return None
    return round(tot, 3)

def fetch_and_store_rain(points_dir: str, points_json: str | None = None):
    if not config.FROST_CLIENT_ID:
        print("[rain] SKIP – FROST_CLIENT_ID ikke satt.")
        return

    # prøv å lese punkter fra blob; fall back til JSON-arg
    try:
        pbytes = blob_container().get_blob_client(f"{points_dir}/{config.POINTS_PARQUET}").download_blob().readall()
        import io  # local import to avoid unused if try fails
        points_df = pd.read_parquet(io.BytesIO(pbytes), engine="pyarrow")
    except Exception:
        pts = json.loads(points_json) if points_json else []
        points_df = pd.DataFrame(pts)

    if points_df.empty or not {"id", "lat", "lon"}.issubset(points_df.columns):
        print("[rain] Ingen punkter med lat/lon – stopper.")
        return

    max_pts = min(len(points_df), config.MAX_POINTS)
    use_df = points_df.iloc[:max_pts].copy()

    for dts in pd.date_range(start=config.START_DATE, end=date.today(), freq="D"):
        d = dts.date()
        rows = []
        for _, r in use_df.iterrows():
            pid, lat, lon = str(r["id"]), float(r["lat"]), float(r["lon"])
            try:
                mm = _frost_daily_precip(lat, lon, d)
            except Exception as ex:
                print(f"[rain] {pid} {d} feilet: {ex}")
                mm = None
            rows.append({"point_id": pid, "date": d.isoformat(), "precip_mm": mm})
            time.sleep(0.02)

        out_dir = part_dir(d, config.BRONZE_RAIN)
        upload_parquet(f"{out_dir}/{config.RAIN_PARQUET}", pd.DataFrame(rows))
        print(f"[rain] skrev {len(rows)} rader → {out_dir}/{config.RAIN_PARQUET}")
