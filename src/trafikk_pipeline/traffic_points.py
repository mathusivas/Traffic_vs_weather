from __future__ import annotations
from datetime import date, datetime
import pandas as pd
from trafikk_pipeline import config
from trafikk_pipeline.clients import gql, upload_parquet, upload_json
from trafikk_pipeline.paths import part_dir
from trafikk_pipeline.gql_queries import REGISTRATION_POINTS_VESTLAND

BERGEN_BOX = (
    config.BERGEN_MIN_LAT,
    config.BERGEN_MAX_LAT,
    config.BERGEN_MIN_LON,
    config.BERGEN_MAX_LON,
)

def today_utc() -> date:
    return datetime.utcnow().date()

def fetch_and_store_points() -> str:
    # 1) Hent rådata fra GraphQL
    print("[points] Henter registreringspunkter…")
    data = gql(REGISTRATION_POINTS_VESTLAND)

    # 2) Skriv rå JSON i dagens partisjon
    d = today_utc()
    points_dir = part_dir(d, config.BRONZE_POINTS)
    upload_json(f"{points_dir}/raw.json", data)

    # 3) Flate ut og filtrer til Bergen-bbox
    nodes = (data.get("data") or {}).get("trafficRegistrationPoints") or []
    rows = []
    for n in nodes:
        lat = (((n.get("location") or {}).get("coordinates") or {}).get("latLon") or {}).get("lat")
        lon = (((n.get("location") or {}).get("coordinates") or {}).get("latLon") or {}).get("lon")
        if lat is None or lon is None:
            continue
        if not (BERGEN_BOX[0] <= lat <= BERGEN_BOX[1] and BERGEN_BOX[2] <= lon <= BERGEN_BOX[3]):
            continue
        rows.append({
            "id": n.get("id"),
            "name": n.get("name"),
            "lat": lat,
            "lon": lon,
            "partition_date": d.isoformat(),
        })
    df = pd.DataFrame(rows)

    today_d = today_utc()
    for dts in pd.date_range(start=config.START_DATE, end=today_d, freq="D"):
        out_dir = part_dir(dts.date(), config.BRONZE_POINTS)
        upload_parquet(f"{out_dir}/{config.POINTS_PARQUET}", df)
        print(f"[points] {len(df)} -> {out_dir}/{config.POINTS_PARQUET}")

    return points_dir
