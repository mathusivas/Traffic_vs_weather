from __future__ import annotations
from datetime import date
import io
import json
import time
import pandas as pd
from azure.core.exceptions import ResourceNotFoundError

from trafikk_pipeline import config
from trafikk_pipeline.clients import gql, blob_container, upload_parquet
from trafikk_pipeline.paths import part_dir
from trafikk_pipeline.gql_queries import VOLUME_BY_DAY

def _volume_window(d_until: date) -> tuple[str, str]:
    if config.ENV_FROM and config.ENV_TO:
        return config.ENV_FROM, config.ENV_TO
    start = f"{config.START_DATE.isoformat()}T00:00:00{config.TIME_OFFSET}"
    end = f"{d_until.isoformat()}T23:59:59{config.TIME_OFFSET}"
    return start, end

def _fetch_point_volumes(pid: str, start_iso: str, end_iso: str) -> list[dict]:
    rows: list[dict] = []
    resp = gql(VOLUME_BY_DAY(pid, start_iso, end_iso))
    if resp.get("errors"):
        print(f"[vol] {pid}: {resp['errors']}")
        return rows
    edges = (
        (((resp.get("data") or {}).get("trafficData") or {}).get("volume") or {})
        .get("byDay", {})
        .get("edges", [])
        or []
    )
    for e in edges:
        node = (e or {}).get("node") or {}
        vfield = (node.get("total") or {}).get("volumeNumbers")
        vol_val = None
        if isinstance(vfield, list) and vfield:
            vol_val = (vfield[0] or {}).get("volume")
        elif isinstance(vfield, dict):
            vol_val = vfield.get("volume")
        rows.append(
            {
                "point_id": pid,
                "from": node.get("from"),
                "to": node.get("to"),
                "total_volume": int(vol_val) if vol_val is not None else None,
            }
        )
    return rows

def fetch_and_store_volumes(points_dir: str) -> str | None:
    d = date.today()
    blob = blob_container()

    points_path = f"{points_dir}/{config.POINTS_PARQUET}"
    try:
        content = blob.get_blob_client(points_path).download_blob().readall()
    except ResourceNotFoundError:
        raise RuntimeError(
            f"Fant ikke punkter-parquet: {points_path} (kjør fetch_registration_points først)"
        )

    df_points = pd.read_parquet(io.BytesIO(content), engine="pyarrow")
    if df_points.empty or "id" not in df_points.columns:
        print("[vol] Points-parquet mangler 'id' eller er tom.")
        return None

    ids = df_points["id"].dropna().astype(str).unique().tolist()[: config.MAX_POINTS]
    start_iso, end_iso = _volume_window(d)
    print(f"[vol] Henter volum for {len(ids)} punkter: {start_iso} → {end_iso}")

    all_rows: list[dict] = []
    for i, pid in enumerate(ids, 1):
        try:
            all_rows.extend(_fetch_point_volumes(pid, start_iso, end_iso))
        except Exception as ex:
            print(f"[vol] Punkt {pid} feilet: {ex}")
        if i % 20 == 0:
            print(f"[vol] Ferdig med {i}/{len(ids)}…")
        time.sleep(config.SLEEP_SECS)

    if not all_rows:
        print("[vol] Ingen volumer hentet.")
        return json.dumps([])

    vol_df = pd.DataFrame(all_rows)

    for col in ["from", "to"]:
        vol_df[col] = pd.to_datetime(vol_df[col], utc=True, errors="coerce").dt.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    if "total_volume" in vol_df.columns:
        vol_df["total_volume"] = pd.to_numeric(vol_df["total_volume"], errors="coerce").astype(
            "Int64"
        )

    vol_df["from_date"] = vol_df["from"].str[:10]
    for day_str, grp in vol_df.groupby("from_date"):
        try:
            day = date.fromisoformat(day_str)
        except Exception:
            continue
        out_dir = part_dir(day, config.BRONZE_VOLUMES)
        out_cols = ["point_id", "from", "to", "total_volume"]
        upload_parquet(f"{out_dir}/{config.VOLUMES_PARQUET}", grp[out_cols].reset_index(drop=True))
        print(f"[vol] Skrev {len(grp)} rader til {out_dir}/{config.VOLUMES_PARQUET}")

    slim = df_points[["id", "lat", "lon"]].dropna()
    return json.dumps(slim.to_dict(orient="records"))
