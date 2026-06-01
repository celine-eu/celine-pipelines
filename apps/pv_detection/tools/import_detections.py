"""Import local .detections.json files into raw.pv_predictions.

Usage:
    python tools/import_detections.py                  # import new only
    python tools/import_detections.py --full-refresh   # truncate + reimport all
"""

import argparse
import json
import os
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "flows"))
from db import get_engine, load_predictions, truncate_predictions, already_processed

APP_DIR = Path(__file__).parent.parent
TILE_DIR = APP_DIR / "data" / "tiles"


def collect_detections() -> pd.DataFrame:
    records = []
    for det_file in sorted(TILE_DIR.glob("*.detections.json")):
        try:
            data = json.loads(det_file.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        for bid, vals in data.items():
            records.append({
                "building_id": vals.get("building_id", bid),
                "has_pv": vals.get("has_pv", False),
                "confidence": vals.get("confidence", 0.0),
                "model_name": vals.get("model_name", ""),
                "reasoning": vals.get("reasoning", ""),
                "description": vals.get("description", ""),
                "raw_response": vals.get("raw_response", ""),
                "lon": vals.get("lon"),
                "lat": vals.get("lat"),
            })

    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records).drop_duplicates(subset="building_id", keep="last")


def main():
    parser = argparse.ArgumentParser(description="Import detections to DB")
    parser.add_argument("--full-refresh", action="store_true", help="Truncate and reimport all")
    args = parser.parse_args()

    engine = get_engine(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "15432"),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "securepassword123"),
        dbname=os.environ.get("POSTGRES_DB", "datasets"),
    )

    df = collect_detections()
    if df.empty:
        print("No .detections.json files found")
        return

    print(f"Found {len(df)} detections ({df['has_pv'].sum()} with PV)")

    if args.full_refresh:
        truncate_predictions(engine)
        n = load_predictions(engine, df)
        print(f"Full refresh: imported {n} records to raw.pv_predictions")
    else:
        existing = already_processed(engine)
        new_df = df[~df["building_id"].isin(existing)]
        if new_df.empty:
            print(f"All {len(df)} records already in DB, nothing to import")
            return
        n = load_predictions(engine, new_df)
        print(f"Imported {n} new records ({len(existing)} already in DB)")


if __name__ == "__main__":
    main()
