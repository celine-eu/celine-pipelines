"""
Tile viewer — browse available tiles, download new ones, overlay building bboxes,
run Ollama detection on demand, and visualize results.

Usage:
    cd apps/pv_detection
    streamlit run tools/viewer.py
"""

import json
import sys
from io import BytesIO
from pathlib import Path

import pandas as pd
import streamlit as st
from PIL import Image, ImageDraw

sys.path.insert(0, str(Path(__file__).parent.parent / "flows"))
sys.path.insert(0, str(Path(__file__).parent))

from db import get_engine, ensure_schema, load_predictions, truncate_predictions, already_processed
from detector import create_detector, DetectionResult
from providers import FilesystemProvider
from download_tiles import TileDownloader

APP_DIR = Path(__file__).parent.parent
FLOWS_DIR = APP_DIR / "flows"
TILE_DIR = APP_DIR / "data" / "tiles"


def _detections_path(provider: FilesystemProvider, tile_id: str) -> Path | None:
    """Return the .detections.json path for a tile."""
    path = provider._index.get(tile_id)
    if path:
        return path.parent / f"{path.name}.detections.json"
    return None


def save_detections(provider: FilesystemProvider, tile_id: str, results: dict[str, DetectionResult]):
    path = _detections_path(provider, tile_id)
    if not path:
        return
    data = {
        bid: r.to_dict() for bid, r in results.items()
    }
    path.write_text(json.dumps(data, indent=2))


def load_detections(provider: FilesystemProvider, tile_id: str) -> dict[str, DetectionResult] | None:
    path = _detections_path(provider, tile_id)
    if not path or not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        return {
            bid: DetectionResult(**vals)
            for bid, vals in data.items()
        }
    except (json.JSONDecodeError, TypeError, KeyError):
        return None


@st.cache_resource
def load_config():
    import os
    import yaml

    config_path = FLOWS_DIR / "config.yaml"
    with open(config_path) as fh:
        config = yaml.safe_load(fh)
    ollama = config.get("detector", {}).get("ollama", {})
    if os.environ.get("OLLAMA_HOST"):
        ollama["host"] = os.environ["OLLAMA_HOST"]
    if os.environ.get("OLLAMA_MODEL"):
        ollama["model"] = os.environ["OLLAMA_MODEL"]
    return config


def get_provider():
    config = load_config()
    tile_dir = config["provider"]["filesystem"]["tile_dir"]
    return FilesystemProvider(tile_dir)


def get_db_engine(host, port, user, password, dbname):
    return get_engine(host, port, user, password, dbname)


def load_buildings_for_tile(meta: dict | None, engine) -> list[dict]:
    if not meta or "bbox_25832" not in meta or engine is None:
        return []

    import sqlalchemy as sa

    bbox = meta["bbox_25832"]
    config = load_config()
    schema = config["buildings"]["schema"]
    table = config["buildings"]["table"]

    query = sa.text(f"""
        SELECT
            building_id,
            ST_X(ST_Centroid(geometry)) as lon,
            ST_Y(ST_Centroid(geometry)) as lat,
            footprint_area_m2::int as area_m2,
            ST_XMin(ST_Transform(geometry, 25832)) as bx_min,
            ST_YMin(ST_Transform(geometry, 25832)) as by_min,
            ST_XMax(ST_Transform(geometry, 25832)) as bx_max,
            ST_YMax(ST_Transform(geometry, 25832)) as by_max
        FROM {schema}.{table}
        WHERE ST_Intersects(
            ST_Transform(geometry, 25832),
            ST_MakeEnvelope(:xmin, :ymin, :xmax, :ymax, 25832)
        )
    """)

    with engine.connect() as conn:
        rows = conn.execute(query, dict(
            xmin=bbox[0], ymin=bbox[1], xmax=bbox[2], ymax=bbox[3]
        )).mappings().all()

    return [dict(r) for r in rows]


def draw_buildings_on_tile(
    image_bytes: bytes,
    meta: dict,
    buildings: list[dict],
    detection_map: dict[str, DetectionResult] | None = None,
) -> Image.Image:
    img = Image.open(BytesIO(image_bytes)).convert("RGB")
    draw = ImageDraw.Draw(img)

    bbox = meta["bbox_25832"]
    tile_xmin, tile_ymin, tile_xmax, tile_ymax = bbox
    tile_w = tile_xmax - tile_xmin
    tile_h = tile_ymax - tile_ymin
    img_w, img_h = img.size

    overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
    overlay_draw = ImageDraw.Draw(overlay)

    min_coverage = 0.5

    for idx, b in enumerate(buildings, 1):
        full_bx0 = (b["bx_min"] - tile_xmin) / tile_w * img_w
        full_by0 = (tile_ymax - b["by_max"]) / tile_h * img_h
        full_bx1 = (b["bx_max"] - tile_xmin) / tile_w * img_w
        full_by1 = (tile_ymax - b["by_min"]) / tile_h * img_h
        full_area = (full_bx1 - full_bx0) * (full_by1 - full_by0)

        bx0 = max(0, full_bx0)
        by0 = max(0, full_by0)
        bx1 = min(img_w, full_bx1)
        by1 = min(img_h, full_by1)
        visible_area = (bx1 - bx0) * (by1 - by0)
        coverage = visible_area / full_area if full_area > 0 else 0

        bid = b["building_id"]
        det = detection_map.get(bid) if detection_map else None

        if coverage < min_coverage:
            outline = (128, 128, 128, 120)
            fill = (0, 0, 0, 0)
            label = f"#{idx} edge"
        elif det and det.has_pv:
            outline = (0, 255, 0, 255)
            fill = (0, 255, 0, 50)
            label = f"#{idx} PV={det.confidence:.0%}"
        elif det and not det.has_pv:
            outline = (255, 60, 60, 255)
            fill = (255, 0, 0, 30)
            label = f"#{idx} no PV"
        else:
            outline = (0, 200, 255, 200)
            fill = (0, 0, 0, 0)
            label = f"#{idx}"

        overlay_draw.rectangle([bx0, by0, bx1, by1], outline=outline, fill=fill, width=2)

        label_y = by0 - 14 if by0 > 16 else by1 + 2
        overlay_draw.rectangle([bx0, label_y, bx0 + len(label) * 7 + 4, label_y + 14], fill=(0, 0, 0, 180))
        overlay_draw.text((bx0 + 2, label_y + 1), label, fill=outline)

    img = img.convert("RGBA")
    img = Image.alpha_composite(img, overlay)
    return img.convert("RGB")


def run_detection_on_buildings(
    provider: FilesystemProvider,
    tile_id: str,
    buildings: list[dict],
    config: dict,
    tile_image_bytes: bytes,
    meta: dict,
    force_refresh: bool = False,
) -> dict[str, DetectionResult]:
    import time

    detector = create_detector(config["detector"])
    model_name = config["detector"].get("ollama", {}).get("model", "unknown")

    # Load existing cached results — skip already-detected buildings
    cached = {} if force_refresh else (load_detections(provider, tile_id) or {})

    bbox = meta["bbox_25832"]
    tile_xmin, tile_ymin, tile_xmax, tile_ymax = bbox
    tile_w = tile_xmax - tile_xmin
    tile_h = tile_ymax - tile_ymin

    img = Image.open(BytesIO(tile_image_bytes)).convert("RGB")
    img_w, img_h = img.size

    results = dict(cached)
    pending = [b for b in buildings if b["building_id"] not in cached]

    status = st.empty()
    log_area = st.container()

    if not pending:
        status.success(f"All {len(buildings)} buildings already cached ({len(cached)} results)")
        return results

    progress = st.progress(0)

    cached_msg = f" ({len(cached)} cached, {len(pending)} remaining)" if cached else ""
    status.info(f"Detecting PV on {len(pending)}/{len(buildings)} buildings using **{model_name}**{cached_msg}...")
    t_start = time.time()

    detected_count = 0
    for i, b in enumerate(pending):

        bid = b["building_id"]
        area = b.get("area_m2", "?")

        # Full building bbox in pixel coords (unclamped)
        full_bx0 = (b["bx_min"] - tile_xmin) / tile_w * img_w
        full_by0 = (tile_ymax - b["by_max"]) / tile_h * img_h
        full_bx1 = (b["bx_max"] - tile_xmin) / tile_w * img_w
        full_by1 = (tile_ymax - b["by_min"]) / tile_h * img_h
        full_area = (full_bx1 - full_bx0) * (full_by1 - full_by0)

        # Clamped to tile bounds
        bx0 = max(0, int(full_bx0))
        by0 = max(0, int(full_by0))
        bx1 = min(img_w, int(full_bx1))
        by1 = min(img_h, int(full_by1))

        crop_w, crop_h = bx1 - bx0, by1 - by0
        visible_area = crop_w * crop_h
        coverage = visible_area / full_area if full_area > 0 else 0

        if crop_w < 10 or crop_h < 10 or coverage < 0.5:
            with log_area:
                st.caption(f"  {i+1}/{len(pending)} `{bid[:12]}...` — skipped ({coverage:.0%} visible, crop {crop_w}x{crop_h}px)")
            continue

        crop = img.crop((bx0, by0, bx1, by1))

        min_dim = 64
        if crop.width < min_dim or crop.height < min_dim:
            scale = max(min_dim / crop.width, min_dim / crop.height)
            crop = crop.resize(
                (max(min_dim, int(crop.width * scale)), max(min_dim, int(crop.height * scale))),
                Image.LANCZOS,
            )

        buf = BytesIO()
        crop.save(buf, format="JPEG", quality=90)

        elapsed = time.time() - t_start
        avg = elapsed / max(detected_count, 1)
        remaining = avg * (len(pending) - i)
        progress.progress(
            (i + 1) / len(pending),
            text=f"Building {i+1}/{len(pending)} — ~{remaining:.0f}s remaining",
        )

        t_call = time.time()
        try:
            result = detector.detect(buf.getvalue(), bid)
            results[bid] = result
        except Exception as e:
            results[bid] = DetectionResult(
                building_id=bid, has_pv=False, confidence=0.0,
                model_name="error", reasoning=f"error: {e}",
            )
            result = results[bid]
        dt = time.time() - t_call
        detected_count += 1

        icon = "+" if result.has_pv else "-"
        desc = f" | {result.description[:80]}..." if result.description else ""
        with log_area:
            st.caption(
                f"  {icon} {i+1}/{len(pending)} `{bid[:12]}...` "
                f"{area}m² {crop_w}x{crop_h}px — "
                f"{'**PV**' if result.has_pv else 'no PV'} "
                f"({result.confidence:.0%}) {dt:.1f}s{desc}"
            )

        # Save incrementally after each detection
        save_detections(provider, tile_id, results)

    elapsed = time.time() - t_start
    pv_count = sum(1 for r in results.values() if r.has_pv)
    status.success(f"Done: {pv_count} PV found in {len(results)} buildings — {elapsed:.0f}s total")
    progress.empty()

    return results


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

def _run_all_tiles(provider: FilesystemProvider, tile_ids: list[str], engine, force_refresh: bool = False):
    import time

    config = load_config()
    total_tiles = len(tile_ids)

    pending = []
    for tid in tile_ids:
        meta = provider.get_metadata(tid)
        if not meta or "bbox_25832" not in meta:
            continue
        cached = {} if force_refresh else (load_detections(provider, tid) or {})
        buildings = load_buildings_for_tile(meta, engine)
        uncached = [b for b in buildings if b["building_id"] not in cached]
        if uncached:
            pending.append((tid, meta, buildings, cached))

    if not pending:
        st.success(f"All {total_tiles} tiles fully cached — nothing to do")
        return

    status = st.empty()
    progress = st.progress(0)
    log_area = st.container()

    total_buildings = sum(len(b) - len(c) for _, _, b, c in pending)
    status.info(f"Processing {len(pending)}/{total_tiles} tiles, {total_buildings} buildings remaining...")
    t_start = time.time()

    done_buildings = 0
    for t_idx, (tid, meta, buildings, cached) in enumerate(pending):
        image_bytes = provider.get_tile(tid)
        if image_bytes is None:
            continue

        with log_area:
            st.caption(f"**Tile {t_idx+1}/{len(pending)}: {tid}** — {len(buildings)} buildings, {len(cached)} cached")

        detection_map = run_detection_on_buildings(
            provider, tid, buildings, config, image_bytes, meta,
            force_refresh=force_refresh,
        )
        st.session_state[f"det_{tid}"] = detection_map

        new_count = len(detection_map) - len(cached)
        done_buildings += new_count
        pv = sum(1 for r in detection_map.values() if r.has_pv)

        with log_area:
            st.caption(f"  Tile {tid}: {pv} PV / {len(detection_map)} total")

        progress.progress((t_idx + 1) / len(pending))

    elapsed = time.time() - t_start
    status.success(f"All done: {len(pending)} tiles, {done_buildings} new detections — {elapsed:.0f}s")
    progress.empty()


def page_browse():
    st.header("Browse Tiles")

    provider = get_provider()
    available = sorted(provider.available_ids())
    stems = sorted({s for s in available if len(s) <= 32})
    if not stems:
        stems = sorted(set(available))

    if not stems:
        st.warning("No tiles in data/tiles/. Use the Download tab to fetch some.")
        return

    engine = st.session_state.get("db_engine")

    # --- Batch detect all tiles ---
    if engine:
        c_all, c_all_refresh = st.columns([3, 1])
        with c_all:
            run_all = st.button("Detect all tiles", type="primary", key="det_all")
        with c_all_refresh:
            force_all = st.checkbox("Force refresh", key="force_all")
        if run_all:
            _run_all_tiles(provider, stems, engine, force_refresh=force_all)

    st.divider()

    tile_id = st.selectbox("Select tile", stems)
    if not tile_id:
        return

    image_bytes = provider.get_tile(tile_id)
    meta = provider.get_metadata(tile_id)

    if image_bytes is None:
        st.error(f"No image found for {tile_id}")
        return

    col1, col2 = st.columns([2, 1])

    engine = st.session_state.get("db_engine")
    buildings = []

    with col2:
        st.subheader("Metadata")
        if meta:
            st.json(meta)
        else:
            st.info("No .json sidecar")

        if engine and meta and "bbox_25832" in meta:
            buildings = load_buildings_for_tile(meta, engine)
            st.metric("Buildings in tile", len(buildings))
            for b in buildings:
                st.caption(f"`{b['building_id'][:12]}...` — {b.get('area_m2', '?')}m²")

    with col1:
        # Load cached detections from disk, fall back to session state
        detection_map = st.session_state.get(f"det_{tile_id}")
        if detection_map is None:
            detection_map = load_detections(provider, tile_id)
            if detection_map:
                st.session_state[f"det_{tile_id}"] = detection_map
                st.caption("Loaded cached detections from disk")

        if buildings:
            c_detect, c_refresh, c_clear = st.columns([3, 1, 1])
            with c_detect:
                run_detect = st.button("Detect PV on buildings", type="primary", key="det_bld")
            with c_refresh:
                force_refresh = st.checkbox("Force refresh", key="force_refresh")
            with c_clear:
                if detection_map and st.button("Clear cache", key="clr"):
                    detection_map = None
                    st.session_state.pop(f"det_{tile_id}", None)
                    det_path = _detections_path(provider, tile_id)
                    if det_path and det_path.exists():
                        det_path.unlink()
                    st.rerun()

            if run_detect:
                config = load_config()
                detection_map = run_detection_on_buildings(
                    provider, tile_id, buildings, config,
                    image_bytes, meta, force_refresh=force_refresh,
                )
                st.session_state[f"det_{tile_id}"] = detection_map

            annotated = draw_buildings_on_tile(image_bytes, meta, buildings, detection_map)
            st.image(annotated, caption=f"{tile_id} — {len(buildings)} buildings", width="stretch")

            if detection_map:
                pv_count = sum(1 for r in detection_map.values() if r.has_pv)
                st.caption(f"Results: {pv_count} PV / {len(detection_map)} detected / {len(buildings)} buildings")
        else:
            img = Image.open(BytesIO(image_bytes))
            st.image(img, caption=tile_id, width="stretch")



def _collect_detections_from_fs(provider: FilesystemProvider) -> pd.DataFrame:
    """Read all .detections.json files and flatten into a DataFrame matching raw.pv_predictions."""
    records = []
    tile_dir = provider.tile_dir

    for det_file in sorted(tile_dir.glob("*.detections.json")):
        try:
            data = json.loads(det_file.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        for bid, vals in data.items():
            records.append({
                "building_id": vals.get("building_id", bid),
                "has_pv": vals.get("has_pv", False),
                "confidence": vals.get("confidence", 0.0),
                "model_name": vals.get("model_name", "unknown"),
                "reasoning": vals.get("reasoning", ""),
                "description": vals.get("description", ""),
                "raw_response": vals.get("raw_response", ""),
                "lon": vals.get("lon"),
                "lat": vals.get("lat"),
            })

    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records).drop_duplicates(subset="building_id", keep="last")


def page_db_import():
    st.header("Import to Database")

    engine = st.session_state.get("db_engine")
    if not engine:
        st.info("Connect to the database in the sidebar first.")
        return

    config = load_config()
    pred_cfg = config.get("predictions", {"schema": "raw", "table": "pv_predictions"})
    schema = pred_cfg["schema"]
    table = pred_cfg["table"]

    provider = get_provider()
    df = _collect_detections_from_fs(provider)

    if df.empty:
        st.warning("No `.detections.json` files found in the tiles directory.")
        return

    pv_count = df["has_pv"].sum()
    st.metric("Local detections", f"{len(df)} buildings ({pv_count} with PV)")

    st.dataframe(
        df[["building_id", "has_pv", "confidence", "model_name"]],
        width="stretch",
        hide_index=True,
    )

    existing = already_processed(engine, schema, table)
    new_ids = set(df["building_id"]) - existing
    overlap = set(df["building_id"]) & existing

    if existing:
        st.caption(f"{len(existing)} already in DB, {len(new_ids)} new, {len(overlap)} overlap")

    col1, col2 = st.columns(2)

    with col1:
        if st.button("Import new only", type="primary", disabled=len(new_ids) == 0):
            to_import = df[df["building_id"].isin(new_ids)]
            rows = load_predictions(engine, to_import, schema, table)
            st.success(f"Imported {rows} new records to `{schema}.{table}`")

    with col2:
        if st.button("Full refresh (truncate + reimport)"):
            truncate_predictions(engine, schema, table)
            rows = load_predictions(engine, df, schema, table)
            st.success(f"Truncated and reimported {rows} records to `{schema}.{table}`")


def page_download():
    st.header("Download Tiles")

    tab_coord, tab_db = st.tabs(["By Coordinates", "From Database"])

    with tab_coord:
        st.subheader("Download by coordinates")
        c1, c2, c3 = st.columns(3)
        lon = c1.number_input("Longitude", value=11.134, format="%.5f")
        lat = c2.number_input("Latitude", value=46.011, format="%.5f")
        size = c3.number_input("Tile size (m)", value=100, min_value=50, max_value=500, step=50)
        name = st.text_input("Filename (optional)", placeholder="auto-generated from coordinates")

        if st.button("Download tile", type="primary", key="dl_coord"):
            with st.spinner("Fetching from WMS..."):
                try:
                    dl = TileDownloader(TILE_DIR)
                    path = dl.fetch_tile(lon, lat, size_m=size, name=name or None)
                    if path:
                        st.success(f"Saved: {path.name}")
                        img = Image.open(path)
                        st.image(img, caption=path.name, width="stretch")
                        st.cache_resource.clear()
                    else:
                        st.error("Download failed — check coordinates are in Trentino")
                except Exception as e:
                    st.error(f"Error: {e}")

    with tab_db:
        st.subheader("Download from building database")
        engine = st.session_state.get("db_engine")
        if not engine:
            st.info("Connect to the database in the sidebar first.")
            return

        c1, c2 = st.columns(2)
        bbox_str = c1.text_input("Bounding box (lon_min,lat_min,lon_max,lat_max)", placeholder="11.0,45.9,11.3,46.1")
        limit = c2.number_input("Max buildings", value=10, min_value=1, max_value=100)
        size_db = st.number_input("Tile size (m)", value=100, min_value=50, max_value=500, step=50, key="size_db")

        if st.button("Download tiles for buildings", type="primary", key="dl_db"):
            if not bbox_str:
                st.warning("Enter a bounding box")
                return
            bbox = tuple(float(x.strip()) for x in bbox_str.split(","))
            with st.spinner(f"Fetching up to {limit} tiles..."):
                try:
                    dl = TileDownloader(TILE_DIR)
                    paths = dl.fetch_buildings_from_db(bbox=bbox, limit=limit, size_m=size_db)
                    st.success(f"Downloaded {len(paths)} tiles")
                    for p in paths:
                        st.caption(p.name)
                    st.cache_resource.clear()
                except Exception as e:
                    st.error(f"Error: {e}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    st.set_page_config(page_title="PV Detection Viewer", layout="wide")
    st.title("PV Detection — Tile Viewer")

    # Auto-connect to DB on first load
    if "db_init" not in st.session_state:
        st.session_state["db_init"] = True
        import sqlalchemy as sa
        try:
            engine = get_db_engine("localhost", "15432", "postgres", "securepassword123", "datasets")
            with engine.connect() as c:
                c.execute(sa.text("SELECT 1"))
            st.session_state["db_engine"] = engine
            st.session_state["db_connected"] = True
        except Exception:
            st.session_state["db_connected"] = False

    with st.sidebar:
        st.header("Database")
        import sqlalchemy as sa
        db_host = st.text_input("Host", "localhost")
        db_port = st.text_input("Port", "15432")
        db_user = st.text_input("User", "postgres")
        db_pass = st.text_input("Password", "securepassword123", type="password")
        db_name = st.text_input("Database", "datasets")

        if st.button("Reconnect", key="db_reconnect"):
            try:
                engine = get_db_engine(db_host, db_port, db_user, db_pass, db_name)
                with engine.connect() as c:
                    c.execute(sa.text("SELECT 1"))
                st.session_state["db_engine"] = engine
                st.session_state["db_connected"] = True
            except Exception as e:
                st.error(f"Connection failed: {e}")
                st.session_state.pop("db_engine", None)
                st.session_state["db_connected"] = False

        if st.session_state.get("db_connected"):
            st.success("Connected")
        else:
            st.warning("Not connected")

    tab_browse, tab_download, tab_db = st.tabs(["Browse & Detect", "Download Tiles", "DB Import"])

    with tab_browse:
        page_browse()

    with tab_download:
        page_download()

    with tab_db:
        page_db_import()


if __name__ == "__main__":
    main()
