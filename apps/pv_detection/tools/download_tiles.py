"""
Download aerial orthophoto tiles from the Trentino WMS proxy.

Usage:
    # Download a single tile by WGS84 coordinates (lon, lat):
    python tools/download_tiles.py --lon 11.134 --lat 46.011 --size 100

    # Download tiles for buildings from DB in a bounding box:
    python tools/download_tiles.py --bbox 11.0,45.9,11.3,46.1 --limit 20

    # Download tiles for specific building IDs:
    python tools/download_tiles.py --building-ids 0dd0b8aa13c24b4cb6ead40d9c4556fb,26c875395eb98c1e4c9b85d3c2b9cc43
"""

import argparse
import json
import logging
import os
from pathlib import Path

import requests
from pyproj import Transformer

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

APP_DIR = Path(__file__).parent.parent
DEFAULT_TILE_DIR = APP_DIR / "data" / "tiles"

WGS84_TO_UTM32 = Transformer.from_crs("EPSG:4326", "EPSG:25832", always_xy=True)

WMS_PROXY = (
    "https://webgis.provincia.tn.it/wgt/services/ogcproxy/wms"
    "?url=https%3A%2F%2Fgeoservices.cloud-intra.tn.it%2Fgeoserver%2Fows%3F"
)
WMS_LAYER = "pub_stem%3Aortofoto_agea_2023"
SESSION_URL = "https://webgis.provincia.tn.it/wgt/index.html?topic=1&lang=it&bgLayer=sfondo&layers=orto2023"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36",
    "Referer": "https://webgis.provincia.tn.it/wgt/index.html",
    "Origin": "https://webgis.provincia.tn.it",
    "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
    "Sec-Fetch-Dest": "image",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}


class TileDownloader:
    def __init__(self, tile_dir: Path | None = None):
        self.tile_dir = tile_dir or DEFAULT_TILE_DIR
        self.tile_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._init_session()

    def _init_session(self):
        resp = self.session.get(SESSION_URL, timeout=15)
        resp.raise_for_status()
        logger.info("Session initialized")

    def fetch_tile(
        self,
        lon: float,
        lat: float,
        size_m: int = 100,
        image_px: int = 512,
        name: str | None = None,
        building_id: str | None = None,
        extra_meta: dict | None = None,
    ) -> Path | None:
        cx, cy = WGS84_TO_UTM32.transform(lon, lat)
        half = size_m / 2
        xmin, ymin = int(cx - half), int(cy - half)
        xmax, ymax = int(cx + half), int(cy + half)

        url = (
            f"{WMS_PROXY}"
            f"&SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap"
            f"&FORMAT=image%2Fjpeg&TRANSPARENT=true"
            f"&LAYERS={WMS_LAYER}"
            f"&CRS=EPSG%3A25832&STYLES="
            f"&WIDTH={image_px}&HEIGHT={image_px}"
            f"&BBOX={xmin}%2C{ymin}%2C{xmax}%2C{ymax}"
        )

        resp = self.session.get(url, timeout=30)

        if resp.status_code != 200 or len(resp.content) < 1000:
            logger.warning("Failed to fetch tile at %.5f,%.5f: HTTP %d, %d bytes", lon, lat, resp.status_code, len(resp.content))
            return None

        fname = name or building_id or f"{lon:.5f}_{lat:.5f}"
        img_path = self.tile_dir / f"{fname}.jpg"
        img_path.write_bytes(resp.content)

        meta = {
            "source": "AGEA 2023 via PAT proxy",
            "layer": "pub_stem:ortofoto_agea_2023",
            "crs": "EPSG:25832",
            "bbox_25832": [xmin, ymin, xmax, ymax],
            "centroid_25832": [int(cx), int(cy)],
            "centroid_wgs84": [lon, lat],
            "tile_size_m": size_m,
            "image_size_px": [image_px, image_px],
            "pixel_size_m": round(size_m / image_px, 4),
        }
        if building_id:
            meta["building_id"] = building_id
        if extra_meta:
            meta.update(extra_meta)

        meta_path = self.tile_dir / f"{fname}.jpg.json"
        meta_path.write_text(json.dumps(meta, indent=2))

        logger.info("Saved %s (%d bytes)", img_path.name, len(resp.content))
        return img_path

    def fetch_buildings_from_db(
        self,
        bbox: tuple[float, float, float, float] | None = None,
        building_ids: list[str] | None = None,
        limit: int = 20,
        size_m: int = 100,
        db_url: str | None = None,
    ) -> list[Path]:
        import sqlalchemy as sa

        db_url = db_url or (
            f"postgresql://{os.environ.get('POSTGRES_USER', 'postgres')}"
            f":{os.environ.get('POSTGRES_PASSWORD', 'securepassword123')}"
            f"@{os.environ.get('POSTGRES_HOST', 'localhost')}"
            f":{os.environ.get('POSTGRES_PORT', '15432')}"
            f"/{os.environ.get('POSTGRES_DB', 'datasets')}"
        )
        engine = sa.create_engine(db_url)

        where = []
        params: dict = {}

        if bbox:
            where.append("ST_Intersects(geometry, ST_MakeEnvelope(:xmin, :ymin, :xmax, :ymax, 4326))")
            params.update(xmin=bbox[0], ymin=bbox[1], xmax=bbox[2], ymax=bbox[3])

        if building_ids:
            where.append("building_id = ANY(:bids)")
            params["bids"] = building_ids

        where_sql = " AND ".join(where) if where else "TRUE"

        query = sa.text(f"""
            SELECT building_id,
                   ST_X(ST_Centroid(geometry)) as lon,
                   ST_Y(ST_Centroid(geometry)) as lat,
                   footprint_area_m2::int as area_m2
            FROM ds_dev_silver.pv_overture_buildings
            WHERE {where_sql}
            ORDER BY random()
            LIMIT :lim
        """)
        params["lim"] = limit

        with engine.connect() as conn:
            rows = conn.execute(query, params).mappings().all()

        logger.info("Found %d buildings from DB", len(rows))

        paths = []
        for row in rows:
            bid = row["building_id"]
            fname = f"{bid[:8]}_{row['area_m2']}m2"
            path = self.fetch_tile(
                lon=row["lon"],
                lat=row["lat"],
                size_m=size_m,
                name=fname,
                building_id=bid,
                extra_meta={
                    "footprint_area_m2": row["area_m2"],
                    "fetched_from": "ds_dev_silver.pv_overture_buildings",
                },
            )
            if path:
                paths.append(path)

        return paths


def main():
    parser = argparse.ArgumentParser(description="Download aerial tiles from Trentino WMS")
    parser.add_argument("--lon", type=float, help="Longitude (WGS84)")
    parser.add_argument("--lat", type=float, help="Latitude (WGS84)")
    parser.add_argument("--size", type=int, default=100, help="Tile size in meters (default: 100)")
    parser.add_argument("--bbox", help="Bounding box: xmin,ymin,xmax,ymax in WGS84")
    parser.add_argument("--building-ids", help="Comma-separated building IDs")
    parser.add_argument("--limit", type=int, default=20, help="Max buildings to download (default: 20)")
    parser.add_argument("--output", type=str, help="Output directory")
    parser.add_argument("--name", type=str, help="Output filename (for single tile)")
    args = parser.parse_args()

    tile_dir = Path(args.output) if args.output else DEFAULT_TILE_DIR
    dl = TileDownloader(tile_dir)

    if args.lon and args.lat:
        dl.fetch_tile(args.lon, args.lat, size_m=args.size, name=args.name)
    elif args.bbox:
        parts = [float(x) for x in args.bbox.split(",")]
        dl.fetch_buildings_from_db(bbox=tuple(parts), limit=args.limit, size_m=args.size)
    elif args.building_ids:
        bids = [b.strip() for b in args.building_ids.split(",")]
        dl.fetch_buildings_from_db(building_ids=bids, size_m=args.size)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
