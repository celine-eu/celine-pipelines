from abc import ABC, abstractmethod
from pathlib import Path
import json
import logging

logger = logging.getLogger(__name__)


class TileProvider(ABC):
    @abstractmethod
    def get_tile(self, building_id: str, bbox: tuple[float, float, float, float] | None = None) -> bytes | None:
        """Fetch tile for a building. bbox is (xmin, ymin, xmax, ymax) in provider CRS."""
        ...


class FilesystemProvider(TileProvider):
    """Loads pre-downloaded tiles from a directory. Matches by building_id prefix in filename."""

    EXTENSIONS = (".jpg", ".jpeg", ".png")

    def __init__(self, tile_dir: str):
        self.tile_dir = Path(tile_dir)
        if not self.tile_dir.is_dir():
            raise FileNotFoundError(f"Tile directory not found: {self.tile_dir}")
        self._index = self._build_index()
        logger.info("FilesystemProvider: indexed %d tiles in %s", len(self._index), self.tile_dir)

    def _build_index(self) -> dict[str, Path]:
        index = {}
        for f in self.tile_dir.iterdir():
            if f.suffix.lower() in self.EXTENSIONS:
                index[f.stem] = f
                meta = self._read_meta(f)
                if meta and "building_id" in meta:
                    index[meta["building_id"]] = f
        return index

    def _read_meta(self, image_path: Path) -> dict | None:
        meta_path = image_path.parent / f"{image_path.name}.json"
        if meta_path.exists():
            try:
                return json.loads(meta_path.read_text())
            except (json.JSONDecodeError, OSError):
                return None
        return None

    def get_tile(self, building_id: str, bbox=None) -> bytes | None:
        path = self._index.get(building_id)
        if path and path.exists():
            return path.read_bytes()
        for bid, p in self._index.items():
            if bid.startswith(building_id[:8]) or building_id.startswith(bid[:8]):
                return p.read_bytes()
        return None

    def get_metadata(self, building_id: str) -> dict | None:
        path = self._index.get(building_id)
        if path:
            return self._read_meta(path)
        return None

    def available_ids(self) -> set[str]:
        return set(self._index.keys())


class WmsProvider(TileProvider):
    def __init__(self, config: dict):
        raise NotImplementedError("WmsProvider not yet implemented")

    def get_tile(self, building_id: str, bbox=None) -> bytes | None:
        raise NotImplementedError


APP_DIR = Path(__file__).parent.parent


def _resolve_path(p: str) -> Path:
    """Resolve a path relative to the app directory."""
    path = Path(p)
    if not path.is_absolute():
        path = APP_DIR / path
    return path.resolve()


def create_provider(config: dict) -> TileProvider:
    provider_type = config.get("type", "filesystem")
    if provider_type == "filesystem":
        tile_dir = str(_resolve_path(config["filesystem"]["tile_dir"]))
        return FilesystemProvider(tile_dir)
    elif provider_type == "wms":
        return WmsProvider(config.get("wms", {}))
    else:
        raise ValueError(f"Unknown provider type: {provider_type}")
