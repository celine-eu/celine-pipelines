"""Load flexibility_config.yaml and expose typed tier helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "flexibility_config.yaml"

def load_config(path: Path | None = None) -> dict[str, Any]:
    """Load flexibility_config.yaml as a dict.

    Args:
        path: Optional override for the config path. Defaults to the production
            flexibility_config.yaml in pipelines/apps/rec_flexibility/.

    Returns:
        Parsed YAML as a dict with keys: baseline, settlement, flexibility_bonus,
        anti_gaming.
    """
    config_path = path or DEFAULT_CONFIG_PATH
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _tiers_from_dictlist(items: list[dict], key: str) -> list[tuple[float, float]]:
    return sorted(
        [(float(item[key]), float(item["multiplier"])) for item in items],
        key=lambda t: t[0],
    )


def get_effort_tiers(cfg: dict[str, Any]) -> list[tuple[float, float]]:
    """Return settlement effort multiplier tiers as (ratio_min, multiplier) sorted ascending."""
    return _tiers_from_dictlist(cfg["settlement"]["effort_multiplier_tiers"], "ratio_min")


def get_shift_effort_tiers(cfg: dict[str, Any]) -> list[tuple[float, float]]:
    """Return shift-effort tiers as (shift_pct_min, multiplier) sorted ascending."""
    return _tiers_from_dictlist(
        cfg["flexibility_bonus"]["shift_effort_tiers"], "shift_pct_min"
    )


def get_event_tiers(cfg: dict[str, Any]) -> list[tuple[float, float]]:
    """Return event multiplier tiers as (ratio_min, multiplier) sorted ascending."""
    return _tiers_from_dictlist(cfg["flexibility_bonus"]["event_multiplier_tiers"], "ratio_min")
