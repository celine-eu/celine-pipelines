"""Load flexibility_config.yaml and expose typed tier helpers."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Iterable

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[1] / "flexibility_config.yaml"

#: Env var holding the comma-separated active-device fleet. Primary source of truth at
#: runtime — the device IDs are private demo data and are kept OUT of git (set via the
#: sops-encrypted deploy values). The yaml ``fleet.active_devices`` is only a local-dev
#: fallback and ships empty in the repo.
ACTIVE_DEVICES_ENV = "REC_ACTIVE_DEVICES"


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


def get_active_devices(cfg: dict[str, Any]) -> list[str]:
    """Return the fleet of devices the points pipeline is scoped to.

    Source order:
        1. ``REC_ACTIVE_DEVICES`` env var (comma-separated) — the authoritative
           runtime source, injected via sops-encrypted deploy values. Private
           device IDs are kept out of git, so this is normally how the fleet is set.
        2. ``fleet.active_devices`` in the parsed yaml — a local-dev fallback only;
           it ships empty in the repo.

    Returns an empty list when neither is configured (callers treat an empty list as
    "no scoping"). The dbt seed ``rec_active_devices.csv`` is generated from this list
    at flow start (see :func:`write_active_devices_seed`).

    Args:
        cfg: Parsed config dict from :func:`load_config`.

    Returns:
        List of device_id strings.
    """
    env_value = os.environ.get(ACTIVE_DEVICES_ENV, "").strip()
    if env_value:
        return [token.strip() for token in env_value.split(",") if token.strip()]
    return list(cfg.get("fleet", {}).get("active_devices", []) or [])


def write_active_devices_seed(devices: Iterable[str], seed_path: Path | str) -> int:
    """Write the dbt seed CSV (``device_id`` header + one row per device).

    Generated at flow start from :func:`get_active_devices` so dbt can resolve
    ``ref('rec_active_devices')`` without the private fleet ever living in git. An
    empty fleet writes a header-only file (dbt still parses; scope resolves to none).

    Args:
        devices: Device IDs to write.
        seed_path: Destination CSV path (parent dirs are created).

    Returns:
        Number of device rows written (excludes the header).
    """
    devices = list(devices)
    path = Path(seed_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(["device_id", *devices]) + "\n", encoding="utf-8")
    return len(devices)


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
