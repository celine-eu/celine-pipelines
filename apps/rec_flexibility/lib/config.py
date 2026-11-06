"""Load flexibility_config.yaml and expose typed tier helpers."""

from __future__ import annotations

import csv
from datetime import date
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


def get_fair_params(cfg: dict[str, Any]) -> dict[str, Any]:
    """Return the fair-gamification block (`fair:`), mirrored one-to-one by dbt vars `fair_*`."""
    return dict(cfg["fair"])


# ─── Fair gamification: overrides seed (spec 2026-10-07 §4.7) ────────────────────

OVERRIDES_SEED_NAME = "rec_device_overrides.csv"
OVERRIDES_COLUMNS = (
    "device_id",
    "pv_class_override",
    "category_override",
    "quarantine_from",
    "quarantine_reason",
    "battery_declared",
)
_PV_CLASSES = {"", "pv", "no_pv"}
_CATEGORIES = {"", "household", "public_entity", "business"}
_BOOLS = {"true": "true", "false": "false", "1": "true", "0": "false", "": "false"}


def _validate_override_row(row: list[str], line_no: int) -> list[str]:
    """Validate one CSV row of the overrides seed and normalise booleans."""
    if len(row) != len(OVERRIDES_COLUMNS):
        raise ValueError(f"overrides line {line_no}: expected {len(OVERRIDES_COLUMNS)} columns, got {len(row)}")
    device_id, pv, cat, q_from, q_reason, battery = (c.strip() for c in row)
    if not device_id:
        raise ValueError(f"overrides line {line_no}: empty device_id")
    if pv not in _PV_CLASSES:
        raise ValueError(f"overrides line {line_no}: pv_class_override must be pv|no_pv, got {pv!r}")
    if cat not in _CATEGORIES:
        raise ValueError(f"overrides line {line_no}: category_override must be household|public_entity|business, got {cat!r}")
    if q_from:
        try:
            date.fromisoformat(q_from)
        except ValueError as exc:
            raise ValueError(f"overrides line {line_no}: quarantine_from must be YYYY-MM-DD, got {q_from!r}") from exc
    if battery.lower() not in _BOOLS:
        raise ValueError(f"overrides line {line_no}: battery_declared must be true|false, got {battery!r}")
    return [device_id, pv, cat, q_from, q_reason, _BOOLS[battery.lower()]]


def write_overrides_seed(seed_dir: Path, csv_text: str | None) -> Path:
    """Write the fair-gamification overrides seed from the REC_FAIR_OVERRIDES_CSV text.

    The seed is gitignored: it may carry device IDs, which never enter the repository.
    When ``csv_text`` is empty the file holds the header only, so ``dbt seed`` still
    creates the (empty) table every model can ``ref``.

    Args:
        seed_dir: The dbt ``seeds`` directory.
        csv_text: CSV rows (with or without the header line), or None.

    Returns:
        Path of the written seed file.

    Raises:
        ValueError: On a malformed row.
    """
    rows: list[list[str]] = []
    for line_no, raw in enumerate((csv_text or "").splitlines(), start=1):
        if not raw.strip():
            continue
        cells = next(csv.reader([raw]))
        if [c.strip() for c in cells] == list(OVERRIDES_COLUMNS):
            continue
        rows.append(_validate_override_row(cells, line_no))
    seed_dir.mkdir(parents=True, exist_ok=True)
    target = seed_dir / OVERRIDES_SEED_NAME
    with target.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(OVERRIDES_COLUMNS)
        writer.writerows(rows)
    return target
