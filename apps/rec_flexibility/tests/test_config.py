"""Tests for the config loader."""

from __future__ import annotations

from pathlib import Path

from lib import config as cfg_mod


def test_load_config_returns_required_sections(config_path):
    cfg = cfg_mod.load_config(config_path)
    assert set(cfg.keys()) >= {"baseline", "settlement", "flexibility_bonus", "anti_gaming"}


def test_the_fleet_is_not_configurable():
    """The fleet is the registry's membership: no env var, yaml list or seed sets it."""
    app = Path(cfg_mod.__file__).resolve().parents[1]
    sources = [*app.glob("flows/*.py"), *app.glob("lib/*.py"), *app.glob("dbt/models/**/*.sql")]

    assert not hasattr(cfg_mod, "get_active_devices")
    assert "fleet" not in cfg_mod.load_config()
    for path in sources:
        text = path.read_text(encoding="utf-8")
        assert "REC_ACTIVE_DEVICES" not in text, path
        assert "rec_active_devices" not in text, path


def test_committed_config_carries_no_device_ids(config_path):
    """Governance: the committed yaml must not contain private device IDs."""
    assert "c2g-" not in config_path.read_text(encoding="utf-8")


def test_get_effort_tiers_sorted(config_path):
    cfg = cfg_mod.load_config(config_path)
    tiers = cfg_mod.get_effort_tiers(cfg)
    assert tiers == sorted(tiers, key=lambda t: t[0])
    assert tiers[0] == (0.0, 0.25)


def test_get_shift_effort_tiers_sorted(config_path):
    cfg = cfg_mod.load_config(config_path)
    tiers = cfg_mod.get_shift_effort_tiers(cfg)
    assert tiers == sorted(tiers, key=lambda t: t[0])


def test_get_event_tiers_sorted(config_path):
    cfg = cfg_mod.load_config(config_path)
    tiers = cfg_mod.get_event_tiers(cfg)
    assert tiers == sorted(tiers, key=lambda t: t[0])


def test_window_promise_config_keys(config_path):
    cfg = cfg_mod.load_config(config_path)
    wp = cfg["window_promise"]
    assert wp["shift_q_hi"] == 0.75
    assert wp["shift_q_lo"] == 0.5
    assert wp["clear_top_days"] == 7
    assert wp["pv_export_threshold_kwh"] == 1.0
    assert wp["lookback_days"] == 35
    assert wp["min_history_days"] == 14
    assert wp["calibration_lambda"] == 1.0


# ─── Fair gamification: runtime-written overrides seed ───────────────────────────

OVERRIDES_HEADER = "device_id,pv_class_override,category_override,quarantine_from,quarantine_reason,battery_declared"


def test_write_overrides_seed_header_only_when_env_empty(tmp_path):
    """Without REC_FAIR_OVERRIDES_CSV the seed is written with the header only."""
    p = cfg_mod.write_overrides_seed(tmp_path, None)
    assert p.name == "rec_device_overrides.csv"
    assert p.read_text(encoding="utf-8").splitlines() == [OVERRIDES_HEADER]


def test_write_overrides_seed_appends_validated_rows(tmp_path):
    """Rows from the env text are validated and written after the header."""
    text = "c2g-X,pv,,2026-06-01,m1_scaled,false\nc2g-Y,,public_entity,,,true\n"
    p = cfg_mod.write_overrides_seed(tmp_path, text)
    lines = p.read_text(encoding="utf-8").splitlines()
    assert lines[0] == OVERRIDES_HEADER and len(lines) == 3
    assert lines[1] == "c2g-X,pv,,2026-06-01,m1_scaled,false"


def test_write_overrides_seed_accepts_text_with_header(tmp_path):
    """A pasted CSV that already carries the header is not duplicated."""
    text = OVERRIDES_HEADER + "\nc2g-X,,,,,false\n"
    lines = cfg_mod.write_overrides_seed(tmp_path, text).read_text(encoding="utf-8").splitlines()
    assert lines == [OVERRIDES_HEADER, "c2g-X,,,,,false"]


def test_write_overrides_seed_rejects_bad_values(tmp_path):
    import pytest

    with pytest.raises(ValueError):
        cfg_mod.write_overrides_seed(tmp_path, "c2g-X,solar,,,,false")
    with pytest.raises(ValueError):
        cfg_mod.write_overrides_seed(tmp_path, "c2g-X,,shop,,,false")
    with pytest.raises(ValueError):
        cfg_mod.write_overrides_seed(tmp_path, "c2g-X,,,not-a-date,,false")
    with pytest.raises(ValueError):
        cfg_mod.write_overrides_seed(tmp_path, "c2g-X,,,,,maybe")


def test_overrides_seed_is_gitignored():
    """The seed may hold device IDs, so it never enters git."""
    repo_root = Path(cfg_mod.__file__).resolve().parents[3]
    assert "apps/rec_flexibility/dbt/seeds/rec_device_overrides.csv" in (repo_root / ".gitignore").read_text(encoding="utf-8")


# ─── Fair gamification: config block ↔ dbt vars ──────────────────────────────────

_BFF_ONLY = {"points_per_level"}  # consumed by the webapp BFF, no dbt counterpart


def test_fair_block_matches_dbt_vars(config_path):
    """Every `fair.<key>` in flexibility_config.yaml equals dbt var `fair_<key>`."""
    import yaml

    fair = cfg_mod.load_config(config_path)["fair"]
    dbt_vars = yaml.safe_load((config_path.parent / "dbt" / "dbt_project.yml").read_text(encoding="utf-8"))["vars"]
    assert set(fair) - _BFF_ONLY <= {k[len("fair_"):] for k in dbt_vars if k.startswith("fair_")}
    for key, value in fair.items():
        if key in _BFF_ONLY:
            continue
        assert dbt_vars[f"fair_{key}"] == value, key


def test_get_fair_params_returns_the_block(config_path):
    cfg = cfg_mod.load_config(config_path)
    fair = cfg_mod.get_fair_params(cfg)
    assert fair["night_start"] == "21:00" and fair["night_end"] == "06:00"
    assert fair["valid_day_min_slots"] == 87 and fair["min_group_size"] == 6
    assert fair["points_per_kept"] == 5 and fair["actions_weekly_cap"] == 20
