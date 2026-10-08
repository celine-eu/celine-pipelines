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
