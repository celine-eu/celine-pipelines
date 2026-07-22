"""Tests for the config loader."""

from __future__ import annotations

from lib import config as cfg_mod


def test_load_config_returns_required_sections(config_path):
    cfg = cfg_mod.load_config(config_path)
    assert set(cfg.keys()) >= {"baseline", "settlement", "flexibility_bonus", "anti_gaming"}


def test_get_active_devices_reads_env_first(monkeypatch):
    monkeypatch.setenv("REC_ACTIVE_DEVICES", " dev-A, dev-B ,dev-C ")
    # env wins even when the yaml carries a (legacy/local) list
    assert cfg_mod.get_active_devices({"fleet": {"active_devices": ["ignored"]}}) == [
        "dev-A",
        "dev-B",
        "dev-C",
    ]


def test_get_active_devices_falls_back_to_yaml_when_env_unset(monkeypatch):
    monkeypatch.delenv("REC_ACTIVE_DEVICES", raising=False)
    assert cfg_mod.get_active_devices({"fleet": {"active_devices": ["x", "y"]}}) == ["x", "y"]


def test_get_active_devices_empty_when_nothing_configured(monkeypatch):
    monkeypatch.delenv("REC_ACTIVE_DEVICES", raising=False)
    assert cfg_mod.get_active_devices({}) == []
    assert cfg_mod.get_active_devices({"fleet": {"active_devices": []}}) == []


def test_committed_config_carries_no_device_ids(config_path):
    """Governance: the committed yaml must not contain private device IDs."""
    assert "c2g-" not in config_path.read_text(encoding="utf-8")


def test_write_active_devices_seed(tmp_path):
    seed_path = tmp_path / "seeds" / "rec_active_devices.csv"
    n = cfg_mod.write_active_devices_seed(["dev-A", "dev-B"], seed_path)
    assert n == 2
    assert seed_path.read_text(encoding="utf-8").splitlines() == [
        "device_id",
        "dev-A",
        "dev-B",
    ]


def test_write_active_devices_seed_empty_fleet_writes_header_only(tmp_path):
    seed_path = tmp_path / "seeds" / "rec_active_devices.csv"
    n = cfg_mod.write_active_devices_seed([], seed_path)
    assert n == 0
    assert seed_path.read_text(encoding="utf-8").splitlines() == ["device_id"]


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
