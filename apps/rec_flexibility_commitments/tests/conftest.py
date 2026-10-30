"""Shared fixtures: a stub flexibility-api and a static token provider.

The stub is a real HTTP server on 127.0.0.1, so the fetch goes through the
celine-sdk client and its generated parser exactly as in production. Each test
sets the status and body the stub answers `GET /api/commitments/export` with.
Synthetic ids only: no real user, device or community.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest


class _Token:
    access_token = "test-token"


class StaticTokenProvider:
    async def get_token(self):
        return _Token()


def commitment(id_: str = "00000000-0000-0000-0000-000000000001") -> dict:
    """One export item, as flexibility-api serialises it."""
    return {
        "id": id_,
        "user_id": "user-ex-00001",
        "suggestion_id": "sugg-ex-00001",
        "suggestion_type": "shift_load",
        "community_id": "example-rec",
        "device_id": "device-ex-00001",
        "period_start": "2026-10-29T10:00:00+00:00",
        "period_end": "2026-10-29T11:00:00+00:00",
        "committed_at": "2026-10-29T08:00:00+00:00",
        "settled_at": None,
        "reminded_at": None,
        "status": "committed",
        "reward_points_estimated": 10,
        "reward_points_actual": None,
    }


class StubExport:
    """What the stub answers, and the requests it saw."""

    def __init__(self, base_url: str):
        self.base_url = base_url
        self.status = 200
        self.body: object = []
        self.paths: list[str] = []


@pytest.fixture
def stub_export():
    state: StubExport

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            state.paths.append(self.path)
            payload = json.dumps(state.body).encode()
            self.send_response(state.status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    state = StubExport(f"http://127.0.0.1:{server.server_address[1]}")
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield state
    finally:
        server.shutdown()
        server.server_close()


def run_flow(monkeypatch, pipeline, stub: StubExport, db_conn) -> dict:
    """Run the flow body once, its tasks as plain functions (no Prefect retries).

    The export is read from `stub` through the SDK; `db_conn` stands in for the
    flow's database connection.
    """
    monkeypatch.setenv("CELINE_ENV", "dev")
    monkeypatch.setattr(pipeline, "_db_conn", lambda cfg: db_conn())
    monkeypatch.setattr(
        pipeline,
        "_fetch_commitments",
        lambda cfg, created_after: pipeline._export_commitments(
            stub.base_url, StaticTokenProvider(), created_after
        ),
    )
    for name in ("ensure_table", "fetch_commitments", "mirror_to_db"):
        monkeypatch.setattr(pipeline, name, getattr(pipeline, name).fn)
    return pipeline.rec_flexibility_commitments_flow.fn({})
