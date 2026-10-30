"""A failed export raises; only a 200 with a list is an export.

The mirror is replaced by what the export returns, the empty list included, so
the flow must never mistake a refused or broken request for "no commitments".
The SDK client is built with `raise_on_unexpected_status=True` and a 422 body
cannot be read as a list: every non-200 raises out of `export_commitments`. These
tests hold the SDK to that, so a release that turns a failure into an empty list
fails here before it can empty the mirror. Which exception does not matter (2.0.0
raises UnexpectedStatus or a parse error; a later release its own error type),
only that one is raised.

A stub flexibility-api answers over real HTTP (see conftest). No database.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from flows import pipeline

from tests.conftest import StaticTokenProvider, commitment

CREATED_AFTER = datetime(2026, 8, 1, tzinfo=UTC)


def _export(stub):
    return asyncio.run(
        pipeline._export_commitments(
            stub.base_url, StaticTokenProvider(), CREATED_AFTER
        )
    )


def test_an_empty_export_is_an_empty_list(stub_export):
    stub_export.body = []

    assert _export(stub_export) == []


def test_the_export_is_asked_for_the_window(stub_export):
    _export(stub_export)

    [path] = stub_export.paths
    assert path.startswith("/api/commitments/export?created_after=2026-08-01T00")


def test_an_export_item_becomes_a_row(stub_export):
    stub_export.body = [commitment()]

    [row] = _export(stub_export)

    assert str(row["id"]) == "00000000-0000-0000-0000-000000000001"
    assert row["user_id"] == "user-ex-00001"
    assert pipeline._to_tuples([row])[0][0] == "00000000-0000-0000-0000-000000000001"


@pytest.mark.parametrize("status", [401, 403, 404, 422, 500, 502, 503])
def test_a_non_200_raises(stub_export, status):
    stub_export.status = status
    stub_export.body = {"detail": []}

    with pytest.raises(Exception):  # noqa: B017 — any error aborts the run
        _export(stub_export)


@pytest.mark.parametrize("body", [None, {"detail": "not a list"}])
def test_a_200_that_is_not_a_list_raises(stub_export, body):
    stub_export.body = body

    with pytest.raises(Exception):  # noqa: B017 — any error aborts the run
        _export(stub_export)
