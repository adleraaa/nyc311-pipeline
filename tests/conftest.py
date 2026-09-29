from __future__ import annotations

from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


def make_row(
    key: str,
    updated_at: str = "2026-09-20T01:33:00.000Z",
    created: str = "2026-09-10T08:00:00.000",
    **fields: str | None,
) -> dict:
    """An API-shaped row: only non-null fields are present, like the real JSON."""
    row = {
        ":id": f"row-{key}",
        ":created_at": "2026-09-11T01:33:00.000Z",
        ":updated_at": updated_at,
        "unique_key": key,
        "created_date": created,
        "agency": "DSNY",
        "complaint_type": "Dirty Condition",
        "status": "Open",
        "borough": "BROOKLYN",
    }
    row.update(fields)
    return {k: v for k, v in row.items() if v is not None}


@pytest.fixture
def sample_fixture_path() -> Path:
    return FIXTURES / "sample_311.json.gz"


@pytest.fixture(autouse=True)
def _no_stale_replica_wait(monkeypatch):
    """Tests simulate stale replicas; do not actually wait between retries."""
    monkeypatch.setattr("nyc311.socrata.STALE_WAIT_SECONDS", 0.0)
