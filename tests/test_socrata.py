from __future__ import annotations

from urllib.parse import parse_qs, urlparse

import pytest
import requests
import responses

from nyc311.socrata import (
    DATASET_URL,
    STALE_ATTEMPTS,
    StaleReplicaError,
    Watermark,
    build_where,
    fetch_pages,
)
from tests.conftest import make_row


def test_build_where_backfill_has_only_window_filter():
    assert build_where("2026-09-01T00:00:00", None) == "created_date >= '2026-09-01T00:00:00'"


def test_build_where_incremental_uses_keyset_tiebreaker():
    where = build_where("2026-09-01T00:00:00", Watermark("2026-09-28T01:33:33.096Z", "70555073"))
    assert "created_date >= '2026-09-01T00:00:00'" in where
    assert ":updated_at > '2026-09-28T01:33:33.096Z'" in where
    # Same timestamp must fall back to the key, otherwise rows sharing the
    # batch timestamp after a page boundary would be skipped.
    assert "(:updated_at = '2026-09-28T01:33:33.096Z' AND unique_key > '70555073')" in where


def _where_params(call) -> str:
    return parse_qs(urlparse(call.request.url).query)["$where"][0]


@responses.activate
def test_fetch_pages_advances_keyset_between_pages():
    same_ts = "2026-09-28T01:33:33.096Z"
    page1 = [make_row("100", same_ts), make_row("101", same_ts)]
    page2 = [make_row("102", same_ts)]
    responses.get(DATASET_URL, json=page1)
    responses.get(DATASET_URL, json=page2)

    pages = list(fetch_pages(requests.Session(), "2026-09-01T00:00:00", None, page_size=2))

    assert [len(rows) for rows, _ in pages] == [2, 1]
    assert pages[0][1] == Watermark(same_ts, "101")
    assert pages[1][1] == Watermark(same_ts, "102")
    # A short page ends the stream: exactly two requests were made.
    assert len(responses.calls) == 2
    second_where = _where_params(responses.calls[1])
    assert f"(:updated_at = '{same_ts}' AND unique_key > '101')" in second_where
    params = parse_qs(urlparse(responses.calls[0].request.url).query)
    assert params["$order"] == [":updated_at, unique_key"]
    assert params["$limit"] == ["2"]


@responses.activate
def test_fetch_pages_stops_on_empty_page_after_exact_multiple():
    responses.get(DATASET_URL, json=[make_row("1"), make_row("2")])
    responses.get(DATASET_URL, json=[])

    pages = list(fetch_pages(requests.Session(), "2026-09-01T00:00:00", None, page_size=2))

    assert len(pages) == 1
    assert len(responses.calls) == 2


@responses.activate
def test_fetch_pages_raises_on_http_error():
    responses.get(DATASET_URL, status=400, json={"message": "bad query"})
    gen = fetch_pages(requests.Session(), "2026-09-01T00:00:00", None)
    try:
        next(gen)
    except requests.HTTPError:
        pass
    else:
        raise AssertionError("expected HTTPError")


STALE = {"X-SODA2-Data-Out-Of-Date": "true"}


@responses.activate
def test_stale_replica_answer_is_retried_not_trusted():
    # A stale replica answers "nothing after the watermark"; a fresh one has rows.
    responses.get(DATASET_URL, json=[], headers=STALE)
    responses.get(DATASET_URL, json=[make_row("1")], headers={"X-SODA2-Data-Out-Of-Date": "false"})

    pages = list(fetch_pages(requests.Session(), "2026-09-01T00:00:00", None, page_size=2))

    assert [len(rows) for rows, _ in pages] == [1]
    assert len(responses.calls) == 2


@responses.activate
def test_persistently_stale_replica_raises():
    responses.get(DATASET_URL, json=[], headers=STALE)
    with pytest.raises(StaleReplicaError):
        list(fetch_pages(requests.Session(), "2026-09-01T00:00:00", None))
    assert len(responses.calls) == STALE_ATTEMPTS
