"""Tests for the sync scraper."""

from __future__ import annotations

import sqlite3

import httpx
import pytest
import respx
from jellyjelly.client import JellyClient
from jellyjelly.models import JellyDetail

from jellydash.db.queries import get_existing_ids, get_jelly_by_id, upsert_jelly_detail
from jellydash.sync.scraper import discover_jelly_ids, fetch_details
from tests.conftest import make_jelly_detail_dict


@pytest.fixture()
def mock_api() -> respx.MockRouter:
    """Return a respx mock router."""
    with respx.mock(
        base_url="https://api.jellyjelly.com",
        assert_all_called=False,
    ) as router:
        yield router


def _search_response(jelly_ids: list[str]) -> dict:  # type: ignore[type-arg]
    """Build a search response with the given IDs."""
    jellies = [
        {
            "id": jid,
            "title": f"Jelly {jid}",
            "participants": [],
            "posted_at": "2026-02-28T14:30:00Z",
        }
        for jid in jelly_ids
    ]
    return {
        "jellies": jellies,
        "total": len(jellies),
        "page": 1,
        "page_size": 100,
    }


@pytest.mark.asyncio
async def test_discover_jelly_ids(
    mock_api: respx.MockRouter, db: sqlite3.Connection
) -> None:
    """Discovery finds new IDs not already in DB."""
    mock_api.get("/v3/jelly/search").mock(
        return_value=httpx.Response(
            200, json=_search_response(["new-1", "new-2", "existing-1"])
        )
    )

    # Seed one existing ID
    data = make_jelly_detail_dict(jelly_id="existing-1")
    upsert_jelly_detail(db, JellyDetail.model_validate(data))

    async with JellyClient() as client:
        existing = get_existing_ids(db)
        new_ids = await discover_jelly_ids(
            client, existing, max_pages_per_query=1, page_size=100
        )

    assert "new-1" in new_ids
    assert "new-2" in new_ids
    assert "existing-1" not in new_ids


@pytest.mark.asyncio
async def test_fetch_details(
    mock_api: respx.MockRouter, db: sqlite3.Connection
) -> None:
    """fetch_details fetches and stores jelly details."""
    detail_data = make_jelly_detail_dict(jelly_id="fetch-1")
    mock_api.get("/v3/jelly/fetch-1").mock(
        return_value=httpx.Response(200, json={"jelly": detail_data})
    )

    async with JellyClient() as client:
        success, errors = await fetch_details(client, ["fetch-1"], db)

    assert success == 1
    assert errors == 0
    assert get_jelly_by_id(db, "fetch-1") is not None


@pytest.mark.asyncio
async def test_fetch_details_handles_errors(
    mock_api: respx.MockRouter, db: sqlite3.Connection
) -> None:
    """fetch_details counts errors gracefully."""
    mock_api.get("/v3/jelly/bad-id").mock(
        return_value=httpx.Response(404, text="Not found")
    )

    async with JellyClient(max_retries=0) as client:
        success, errors = await fetch_details(client, ["bad-id"], db)

    assert success == 0
    assert errors == 1
