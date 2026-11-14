"""Tests for featured.py and the crawler modes that rebuild the featured list."""

from unittest.mock import MagicMock, patch

import pytest

from gs_crawler import featured, main


ROWS = [
    dict(scholar_id="6-e-ZBEAAAAJ", name="Dario Amodei", affiliation="Anthropic", hindex=69, citedby=207728,
         pip_score=0.5812, pip_percentile=0.861, pip_auc_score=0.5051, pip_auc_percentile=0.843,
         wikipedia_title="Dario Amodei", wikipedia_views_60d=902369),
    dict(scholar_id="WhhC4LwAAAAJ", name="Karl Deisseroth", affiliation="Stanford", hindex=180, citedby=140003,
         pip_score=0.8920, pip_percentile=0.991, pip_auc_score=0.8197, pip_auc_percentile=0.987,
         wikipedia_title="Karl Deisseroth", wikipedia_views_60d=402095),
]


def _bq(rows):
    bq = MagicMock()
    bq.query.return_value.result.return_value = rows
    return bq


def test_query_leaves_out_deceased_and_stub_profiles():
    sql = featured.featured_query()
    assert "NOT COALESCE(w.died, FALSE)" in sql
    assert "a.hindex >= @min_hindex" in sql
    assert "ORDER BY w.enwiki_views_60d DESC" in sql
    assert "p.pip_score" in sql
    assert "p.pip_score_percentile AS pip_percentile" in sql
    assert "`scholar-version2.gs_statistics.ranked_author_current`" in sql
    assert "`scholar-version2.gs_data.wikidata_authors`" in sql


def test_select_passes_limit_and_min_hindex():
    bq = _bq(ROWS)
    entries = featured.select_featured(bq, limit=5)
    assert [e["name"] for e in entries] == ["Dario Amodei", "Karl Deisseroth"]
    params = {p.name: p.value for p in bq.query.call_args.kwargs["job_config"].query_parameters}
    assert params == {"min_hindex": featured.MIN_HINDEX, "limit": 5}


def test_run_stores_list_in_firestore():
    db = MagicMock()
    assert featured.run(bq=_bq(ROWS), db=db) == 2
    db.collection.assert_called_once_with(featured.FEATURED_COLLECTION)
    db.collection.return_value.document.assert_called_once_with(featured.FEATURED_DOC_ID)
    stored = db.collection.return_value.document.return_value.set.call_args.args[0]
    assert stored["data"][0]["scholar_id"] == "6-e-ZBEAAAAJ"
    assert set(stored["data"][0]) >= {"scholar_id", "name", "affiliation", "hindex", "citedby",
                                      "pip_score", "pip_percentile", "pip_auc_score", "pip_auc_percentile"}
    assert "timestamp" in stored


def test_run_keeps_stored_list_when_query_is_empty():
    db = MagicMock()
    assert featured.run(bq=_bq([]), db=db) == 0
    db.collection.assert_not_called()


def test_collection_names_match_frontend():
    import os
    path = os.path.join(os.path.dirname(__file__), "..", "..", "frontend", "cache.py")
    source = open(path).read()
    assert f'FEATURED_AUTHORS_COLLECTION = "{featured.FEATURED_COLLECTION}"' in source
    assert f'FEATURED_AUTHORS_DOC_ID = "{featured.FEATURED_DOC_ID}"' in source
    assert f'RECENT_AUTHORS_COLLECTION = "{featured.RECENT_COLLECTION}"' in source
    assert f'RECENT_AUTHORS_DOC_ID = "{featured.RECENT_DOC_ID}"' in source


def test_refresh_recent_updates_scores_in_place():
    db = MagicMock()
    doc_ref = db.collection.return_value.document.return_value
    snap = MagicMock()
    snap.exists = True
    snap.to_dict.return_value = {
        "data": [
            {"scholar_id": "6-e-ZBEAAAAJ", "name": "Dario Amodei", "pip_auc_score": 0.5051},
            {"scholar_id": "UNKNOWNAUTHR", "name": "Kept As Is", "pip_auc_score": 0.2},
        ]
    }
    doc_ref.get.return_value = snap
    tx = MagicMock()
    db.transaction.return_value = tx

    with patch.object(featured.firestore, "transactional", side_effect=lambda fn: fn):
        refreshed = featured.refresh_recent(bq=_bq([ROWS[0]]), db=db)

    assert refreshed == 1
    tx.set.assert_called_once()
    written = tx.set.call_args.args[1]["data"]
    assert written[0]["scholar_id"] == "6-e-ZBEAAAAJ"
    assert written[0]["pip_score"] == 0.5812
    assert written[0]["pip_percentile"] == 0.861
    assert written[1] == {"scholar_id": "UNKNOWNAUTHR", "name": "Kept As Is", "pip_auc_score": 0.2}


def test_refresh_featured_logs_failures():
    with patch.object(main.featured, "run", side_effect=RuntimeError("boom")), \
         patch.object(main.featured, "refresh_recent", side_effect=RuntimeError("boom")):
        assert main.refresh_featured() == 0


@pytest.mark.parametrize("mode", ["materialize", "wikidata"])
def test_scheduled_modes_rebuild_featured(monkeypatch, mode):
    monkeypatch.setenv("CRAWL_MODE", mode)
    with patch.object(main.materialize, "run", return_value={}), \
         patch.object(main.wikidata, "run", return_value=1), \
         patch.object(main, "refresh_search_index"), \
         patch.object(main, "refresh_featured") as refresh:
        main.main()
    refresh.assert_called_once()


def test_featured_mode_fails_when_nothing_stored(monkeypatch):
    monkeypatch.setenv("CRAWL_MODE", "featured")
    with patch.object(main.featured, "run", return_value=0), \
         patch.object(main.featured, "refresh_recent", return_value=0), \
         pytest.raises(SystemExit):
        main.main()
    with patch.object(main.featured, "run", return_value=2), \
         patch.object(main.featured, "refresh_recent", return_value=1):
        main.main()
