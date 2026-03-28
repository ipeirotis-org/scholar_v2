#!/usr/bin/env python3
"""Validate PiP-AUC scores from Semantic Scholar-based BigQuery views.

Runs a suite of checks against the S2-based analytics views to verify:
1. Mathematical correctness: scores in [0, 1], monotonicity, no NULLs
2. Data integrity: no duplicates, correct joins, reasonable row counts
3. Benchmark authors: scores are reasonable for 16 known authors
4. Cross-view consistency: pip_inputs → pip_scores → ranked all agree

Strategy: The full view chain (pip_inputs → pip_scores) is expensive to scan
across all authors. We use two approaches:
  - Per-author queries with WHERE scholar_id IN (...) for views (cheap, pushes
    down predicates through the view chain)
  - Direct scans of materialized dist_* tables (already small)

Usage:
    export GOOGLE_APPLICATION_CREDENTIALS=/tmp/sa-key.json
    python3 scripts/validate_s2_pip_scores.py
"""

import sys
from google.cloud import bigquery

PROJECT = "scholar-version2"

# 16 benchmark authors from TASKS.md Phase 0 validation
BENCHMARK_AUTHORS = {
    "2350669": {"name": "Ronald C Kessler", "s2_h": 235, "s2_pubs": 1113},
    "3988124": {"name": "JoAnn Manson", "s2_h": 263, "s2_pubs": 2218},
    "9311320": {"name": "Eric Lander", "s2_h": 290, "s2_pubs": 779},
    "2242100447": {"name": "Frank B. Hu", "s2_h": 269, "s2_pubs": 1906},
    "1965563": {"name": "Bert Vogelstein", "s2_h": 268, "s2_pubs": 859},
    "145882172": {"name": "Christopher Murray", "s2_h": 195, "s2_pubs": 627},
    "145441750": {"name": "John P.A. Ioannidis", "s2_h": 195, "s2_pubs": 1237},
    "144524355": {"name": "Mark Daly", "s2_h": 203, "s2_pubs": 786},
    "1751762": {"name": "Yoshua Bengio", "s2_h": 212, "s2_pubs": 812},
    "1695689": {"name": "Geoffrey Hinton", "s2_h": 162, "s2_pubs": 467},
    "1761784": {"name": "Robert Tibshirani", "s2_h": 160, "s2_pubs": 687},
    "3683465": {"name": "Daniel Kahneman", "s2_h": 124, "s2_pubs": 291},
    "1701686": {"name": "Ilya Sutskever", "s2_h": 75, "s2_pubs": 164},
    "2983898": {"name": "Ross Girshick", "s2_h": 79, "s2_pubs": 112},
    "39353098": {"name": "Kaiming He", "s2_h": 67, "s2_pubs": 84},
    "2942126": {"name": "Panos Ipeirotis", "s2_h": 48, "s2_pubs": 125},
}

BENCHMARK_IDS = list(BENCHMARK_AUTHORS.keys())
IDS_SQL = ", ".join(f"'{aid}'" for aid in BENCHMARK_IDS)


def run_query(client, sql):
    """Run a BigQuery query and return rows as list of dicts."""
    result = client.query(sql).result()
    return [dict(row) for row in result]


def check_dist_pip_auc_populated(client):
    """Check 1: dist_pip_auc_scores materialized table is populated and sane."""
    print("\n=== Check 1: dist_pip_auc_scores table populated ===")
    rows = run_query(client, """
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT year_of_first_pub) AS distinct_years,
            MIN(year_of_first_pub) AS min_year,
            MAX(year_of_first_pub) AS max_year,
            MIN(percentile) AS min_pct,
            MAX(percentile) AS max_pct,
            MIN(pip_auc_score) AS min_score,
            MAX(pip_auc_score) AS max_score
        FROM `scholar-version2.statistics.dist_pip_auc_scores`
    """)
    r = rows[0]
    print(f"  Total rows: {r['total_rows']:,}")
    print(f"  Distinct years: {r['distinct_years']} ({r['min_year']}-{r['max_year']})")
    print(f"  Percentile range: [{r['min_pct']:.4f}, {r['max_pct']:.4f}]")
    print(f"  Score range: [{r['min_score']:.4f}, {r['max_score']:.4f}]")

    ok = (r['total_rows'] > 0 and r['distinct_years'] > 10
          and r['min_pct'] >= 0 and r['max_pct'] <= 1
          and r['min_score'] >= 0 and r['max_score'] <= 1)
    print(f"  RESULT: {'PASS' if ok else 'FAIL'}")
    return ok


def check_dist_tables_consistency(client):
    """Check 2: All required dist tables have data."""
    print("\n=== Check 2: Distribution tables populated ===")
    tables = [
        "dist_publication_citations",
        "dist_author_metrics",
        "dist_pip_auc_scores",
    ]
    all_ok = True
    for t in tables:
        rows = run_query(client, f"SELECT COUNT(*) AS cnt FROM `scholar-version2.statistics.{t}`")
        cnt = rows[0]['cnt']
        ok = cnt > 0
        all_ok = all_ok and ok
        print(f"  {t}: {cnt:,} rows {'OK' if ok else 'EMPTY!'}")

    # Check dist_author_metrics has the required metric names
    rows = run_query(client, """
        SELECT DISTINCT metric_name FROM `scholar-version2.statistics.dist_author_metrics`
        ORDER BY metric_name
    """)
    metrics = [r['metric_name'] for r in rows]
    required = {'hindex', 'citedby', 'i10index', 'total_publications', 'total_publications_with_citations'}
    missing = required - set(metrics)
    if missing:
        print(f"  MISSING metrics in dist_author_metrics: {missing}")
        all_ok = False
    else:
        print(f"  dist_author_metrics metrics: {metrics}")

    print(f"  RESULT: {'PASS' if all_ok else 'FAIL'}")
    return all_ok


def check_benchmark_authors_stats(client):
    """Check 3: Benchmark authors exist in stats_author_current with reasonable data."""
    print("\n=== Check 3: Benchmark authors in stats_author_current ===")
    rows = run_query(client, f"""
        SELECT
            scholar_id, name, hindex, citedby, i10index,
            total_publications, total_publications_with_citations, year_of_first_pub
        FROM `scholar-version2.statistics.stats_author_current`
        WHERE scholar_id IN ({IDS_SQL})
        ORDER BY hindex DESC
    """)

    found_ids = {r['scholar_id'] for r in rows}
    missing = set(BENCHMARK_IDS) - found_ids

    all_ok = len(missing) == 0
    print(f"\n  {'Name':<25} {'S2 ID':<14} {'h-idx':>5} {'exp_h':>5} {'cites':>8} {'pubs':>5} {'exp_p':>5} {'yr1st':>5}")
    print(f"  {'-'*25} {'-'*14} {'-'*5} {'-'*5} {'-'*8} {'-'*5} {'-'*5} {'-'*5}")
    for r in rows:
        ba = BENCHMARK_AUTHORS.get(r['scholar_id'], {})
        name = ba.get('name', r['name'])[:25]
        exp_h = ba.get('s2_h', '?')
        exp_p = ba.get('s2_pubs', '?')
        # Fail on NULL core metrics (would indicate a broken join/migration)
        status = ""
        if r['hindex'] is None:
            status += " <- hindex is NULL!"
            all_ok = False
        if r['total_publications'] is None:
            status += " <- total_publications is NULL!"
            all_ok = False
        # Flag large deviations from expected values (>20% off)
        if isinstance(exp_h, int) and r['hindex'] is not None:
            h_diff_pct = abs(r['hindex'] - exp_h) / max(exp_h, 1) * 100
            if h_diff_pct > 20:
                status += f" <- h-index off by {h_diff_pct:.0f}%"
                all_ok = False
        if isinstance(exp_p, int) and r['total_publications'] is not None:
            p_diff_pct = abs(r['total_publications'] - exp_p) / max(exp_p, 1) * 100
            if p_diff_pct > 20:
                status += f" <- pubs off by {p_diff_pct:.0f}%"
                all_ok = False
        print(f"  {name:<25} {r['scholar_id']:<14} {r['hindex']:>5} {exp_h:>5} {r['citedby']:>8,} {r['total_publications']:>5} {exp_p:>5} {r['year_of_first_pub']:>5}{status}")

    if missing:
        print(f"\n  MISSING from stats_author_current: {missing}")

    print(f"\n  Found: {len(found_ids)}/{len(BENCHMARK_IDS)}")
    print(f"  RESULT: {'PASS' if all_ok else 'FAIL'}")
    return all_ok


def check_benchmark_pip_inputs(client):
    """Check 4: PiP inputs for benchmark authors are correct."""
    print("\n=== Check 4: PiP inputs for benchmark authors ===")

    # Check all benchmark authors at once
    rows = run_query(client, f"""
        SELECT
            scholar_id,
            COUNT(*) AS pub_count,
            MIN(num_citations_percentile) AS min_cit_pct,
            MAX(num_citations_percentile) AS max_cit_pct,
            MIN(num_papers_percentile) AS min_pap_pct,
            MAX(num_papers_percentile) AS max_pap_pct,
            COUNTIF(num_citations_percentile IS NULL) AS null_cit,
            COUNTIF(num_papers_percentile IS NULL) AS null_pap,
            COUNTIF(num_citations_percentile < 0 OR num_citations_percentile > 1) AS bad_cit,
            COUNTIF(num_papers_percentile < 0 OR num_papers_percentile > 1) AS bad_pap
        FROM `scholar-version2.statistics.stats_author_publication_pip_inputs_current`
        WHERE scholar_id IN ({IDS_SQL})
        GROUP BY scholar_id
        ORDER BY pub_count DESC
    """)

    found_ids = {r['scholar_id'] for r in rows}
    missing = set(BENCHMARK_IDS) - found_ids

    all_ok = True
    print(f"\n  {'Name':<25} {'pubs':>5} {'cit_pct':>12} {'pap_pct':>12} {'nulls':>5} {'bad':>5}")
    print(f"  {'-'*25} {'-'*5} {'-'*12} {'-'*12} {'-'*5} {'-'*5}")
    for r in rows:
        name = BENCHMARK_AUTHORS.get(r['scholar_id'], {}).get('name', r['scholar_id'])[:25]
        cit_range = f"[{r['min_cit_pct']:.2f},{r['max_cit_pct']:.2f}]"
        pap_range = f"[{r['min_pap_pct']:.2f},{r['max_pap_pct']:.2f}]"
        nulls = r['null_cit'] + r['null_pap']
        bad = r['bad_cit'] + r['bad_pap']
        if nulls > 0 or bad > 0:
            all_ok = False
        print(f"  {name:<25} {r['pub_count']:>5} {cit_range:>12} {pap_range:>12} {nulls:>5} {bad:>5}")

    if missing:
        print(f"\n  MISSING: {missing}")
        all_ok = False

    ok = all_ok and len(missing) == 0
    print(f"  RESULT: {'PASS' if ok else 'FAIL'}")
    return ok


def check_benchmark_pip_inputs_detail(client):
    """Check 5: Detailed PiP inputs for Panos Ipeirotis — verify ordering."""
    print("\n=== Check 5: PiP inputs detail for Panos Ipeirotis (2942126) ===")
    rows = run_query(client, """
        SELECT
            author_pub_id,
            num_citations,
            num_citations_percentile,
            publication_rank,
            num_papers_percentile
        FROM `scholar-version2.statistics.stats_author_publication_pip_inputs_current`
        WHERE scholar_id = '2942126'
        ORDER BY publication_rank
    """)
    print(f"  Publications with PiP inputs: {len(rows)}")
    if not rows:
        print("  RESULT: FAIL (no data)")
        return False

    # Check ordering: publication_rank should be sequential
    ranks = [r['publication_rank'] for r in rows]
    is_sequential = ranks == list(range(1, len(ranks) + 1))
    print(f"  Ranks sequential (1..{len(rows)}): {is_sequential}")

    # Check citation percentile non-increasing (papers sorted by citation rank DESC)
    cit_pcts = [r['num_citations_percentile'] for r in rows]
    is_decreasing = all(a >= b for a, b in zip(cit_pcts, cit_pcts[1:]))
    print(f"  Citation percentiles non-increasing: {is_decreasing}")

    # Show first and last few
    print(f"\n  Top 5 publications (highest citation percentile):")
    for r in rows[:5]:
        print(f"    rank={r['publication_rank']:>3}  cites={r['num_citations']:>5}  cit%={r['num_citations_percentile']:.4f}  pap%={r['num_papers_percentile']:.4f}")
    print(f"  Bottom 5:")
    for r in rows[-5:]:
        print(f"    rank={r['publication_rank']:>3}  cites={r['num_citations']:>5}  cit%={r['num_citations_percentile']:.4f}  pap%={r['num_papers_percentile']:.4f}")

    # Check for duplicates
    pub_ids = [r['author_pub_id'] for r in rows]
    has_dups = len(pub_ids) != len(set(pub_ids))
    if has_dups:
        print(f"  WARNING: Duplicate author_pub_ids found!")

    ok = is_sequential and is_decreasing and not has_dups
    print(f"  RESULT: {'PASS' if ok else 'FAIL'}")
    return ok


def check_benchmark_pip_scores(client):
    """Check 6: PiP-AUC scores for all benchmark authors."""
    print("\n=== Check 6: Benchmark authors PiP-AUC scores ===")
    rows = run_query(client, f"""
        SELECT
            r.scholar_id,
            r.year_of_first_pub,
            r.pip_auc_score,
            r.pip_auc_score_percentile
        FROM `scholar-version2.statistics.ranked_author_pip_scores_current` r
        WHERE r.scholar_id IN ({IDS_SQL})
        ORDER BY r.pip_auc_score_percentile DESC
    """)

    found_ids = {r['scholar_id'] for r in rows}
    missing = set(BENCHMARK_IDS) - found_ids

    all_ok = True
    print(f"\n  {'Name':<25} {'S2 ID':<14} {'yr1st':>5} {'PiP-AUC':>8} {'PiP%':>8}")
    print(f"  {'-'*25} {'-'*14} {'-'*5} {'-'*8} {'-'*8}")
    for r in rows:
        name = BENCHMARK_AUTHORS.get(r['scholar_id'], {}).get('name', r['scholar_id'])[:25]
        score = r['pip_auc_score']
        pct = r['pip_auc_score_percentile']
        if score < 0 or score > 1:
            all_ok = False
        if pct < 0 or pct > 1:
            all_ok = False
        print(f"  {name:<25} {r['scholar_id']:<14} {r['year_of_first_pub']:>5} {score:>8.4f} {pct:>8.4f}")

    if missing:
        print(f"\n  MISSING: {missing}")
        all_ok = False

    # All benchmark authors should have high PiP scores (they're top researchers)
    scores = [r['pip_auc_score'] for r in rows]
    if scores:
        min_score = min(scores)
        avg_score = sum(scores) / len(scores)
        print(f"\n  Score range: [{min_score:.4f}, {max(scores):.4f}], avg: {avg_score:.4f}")
        # These are all world-class researchers — PiP should be > 0.3 at minimum
        if min_score < 0.3:
            print(f"  FAIL: Min score {min_score:.4f} is too low for top researchers")
            all_ok = False

    # Percentile check: a broken dist lookup would collapse percentiles to 0.0
    # All benchmark authors are top researchers — percentile should be > 0.5
    percentiles = [r['pip_auc_score_percentile'] for r in rows]
    if percentiles:
        low_pct = [r for r in rows if r['pip_auc_score_percentile'] < 0.5]
        if low_pct:
            print(f"  FAIL: {len(low_pct)} benchmark author(s) have percentile < 0.5:")
            for r in low_pct:
                name = BENCHMARK_AUTHORS.get(r['scholar_id'], {}).get('name', r['scholar_id'])
                print(f"    {name}: {r['pip_auc_score_percentile']:.4f}")
            all_ok = False

    ok = all_ok and len(missing) == 0
    print(f"\n  Found: {len(found_ids)}/{len(BENCHMARK_IDS)}")
    print(f"  RESULT: {'PASS' if ok else 'FAIL'}")
    return ok


def check_cross_view_consistency(client):
    """Check 7: pip_scores matches manual trapezoidal computation from pip_inputs."""
    print("\n=== Check 7: Cross-view consistency (manual AUC vs view) ===")
    rows = run_query(client, f"""
        WITH inputs AS (
            SELECT
                scholar_id,
                num_citations_percentile,
                num_papers_percentile,
                COALESCE(
                    LAG(num_citations_percentile) OVER(PARTITION BY scholar_id ORDER BY num_papers_percentile),
                    num_citations_percentile
                ) AS prev_cit_pct,
                COALESCE(
                    LAG(num_papers_percentile) OVER(PARTITION BY scholar_id ORDER BY num_papers_percentile),
                    0
                ) AS prev_pap_pct
            FROM `scholar-version2.statistics.stats_author_publication_pip_inputs_current`
            WHERE scholar_id IN ({IDS_SQL})
        ),
        manual_auc AS (
            SELECT
                scholar_id,
                ROUND(SUM((num_papers_percentile - prev_pap_pct) * (num_citations_percentile + prev_cit_pct) / 2), 4) AS manual_pip_auc
            FROM inputs
            GROUP BY scholar_id
        )
        SELECT
            m.scholar_id,
            m.manual_pip_auc,
            v.pip_auc_score AS view_pip_auc,
            ABS(m.manual_pip_auc - v.pip_auc_score) AS diff
        FROM manual_auc m
        JOIN `scholar-version2.statistics.stats_author_pip_scores_current` v
          ON m.scholar_id = v.scholar_id
        ORDER BY diff DESC
    """)

    max_diff = 0
    for r in rows:
        diff = float(r['diff'])
        max_diff = max(max_diff, diff)
        name = BENCHMARK_AUTHORS.get(r['scholar_id'], {}).get('name', r['scholar_id'])[:25]
        status = "OK" if diff < 0.0001 else "MISMATCH!"
        print(f"  {name:<25} manual={r['manual_pip_auc']:.4f}  view={r['view_pip_auc']:.4f}  diff={diff:.6f}  {status}")

    ok = max_diff < 0.0001
    print(f"\n  Max difference: {max_diff:.6f}")
    print(f"  RESULT: {'PASS' if ok else 'FAIL'}")
    return ok


def check_pip_inputs_no_duplicates(client):
    """Check 8: No duplicate (scholar_id, author_pub_id) in PiP inputs for benchmark authors."""
    print("\n=== Check 8: No duplicates in PiP inputs (benchmark authors) ===")
    rows = run_query(client, f"""
        SELECT scholar_id, author_pub_id, COUNT(*) AS cnt
        FROM `scholar-version2.statistics.stats_author_publication_pip_inputs_current`
        WHERE scholar_id IN ({IDS_SQL})
        GROUP BY scholar_id, author_pub_id
        HAVING cnt > 1
        LIMIT 20
    """)
    dup_count = len(rows)
    if dup_count > 0:
        print(f"  Found {dup_count} duplicate pairs:")
        for r in rows[:5]:
            name = BENCHMARK_AUTHORS.get(r['scholar_id'], {}).get('name', r['scholar_id'])
            print(f"    {name}: pub {r['author_pub_id']} appears {r['cnt']} times")
    else:
        print(f"  No duplicates found")

    ok = dup_count == 0
    print(f"  RESULT: {'PASS' if ok else 'FAIL'}")
    return ok


def check_score_percentile_monotonicity(client):
    """Check 9: Higher PiP-AUC score → higher percentile within dist_pip_auc_scores table."""
    print("\n=== Check 9: Score-percentile monotonicity (dist_pip_auc_scores) ===")
    # Check monotonicity directly in the dist table (small, materialized)
    rows = run_query(client, """
        WITH pairs AS (
            SELECT
                year_of_first_pub,
                pip_auc_score,
                percentile,
                LAG(pip_auc_score) OVER (PARTITION BY year_of_first_pub ORDER BY pip_auc_score) AS prev_score,
                LAG(percentile) OVER (PARTITION BY year_of_first_pub ORDER BY pip_auc_score) AS prev_pct
            FROM `scholar-version2.statistics.dist_pip_auc_scores`
        )
        SELECT
            COUNT(*) AS total_pairs,
            COUNTIF(pip_auc_score > prev_score AND percentile < prev_pct) AS violations
        FROM pairs
        WHERE prev_score IS NOT NULL
    """)

    r = rows[0]
    print(f"  Total consecutive pairs: {r['total_pairs']:,}")
    print(f"  Monotonicity violations: {r['violations']}")

    ok = r['violations'] == 0
    print(f"  RESULT: {'PASS' if ok else 'FAIL'}")
    return ok


def check_publication_count_alignment(client):
    """Check 10: PiP input pub counts align with stats_author_current."""
    print("\n=== Check 10: Publication count alignment ===")
    rows = run_query(client, f"""
        WITH pip_counts AS (
            SELECT scholar_id, COUNT(*) AS pip_pub_count
            FROM `scholar-version2.statistics.stats_author_publication_pip_inputs_current`
            WHERE scholar_id IN ({IDS_SQL})
            GROUP BY scholar_id
        )
        SELECT
            a.scholar_id,
            a.total_publications_with_citations,
            p.pip_pub_count,
            a.total_publications_with_citations - p.pip_pub_count AS diff
        FROM `scholar-version2.statistics.stats_author_current` a
        JOIN pip_counts p ON a.scholar_id = p.scholar_id
        ORDER BY ABS(a.total_publications_with_citations - p.pip_pub_count) DESC
    """)

    all_ok = True
    print(f"\n  {'Name':<25} {'stats_pubs':>10} {'pip_pubs':>10} {'diff':>6}")
    print(f"  {'-'*25} {'-'*10} {'-'*10} {'-'*6}")
    for r in rows:
        name = BENCHMARK_AUTHORS.get(r['scholar_id'], {}).get('name', r['scholar_id'])[:25]
        diff = r['diff']
        # Some difference is acceptable — pubs with 0 citations may be filtered differently
        # But large differences indicate a join bug
        status = ""
        if abs(diff) > max(5, r['total_publications_with_citations'] * 0.1):
            status = " ← LARGE DIFF"
            all_ok = False
        print(f"  {name:<25} {r['total_publications_with_citations']:>10} {r['pip_pub_count']:>10} {diff:>6}{status}")

    print(f"  RESULT: {'PASS' if all_ok else 'FAIL'}")
    return all_ok


def main():
    print("=" * 70)
    print("PiP-AUC Score Validation: Semantic Scholar Views")
    print("=" * 70)
    print("Strategy: per-author queries for views, direct scans for dist tables")

    client = bigquery.Client(project=PROJECT)

    checks = [
        ("dist_pip_auc_scores populated", check_dist_pip_auc_populated),
        ("Distribution tables consistency", check_dist_tables_consistency),
        ("Benchmark authors in stats", check_benchmark_authors_stats),
        ("Benchmark PiP inputs", check_benchmark_pip_inputs),
        ("PiP inputs detail (Ipeirotis)", check_benchmark_pip_inputs_detail),
        ("Benchmark PiP-AUC scores", check_benchmark_pip_scores),
        ("Cross-view consistency", check_cross_view_consistency),
        ("No duplicate PiP inputs", check_pip_inputs_no_duplicates),
        ("Score-percentile monotonicity", check_score_percentile_monotonicity),
        ("Publication count alignment", check_publication_count_alignment),
    ]

    results = {}
    for name, check_fn in checks:
        try:
            ok = check_fn(client)
            results[name] = {"passed": ok}
        except Exception as e:
            print(f"\n  ERROR: {e}")
            results[name] = {"passed": False, "error": str(e)}

    # Summary
    print("\n" + "=" * 70)
    print("VALIDATION SUMMARY")
    print("=" * 70)
    passed = sum(1 for r in results.values() if r.get("passed"))
    total = len(results)
    for name, r in results.items():
        status = "PASS" if r.get("passed") else "FAIL"
        err = f" ({r['error']})" if "error" in r else ""
        print(f"  [{status}] {name}{err}")

    print(f"\n  {passed}/{total} checks passed")
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
