#!/usr/bin/env python3
"""Benchmark cache population latency for authors with many publications.

For each test author:
1. POST /api/rebuild_statistics to force cache re-population from BigQuery
2. Poll /results until the results page loads (not the loading/processing page)
3. Report total wall-clock time

Usage:
    python scripts/benchmark_cache_populate.py
    python scripts/benchmark_cache_populate.py --site https://www.pip-score.org --authors 5
    python scripts/benchmark_cache_populate.py --timeout 180
"""

import argparse
import time

import requests

# Test authors: 50-200 publications, h-index >= 10
TEST_AUTHORS = [
    ("4564493", "Jelena J. Vulić", 104),
    ("1875084", "M. Olgun", 77),
    ("2108452148", "Xiaodong Zhang", 78),
    ("1403253615", "Y. Al-Wahaibi", 166),
    ("4681610", "E. Titlyanov", 94),
    ("32245290", "T. Nakagawa", 170),
    ("1400090424", "S. Maki-Yonekura", 91),
    ("145340358", "W. Clauss", 74),
    ("50144057", "R. Paterson", 61),
    ("145593373", "Zheng Wu", 88),
]


def trigger_rebuild(site, author_id):
    """POST to /api/rebuild_statistics to force cache re-population."""
    url = f"{site}/api/rebuild_statistics"
    resp = requests.post(url, data={"scholar_ids": author_id}, timeout=15)
    return resp.status_code == 200


def poll_for_results(site, author_id, timeout=120, poll_interval=3):
    """Poll /results until it returns actual results (not the processing page).

    The processing page has title "Processing -- PiP Score".
    The results page has the author's name in the title.

    Returns (elapsed_seconds, success_bool).
    """
    url = f"{site}/results?author_id={author_id}"
    start = time.monotonic()

    while True:
        elapsed = time.monotonic() - start
        if elapsed > timeout:
            return elapsed, False

        try:
            resp = requests.get(url, timeout=15, allow_redirects=True)
            body = resp.text
            if resp.status_code == 200 and "Processing -- PiP Score" not in body and "Computing Statistics -- PiP Score" not in body and "not_found" not in body:
                return time.monotonic() - start, True
            if "not_found" in body:
                return time.monotonic() - start, False
        except requests.RequestException:
            pass  # transient network error, retry on next poll

        time.sleep(poll_interval)


def main():
    parser = argparse.ArgumentParser(description="Benchmark cache population latency")
    parser.add_argument("--site", default="https://www.pip-score.org",
                        help="Frontend URL (default: https://www.pip-score.org)")
    parser.add_argument("--timeout", type=int, default=120,
                        help="Max seconds to wait per author (default: 120)")
    parser.add_argument("--authors", type=int, default=5,
                        help="Number of authors to test (default: 5)")
    args = parser.parse_args()

    authors_to_test = TEST_AUTHORS[:args.authors]
    results = []

    print(f"Testing {len(authors_to_test)} authors against {args.site}")
    print(f"Timeout: {args.timeout}s per author")
    print("-" * 70)

    for author_id, name, pubs in authors_to_test:
        print(f"\n{name} ({author_id}, {pubs} pubs)")

        # Trigger rebuild to force fresh cache population
        ok = trigger_rebuild(args.site, author_id)
        if not ok:
            print("  WARNING: rebuild_statistics request failed")

        # Poll until results are ready
        print(f"  Waiting for results...", end="", flush=True)
        elapsed, success = poll_for_results(args.site, author_id, timeout=args.timeout)

        status = "OK" if success else "TIMEOUT"
        print(f" {elapsed:.1f}s [{status}]")
        results.append((author_id, name, pubs, elapsed, success))

    # Summary
    print("\n" + "=" * 70)
    print(f"{'Author':<25} {'Pubs':>5} {'Time':>8} {'Status':>8}")
    print("-" * 70)
    for author_id, name, pubs, elapsed, success in results:
        status = "OK" if success else "TIMEOUT"
        print(f"{name:<25} {pubs:>5} {elapsed:>7.1f}s {status:>8}")

    successful = [r for r in results if r[4]]
    if successful:
        times = [r[3] for r in successful]
        print("-" * 70)
        print(f"{'Avg':<25} {'':>5} {sum(times)/len(times):>7.1f}s")
        print(f"{'Min':<25} {'':>5} {min(times):>7.1f}s")
        print(f"{'Max':<25} {'':>5} {max(times):>7.1f}s")


if __name__ == "__main__":
    main()
