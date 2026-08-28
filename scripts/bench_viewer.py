"""Benchmark harness for the viewer backend.

Measures per-step latencies (p50/p95) of the viewer API endpoints against a
running instance::

    python scripts/bench_viewer.py --base-url http://localhost:8080/api --reps 5

The viewer VM is firewalled, so run this ON the VM (via SSH) where the viewer
listens on localhost:8080. Record the baseline in PROGRESS.md and re-run after
each phase to track deltas. (Ingest->SSE delivery latency is added with the
SSE endpoint in Phase 4.)

Uses only the standard library so it runs with any Python >= 3.10.
"""

import argparse
import json
import time
import urllib.error
import urllib.parse
import urllib.request

DEFAULT_BASE_URL = "http://localhost:8080/api"


def http_get(url: str, timeout: float = 600.0) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def percentile(sorted_values: list[float], p: float) -> float:
    """Nearest-rank percentile over sorted values."""
    if not sorted_values:
        return float("nan")
    idx = min(
        len(sorted_values) - 1,
        max(0, round(p / 100.0 * (len(sorted_values) - 1))),
    )
    return sorted_values[idx]


def bench(name: str, fn, reps: int, results: dict) -> None:
    """Run *fn* ``reps`` times, print per-attempt latencies, store in *results*."""
    latencies = []
    for i in range(reps):
        start = time.perf_counter()
        try:
            fn()
        except Exception as exc:  # noqa: BLE001 - benchmark keeps going on failure
            print(f"  {name}: attempt {i + 1} FAILED: {exc}")
            latencies.append(float("nan"))
        else:
            latencies.append((time.perf_counter() - start) * 1000)
            print(f"  {name}: attempt {i + 1}: {latencies[-1]:.0f} ms")
    results[name] = latencies


def qs(base: str, params: dict) -> str:
    return f"{base}?{urllib.parse.urlencode(params)}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--reps", type=int, default=5, help="repetitions per step (default 5)")
    parser.add_argument("--dataset", default=None, help="dataset name (default: first from /datasets)")
    parser.add_argument("--page-size", type=int, default=50)
    parser.add_argument("--skip-deep", action="store_true", help="skip the deep-page (last page) step")
    args = parser.parse_args()

    base = args.base_url.rstrip("/")
    reps = args.reps
    results: dict = {}

    print(f"Benchmarking {base} ({reps} reps per step)")

    dataset = args.dataset
    if dataset is None:
        datasets = http_get(f"{base}/datasets")["datasets"]
        if not datasets:
            sys.exit("No datasets found")
        dataset = datasets[0]
        print(f"Using dataset: {dataset} ({len(datasets)} datasets available)")
    else:
        print(f"Using dataset: {dataset}")

    ds = urllib.parse.quote(dataset, safe="")

    bench("list datasets", lambda: http_get(f"{base}/datasets"), reps, results)
    bench("schema", lambda: http_get(f"{base}/datasets/{ds}/schema"), reps, results)
    bench("count", lambda: http_get(f"{base}/datasets/{ds}/count"), reps, results)

    page_size = args.page_size

    def page(p: int):
        return http_get(
            qs(f"{base}/datasets/{ds}/data", {"page": p, "page_size": page_size})
        )

    bench("data page 1", lambda: page(1), reps, results)
    page1 = page(1)

    count = http_get(f"{base}/datasets/{ds}/count")["count"]
    total_pages = max(1, count // page_size)
    mid_page = max(1, total_pages // 2)
    bench(f"data page {mid_page} (mid)", lambda: page(mid_page), reps, results)
    if not args.skip_deep and total_pages > mid_page:
        bench(f"data page {total_pages} (deep)", lambda: page(total_pages), reps, results)

    rows = page1.get("rows", [])
    if rows and rows[0].get("id"):
        row_id = rows[0]["id"]
        bench(
            "row by id",
            lambda: http_get(
                qs(f"{base}/datasets/{ds}/data", {"row_id": row_id})
            ),
            reps,
            results,
        )

    print("\nSummary (ms):")
    print(f"{'step':<32} {'p50':>8} {'p95':>8} {'n':>4}")
    for name, latencies in results.items():
        valid = sorted(v for v in latencies if v == v)
        print(
            f"{name:<32} "
            f"{percentile(valid, 50):>8.0f} "
            f"{percentile(valid, 95):>8.0f} "
            f"{len(valid):>4}"
        )


if __name__ == "__main__":
    import sys

    main()
