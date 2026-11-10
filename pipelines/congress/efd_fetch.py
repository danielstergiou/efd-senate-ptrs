"""Senate PTR fetcher — runs on a US GitHub Actions runner, never on the box.

efdsearch.senate.gov answers 403 "Access Denied" to the box (Hetzner, EU) and
to Melbourne, and 200 to a US runner. This module reuses the live Senate leg
of congress_trades.py (handshake, DataTables search, PTR page parser) and
writes the public records it read to JSON; the box stores them with
``python -m pipelines.congress.congress_trades --senate-from-url`` (or env
EFD_DATA_URL on the bare cron run). No database, no secret.

    python -m pipelines.congress.efd_fetch --days 45 --out data/ [--previous data/efd/latest.json]

Writes ``<out>/efd/senate-ptrs-<UTC stamp>.json`` and ``<out>/efd/latest.json``
(same content). Exit 1 on the first 403 (the workflow then fails loudly) and
nothing is written.

Terms: the eFD click-through (5 U.S.C. app. § 105(c)) forbids commercial use
"other than by news and communications media for dissemination to the general
public". Every report keeps its ``ptr_link``; paper filings are link-only.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional

import httpx

from pipelines.congress.congress_trades import CongressClient, fetch_senate_reports

logger = logging.getLogger(__name__)


def build_payload(reports: list[dict], window_days: int, fetched_at: Optional[datetime] = None) -> dict:
    fetched_at = fetched_at or datetime.now(tz=timezone.utc)
    return {
        "fetched_at": fetched_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_days": window_days,
        "source": "U.S. Senate Office of Public Records — efdsearch.senate.gov",
        "reports": reports,
    }


def write_payload(payload: dict, out_dir: str) -> tuple[str, str]:
    """Write the stamped file and latest.json. Returns both paths."""
    efd_dir = os.path.join(out_dir, "efd")
    os.makedirs(efd_dir, exist_ok=True)
    stamp = payload["fetched_at"].replace("-", "").replace(":", "")
    stamped = os.path.join(efd_dir, f"senate-ptrs-{stamp}.json")
    latest = os.path.join(efd_dir, "latest.json")
    body = json.dumps(payload, default=str, indent=1, sort_keys=False)
    for path in (stamped, latest):
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(body + "\n")
    return stamped, latest


def _load_previous(path: Optional[str]) -> dict[str, dict]:
    """Report id → report from an earlier run, so an e-filed PTR already read
    is not fetched again (one request per new filing, not per run)."""
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, encoding="utf-8") as fh:
            return {r["report_id"]: r for r in (json.load(fh).get("reports") or []) if r.get("report_id")}
    except (OSError, ValueError, KeyError, AttributeError) as e:
        logger.warning("efd: could not read the previous run %s (%s) — fetching every PTR", path, e)
        return {}


def summarize(payload: dict) -> dict:
    reports = payload["reports"]
    return {
        "reports": len(reports),
        "efiled": sum(1 for r in reports if r["kind"] == "efiled"),
        "paper": sum(1 for r in reports if r["kind"] == "paper"),
        "transactions": sum(len(r.get("transactions") or []) for r in reports),
    }


def run(days: int, out_dir: str, previous: Optional[str] = None, client: Optional[CongressClient] = None) -> int:
    client = client or CongressClient()
    try:
        reports, fetched = fetch_senate_reports(client, days, reuse=_load_previous(previous))
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 403:
            logger.error("efd: 403 at %s — stopping; nothing written", e.request.url)
            return 1
        raise
    finally:
        client.close()
    payload = build_payload(reports, days)
    stamped, latest = write_payload(payload, out_dir)
    s = summarize(payload)
    logger.info(
        "efd: reports=%s efiled=%s paper=%s transactions=%s pages_fetched=%s -> %s",
        s["reports"], s["efiled"], s["paper"], s["transactions"], fetched, stamped,
    )
    # One machine-readable line for the workflow log / step summary.
    print("EFD_COUNTS " + json.dumps({**s, "pages_fetched": fetched, "file": os.path.basename(stamped)}))
    return 0


def _main(argv: list[str]) -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(prog="python -m pipelines.congress.efd_fetch")
    parser.add_argument("--days", type=int, default=45, help="trailing window of filing dates (default 45)")
    parser.add_argument("--out", default="data/", help="output root; files go to <out>/efd/")
    parser.add_argument("--previous", default=None, help="an earlier latest.json whose PTRs need no refetch")
    args = parser.parse_args(argv)
    return run(args.days, args.out, previous=args.previous)


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv[1:]))
