"""Congressional trading scraper — House + Senate PTR (Periodic Transaction
Report) filings into `congress.politicians` / `congress.congress_trades`
(migration 014).

One consolidated module per founder simplicity rule. Sections are separated
by `# ════════` dividers below.

CLI (always `python -m`, conda env `redditapp`):
    python -m pipelines.congress.congress_trades --seed-politicians
    python -m pipelines.congress.congress_trades --house [--year 2026]
    python -m pipelines.congress.congress_trades --senate [--days 30]
    python -m pipelines.congress.congress_trades --senate-from-url [URL|path]   # the US-fetched copy
    python -m pipelines.congress.congress_trades          # house + senate incremental

Sources
-------
- Politicians: https://unitedstates.github.io/congress-legislators/legislators-current.json
- House PTRs:  https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.zip
               (XML index) + per-DocID PDFs parsed with pdfplumber
- Senate PTRs: https://efdsearch.senate.gov (csrf handshake + DataTables API
               + e-filed PTR HTML tables parsed with lxml)

Scanned / paper filings that cannot be parsed get ONE stub row with
needs_review=true so the review dashboard can surface them.
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import os
import re
import sys
import time
import unicodedata
import zipfile
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Optional

import httpx
from lxml import etree, html as lxml_html

from pipelines._shared.db import get_connection

logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════════════════
# Constants
# ════════════════════════════════════════════════════════════════════════════
LEGISLATORS_URL = (
    "https://unitedstates.github.io/congress-legislators/legislators-current.json"
)
# Committee rosters (same public-domain project). TOP-LEVEL committees only
# reach the About copy — subcommittee ids (parent + 2 digits) are skipped.
COMMITTEES_URL = (
    "https://unitedstates.github.io/congress-legislators/committees-current.json"
)
COMMITTEE_MEMBERSHIP_URL = (
    "https://unitedstates.github.io/congress-legislators/committee-membership-current.json"
)
PHOTO_URL_TPL = "https://theunitedstates.io/images/congress/225x275/{bioguide}.jpg"

HOUSE_ZIP_URL_TPL = (
    "https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}FD.zip"
)
HOUSE_PDF_URL_TPL = (
    "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{doc_id}.pdf"
)

SENATE_BASE = "https://efdsearch.senate.gov"
SENATE_HOME_URL = f"{SENATE_BASE}/search/home/"
SENATE_SEARCH_REFERER = f"{SENATE_BASE}/search/"
SENATE_DATA_URL = f"{SENATE_BASE}/search/report/data/"
SENATE_PAGE_SIZE = 100
SENATE_MAX_PAGES = 100  # safety valve

# Naive clients get 403 from disclosures-clerk.house.gov — a browser UA is
# mandatory there, and harmless everywhere else we fetch.
BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

_NAME_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}


# ════════════════════════════════════════════════════════════════════════════
# CongressClient — throttled httpx client (browser UA, ≥1s between requests)
# ════════════════════════════════════════════════════════════════════════════
class CongressClient:
    """HTTP client for the disclosure sites.

    - Browser User-Agent (House clerk 403s naive clients).
    - Throttles to at least ``MIN_INTERVAL`` seconds between requests.
    - Retries 429/503/network errors with exponential backoff.
    - Cookie jar persists across requests (needed for the Senate handshake).
    """

    MIN_INTERVAL = 1.0
    MAX_RETRIES = 3
    TIMEOUT = 60.0

    def __init__(self, client: Optional[httpx.Client] = None) -> None:
        headers = {
            "User-Agent": BROWSER_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate",
        }
        if client is None:
            self._client = httpx.Client(
                headers=headers, timeout=self.TIMEOUT, follow_redirects=True
            )
        else:
            # Custom client (e.g. MockTransport in tests) — apply our headers.
            self._client = client
            for k, v in headers.items():
                self._client.headers[k] = v
        self._last_request_at: float = 0.0

    # -- internals -----------------------------------------------------------
    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        wait = self.MIN_INTERVAL - elapsed
        if wait > 0:
            time.sleep(wait)
        self._last_request_at = time.monotonic()

    def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        last_status: Optional[int] = None
        for attempt in range(self.MAX_RETRIES):
            self._throttle()
            try:
                resp = self._client.request(method, url, **kwargs)
            except httpx.HTTPError as e:
                last_status = -1
                logger.warning("network error (%s) on %s; retrying", e, url)
                time.sleep(2 ** attempt)
                continue
            if resp.status_code in (429, 503):
                last_status = resp.status_code
                sleep_for = 2 ** attempt
                logger.warning(
                    "%s returned %s; sleeping %ss", url, resp.status_code, sleep_for
                )
                time.sleep(sleep_for)
                continue
            resp.raise_for_status()
            return resp
        raise RuntimeError(
            f"gave up after {self.MAX_RETRIES} retries "
            f"(last_status={last_status}) for {url}"
        )

    # -- public --------------------------------------------------------------
    def get(self, url: str, **kwargs: Any) -> bytes:
        return self._request("GET", url, **kwargs).content

    def get_text(self, url: str, **kwargs: Any) -> str:
        return self._request("GET", url, **kwargs).text

    def post_text(self, url: str, **kwargs: Any) -> str:
        return self._request("POST", url, **kwargs).text

    def cookie(self, name: str) -> str:
        try:
            return self._client.cookies.get(name) or ""
        except Exception:
            return ""

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "CongressClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


# ════════════════════════════════════════════════════════════════════════════
# Pure parsing helpers (unit-tested, no network / DB)
# ════════════════════════════════════════════════════════════════════════════
_AMOUNT_NUM_RE = re.compile(r"\$\s*([\d,]+)")
_TICKER_PARENS_RE = re.compile(r"\(([A-Z][A-Z0-9.\-]{0,5})\)")
_TICKER_PARENS_LOOSE_RE = re.compile(r"\(([A-Za-z][A-Za-z0-9.\-]{1,5})\)")
_HTML_TAG_RE = re.compile(r"<[^>]+>")


def _clean_cell(value: Any) -> str:
    """Collapse whitespace/newlines in a (possibly None) table cell."""
    if value is None:
        return ""
    return " ".join(str(value).split())


def strip_accents(text: str) -> str:
    """'José' → 'Jose'. NFKD-decompose then drop combining marks."""
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(c for c in decomposed if not unicodedata.combining(c))


def parse_amount_range(text: Optional[str]) -> tuple[Optional[int], Optional[int]]:
    """Parse a disclosure amount range into (low, high).

    Handles: '$1,001 - $15,000' → (1001, 15000); '$1,001 -' → (1001, None);
    '$50,000,001 +' → (50000001, None); 'Over $50,000,000' → (50000000, None).
    Unparseable / empty → (None, None).
    """
    if not text:
        return (None, None)
    cleaned = _clean_cell(text)
    nums = [int(m.group(1).replace(",", "")) for m in _AMOUNT_NUM_RE.finditer(cleaned)]
    if not nums:
        return (None, None)
    low = nums[0]
    high = nums[1] if len(nums) > 1 else None
    return (low, high)


def map_tx_type(text: Optional[str]) -> Optional[str]:
    """Map House ('P'/'S'/'E', 'S (partial)') and Senate ('Purchase',
    'Sale (Full)', 'Sale (Partial)', 'Exchange') transaction labels to
    'buy' / 'sell' / 'exchange'. Unknown → None.
    """
    if not text:
        return None
    t = _clean_cell(text).lower()
    if not t:
        return None
    if t == "p" or t.startswith("p (") or t.startswith("p(") or t.startswith("purchase") or t == "buy":
        return "buy"
    if t == "s" or t.startswith("s (") or t.startswith("s(") or t.startswith("sale") or t == "sell":
        return "sell"
    if t == "e" or t.startswith("e (") or t.startswith("exchange"):
        return "exchange"
    return None


def slugify_name(full_name: str) -> str:
    """'José E. Serrano' → 'jose-e-serrano' (kebab-case, accents stripped)."""
    s = strip_accents(str(full_name or "")).lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")


def assign_slugs(people: list[dict]) -> list[str]:
    """Assign unique slugs for seed rows. Base slug = kebab-case full name;
    on collision append '-<state-lower>'; if STILL colliding (same name,
    same state) append '-<bioguide-lower>'.
    """
    base = [slugify_name(p.get("full_name", "")) for p in people]
    counts = Counter(base)
    seen: set[str] = set()
    slugs: list[str] = []
    for person, b in zip(people, base):
        slug = b
        if counts[b] > 1 or slug in seen:
            state = (person.get("state") or "").lower()
            slug = f"{b}-{state}".strip("-") if state else b
        if slug in seen:
            slug = f"{slug}-{(person.get('bioguide_id') or '').lower()}".strip("-")
        seen.add(slug)
        slugs.append(slug)
    return slugs


def extract_ticker(asset_text: Optional[str]) -> Optional[str]:
    """Pull a ticker out of parens in an asset description, e.g.
    'Apple Inc. - Common Stock (AAPL) [ST]' → 'AAPL'.

    Rejects lowercase contents ('(partial)') and anything longer than
    6 characters. Returns None when no candidate is found.
    """
    if not asset_text:
        return None
    cleaned = _clean_cell(asset_text)
    m = _TICKER_PARENS_RE.search(cleaned)
    if m:
        return m.group(1)
    # The Clerk's small-caps font mangles case in older e-filed PDFs ("(llTI)"
    # for LLTI): a parenthesised 2–6 letter/digit code with at least one
    # letter is upper-cased and offered — normalize_ticker still validates it
    # against ref_stocks, so a wrong guess never lands as a ticker.
    m2 = _TICKER_PARENS_LOOSE_RE.search(cleaned)
    if m2 and m2.group(1).lower() not in ("partial", "full"):
        return m2.group(1).upper()
    return None


def normalize_ticker(
    raw: Optional[str], ref_tickers: Optional[set[str]] = None
) -> Optional[str]:
    """Uppercase + strip; '--'/empty → None. When ``ref_tickers`` is given,
    unknown tickers also become None (the text stays in asset_description).
    """
    if raw is None:
        return None
    t = str(raw).strip().upper()
    if not t or t in ("--", "-", "N/A", "NONE"):
        return None
    if ref_tickers is not None and t not in ref_tickers:
        return None
    return t


def normalize_owner(text: Optional[str]) -> Optional[str]:
    """House codes (SP/JT/DC) and Senate labels → self/spouse/joint/dependent."""
    if text is None:
        return None
    t = _clean_cell(text).lower()
    mapping = {
        "": "self",
        "self": "self",
        "sp": "spouse",
        "spouse": "spouse",
        "jt": "joint",
        "joint": "joint",
        "dc": "dependent",
        "child": "dependent",
        "dependent": "dependent",
        "dependent child": "dependent",
    }
    return mapping.get(t, t or None)


def parse_us_date(text: Optional[str]) -> Optional[date]:
    """Parse '5/1/2026', '05/12/2026' or '2026-05-12' → date. Else None."""
    if not text:
        return None
    s = _clean_cell(text)
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def parse_house_index_xml(xml_bytes: bytes) -> list[dict]:
    """Parse the {year}FD.xml index inside the House FD zip.

    Returns one dict per <Member> node:
    {doc_id, last, first, filing_type, filing_date (date|None), year, state_dst}.
    Tag names are matched case-insensitively; members without a DocID are
    skipped.
    """
    parser = etree.XMLParser(recover=True, resolve_entities=False, no_network=True)
    root = etree.fromstring(xml_bytes, parser=parser)
    out: list[dict] = []
    if root is None:
        return out
    for node in root.iter():
        if not isinstance(node.tag, str) or node.tag.lower() != "member":
            continue
        fields: dict[str, str] = {}
        for child in node:
            if isinstance(child.tag, str):
                fields[child.tag.lower()] = (child.text or "").strip()
        doc_id = fields.get("docid", "")
        if not doc_id:
            continue
        year_str = fields.get("year", "")
        out.append(
            {
                "doc_id": doc_id,
                "last": fields.get("last", ""),
                "first": fields.get("first", ""),
                "filing_type": fields.get("filingtype", ""),
                "filing_date": parse_us_date(fields.get("filingdate", "")),
                "year": int(year_str) if year_str.isdigit() else None,
                "state_dst": fields.get("statedst", ""),
            }
        )
    return out


# Older e-filed PTRs (2013–2016) come out of pdfplumber with the WHOLE row in
# the first cell — no column rulings — while the header row still splits:
#   "sP laserlock Technologies, Inc. (llTI) s 09/16/2013 09/16/2013 $15,001 - $50,000\nFIlINg sTaTus: New"
# Owner prefix (SP/JT/DC) optional; the Clerk's small-caps font mangles case,
# so everything matches case-insensitively. Continuation lines belong to the
# asset until a status/subholding/description line (B1 step 9, 2026-09-30).
_MERGED_ROW_RE = re.compile(
    r"^(?:(?P<owner>sp|jt|dc)\s+)?(?P<asset>.+?)\s+(?P<type>p|s|e|s\s*\(partial\)|p\s*\(partial\))\s+"
    r"(?P<d1>\d{1,2}/\d{1,2}/\d{4})\s+(?P<d2>\d{1,2}/\d{1,2}/\d{4})\s+"
    r"(?P<amount>(?:\$[\d,]+\s*-\s*\$[\d,]+)|(?:over\s+\$[\d,]+)|(?:\$[\d,]+\s*\+?))\s*$",
    re.I,
)
_MERGED_STOP_RE = re.compile(r"^(filing\s+status|subholding\s+of|description|asset\s+class\s+details|location|comments?)\b", re.I)


def parse_merged_row(cell: str) -> Optional[dict]:
    """One transaction from a merged first cell; None when the first line is
    not a transaction line (status, heading, blank)."""
    lines = [l.strip() for l in (cell or "").split("\n") if l.strip()]
    if not lines:
        return None
    m = _MERGED_ROW_RE.match(lines[0])
    if not m:
        return None
    asset_parts = [m.group("asset").strip()]
    for line in lines[1:]:
        if _MERGED_STOP_RE.match(line):
            break
        asset_parts.append(line)
    tx_type = map_tx_type(m.group("type"))
    low, high = parse_amount_range(m.group("amount"))
    return {
        "owner": (m.group("owner") or "").upper(),
        "asset": " ".join(asset_parts),
        "transaction_type": tx_type,
        "transaction_date": parse_us_date(m.group("d1")),
        "notification_date": parse_us_date(m.group("d2")),
        "amount_low": low,
        "amount_high": high,
        "amount_text": m.group("amount"),
        "raw": lines,
    }


def classify_unparsed_pdf(text_chars: int, images: int, tables: int) -> str:
    """The stub reason: a paper filing scanned as images has no text layer
    ("scanned_paper" — OCR would be a new dependency, not built); a PDF with
    text we still could not read is "unparseable_pdf" (a parser gap); an
    empty file is "empty_pdf"."""
    if text_chars == 0 and images > 0:
        return "scanned_paper"
    if text_chars == 0 and tables == 0:
        return "empty_pdf"
    return "unparseable_pdf"


def parse_house_table(table: list[list[Optional[str]]]) -> list[dict]:
    """Parse one pdfplumber-extracted table from a House PTR PDF.

    Locates the header row (must mention 'Asset' plus a transaction-type
    column), maps column indices by header text, then parses data rows.
    Rows with no parseable type/date/amount (description or filing-status
    continuation rows) are skipped. Returns [] when no header is found.
    """
    if not table:
        return []
    header_idx: Optional[int] = None
    colmap: dict[str, int] = {}
    for i, row in enumerate(table):
        cells = [_clean_cell(c).lower() for c in row]
        if not any("asset" in c for c in cells):
            continue
        if not any("transaction" in c or c == "type" for c in cells):
            continue
        header_idx = i
        for j, c in enumerate(cells):
            if "owner" in c:
                colmap.setdefault("owner", j)
            elif "asset" in c:
                colmap.setdefault("asset", j)
            elif "transaction" in c or c == "type":
                colmap.setdefault("tx_type", j)
            elif "notification" in c:
                colmap.setdefault("notification", j)
            elif c == "date":
                colmap.setdefault("date", j)
            elif "amount" in c:
                colmap.setdefault("amount", j)
        break
    if header_idx is None or "asset" not in colmap:
        return []

    out: list[dict] = []
    for row in table[header_idx + 1 :]:
        cells = [_clean_cell(c) for c in row]
        # Merged shape: everything in the first cell, the rest empty.
        if cells and cells[0] and not any(cells[1:]):
            merged = parse_merged_row(str(row[0] or ""))
            if merged:
                out.append(merged)
            elif re.search(r"\d{1,2}/\d{1,2}/\d{4}", cells[0]) and "$" in cells[0]:
                # Looks like a transaction line the regex did not read — never silent.
                logger.warning("merged row not parsed: %s", cells[0][:120])
            continue

        def get(key: str) -> str:
            j = colmap.get(key)
            return cells[j] if j is not None and j < len(cells) else ""

        asset = get("asset")
        if not asset:
            continue
        tx_type = map_tx_type(get("tx_type"))
        tx_date = parse_us_date(get("date"))
        low, high = parse_amount_range(get("amount"))
        if tx_type is None and tx_date is None and low is None:
            continue  # continuation / description row
        out.append(
            {
                "owner": get("owner"),
                "asset": asset,
                "transaction_type": tx_type,
                "transaction_date": tx_date,
                "notification_date": parse_us_date(get("notification")),
                "amount_low": low,
                "amount_high": high,
                "amount_text": get("amount"),
                "raw": cells,
            }
        )
    return out


def parse_senate_ptr_html(html_text: str) -> list[dict]:
    """Parse an e-filed Senate PTR page's transaction <table> with lxml.

    Expected columns: # | Transaction Date | Owner | Ticker | Asset Name |
    Asset Type | Type | Amount | Comment. Header rows use <th> so they have
    no <td> cells and are skipped naturally.
    """
    if not html_text:
        return []
    try:
        doc = lxml_html.fromstring(html_text)
    except Exception:
        return []
    out: list[dict] = []
    for tr in doc.xpath("//table//tr"):
        cells = [_clean_cell(td.text_content()) for td in tr.xpath("./td")]
        if len(cells) < 8:
            continue
        ticker = normalize_ticker(cells[3])
        low, high = parse_amount_range(cells[7])
        out.append(
            {
                "transaction_date": parse_us_date(cells[1]),
                "owner": cells[2],
                "ticker": ticker,
                "asset_name": cells[4],
                "asset_type": cells[5],
                "transaction_type": map_tx_type(cells[6]),
                "amount_low": low,
                "amount_high": high,
                "amount_text": cells[7],
                "raw": cells,
            }
        )
    return out


# ════════════════════════════════════════════════════════════════════════════
# Politician name matching (accent-insensitive, suffix-tolerant)
# ════════════════════════════════════════════════════════════════════════════
def normalize_person_text(text: Optional[str]) -> str:
    s = strip_accents(str(text or "")).lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    return " ".join(s.split())


def build_name_index(rows: Iterable[tuple[int, str]]) -> dict:
    """Build a lookup over (id, full_name) rows with three key kinds:
    ('full', 'nancy pelosi'), ('fl', first_token, last_token), and
    ('l', last_token) — the latter two are dropped when ambiguous.
    """
    index: dict[Any, int] = {}
    ambiguous: set[Any] = set()
    for pid, full_name in rows:
        norm = normalize_person_text(full_name)
        if not norm:
            continue
        index.setdefault(("full", norm), pid)
        tokens = [t for t in norm.split() if t not in _NAME_SUFFIXES]
        if len(tokens) >= 2:
            fl_key = ("fl", tokens[0], tokens[-1])
            if fl_key in index and index[fl_key] != pid:
                ambiguous.add(fl_key)
            else:
                index[fl_key] = pid
        if tokens:
            l_key = ("l", tokens[-1])
            if l_key in index and index[l_key] != pid:
                ambiguous.add(l_key)
            else:
                index[l_key] = pid
    for key in ambiguous:
        index.pop(key, None)
    return index


def match_politician(
    index: dict, first: Optional[str], last: Optional[str]
) -> Optional[int]:
    """Match a filer's (first, last) against the index. Returns id or None."""
    nf = normalize_person_text(first)
    nl = normalize_person_text(last)
    last_tokens = [t for t in nl.split() if t not in _NAME_SUFFIXES]
    last_tok = last_tokens[-1] if last_tokens else ""
    first_tok = nf.split()[0] if nf else ""
    if nf and nl:
        pid = index.get(("full", f"{nf} {nl}"))
        if pid is not None:
            return pid
    if first_tok and last_tok:
        pid = index.get(("fl", first_tok, last_tok))
        if pid is not None:
            return pid
    if last_tok:
        return index.get(("l", last_tok))
    return None


# ════════════════════════════════════════════════════════════════════════════
# Storage helpers (psycopg2 connection via pipelines._shared.db)
# ════════════════════════════════════════════════════════════════════════════
TRADE_INSERT_SQL = """
    INSERT INTO congress.congress_trades (
        politician_id, ticker, asset_description, asset_type,
        transaction_type, transaction_date, filing_date,
        amount_low, amount_high, owner,
        source_doc_id, source_url, needs_review, raw_payload
    ) VALUES (
        %(politician_id)s, %(ticker)s, %(asset_description)s, %(asset_type)s,
        %(transaction_type)s, %(transaction_date)s, %(filing_date)s,
        %(amount_low)s, %(amount_high)s, %(owner)s,
        %(source_doc_id)s, %(source_url)s, %(needs_review)s, %(raw_payload)s::jsonb
    )
    ON CONFLICT (source_doc_id, ticker, transaction_date,
                 transaction_type, amount_low) DO NOTHING;
"""

POLITICIAN_UPSERT_SQL = """
    INSERT INTO congress.politicians (
        bioguide_id, full_name, slug, chamber, party, state, in_office, photo_url,
        committees
    ) VALUES (
        %(bioguide_id)s, %(full_name)s, %(slug)s, %(chamber)s,
        %(party)s, %(state)s, TRUE, %(photo_url)s, %(committees)s
    )
    ON CONFLICT (bioguide_id) DO UPDATE SET
        full_name = EXCLUDED.full_name,
        chamber   = EXCLUDED.chamber,
        party     = EXCLUDED.party,
        state     = EXCLUDED.state,
        in_office = TRUE,
        -- The enrichment (083) may have chosen a licensed Commons/Congress.gov
        -- portrait; the seed's unitedstates URL never overwrites it.
        photo_url = CASE WHEN congress.politicians.photo_source IN ('commons', 'congress.gov')
                         THEN congress.politicians.photo_url ELSE EXCLUDED.photo_url END,
        -- committees: keep the previous value rather than blanking when the
        -- committee fetch failed for this run (fail closed, never regress).
        committees = COALESCE(EXCLUDED.committees, congress.politicians.committees);
"""


def _fetch_committee_map(client: "CongressClient") -> "dict[str, list[dict]] | None":
    """bioguide -> [{"id": thomas_id, "name": committee name}], top-level
    committees only, sorted by name. Returns None (not {}) on any fetch or
    parse failure so the upsert's COALESCE keeps existing rows intact —
    a missing feed must never erase real seats (migration 062)."""
    try:
        committees = json.loads(client.get_text(COMMITTEES_URL))
        membership = json.loads(client.get_text(COMMITTEE_MEMBERSHIP_URL))
    except Exception as e:  # noqa: BLE001 — any failure means "no data this run"
        logger.warning("seed-politicians: committee fetch failed (%s) — keeping existing committees", e)
        return None
    names = {
        c.get("thomas_id"): c.get("name")
        for c in committees
        if c.get("thomas_id") and c.get("name")
    }
    out: dict[str, list[dict]] = {}
    for thomas_id, members in membership.items():
        name = names.get(thomas_id)
        if name is None:
            continue  # subcommittees ("SSAS01") and unknown ids are skipped
        for m in members or []:
            bioguide = m.get("bioguide")
            if bioguide:
                out.setdefault(bioguide, []).append({"id": thomas_id, "name": name})
    for seats in out.values():
        seats.sort(key=lambda s: s["name"])
    return out


def _existing_doc_ids(conn: Any) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT source_doc_id FROM congress.congress_trades")
        return {r[0] for r in cur.fetchall() if r[0]}


def _load_ref_tickers(conn: Any) -> Optional[set[str]]:
    """Known-good tickers from public.ref_stocks (the ground-truth ref table).
    Returns None (verification disabled) if the query fails or comes back
    empty, so a missing table never NULLs out every ticker.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT ticker FROM public.ref_stocks WHERE ticker IS NOT NULL")
            tickers = {str(r[0]).strip().upper() for r in cur.fetchall() if r[0]}
    except Exception as e:
        conn.rollback()
        logger.warning("could not load ref_stocks tickers (%s) — skipping verification", e)
        return None
    return tickers or None


def _politician_name_index(conn: Any) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT id, full_name FROM congress.politicians")
        rows = cur.fetchall()
    return build_name_index(rows)


def strip_nul(value: Any) -> Any:
    """Drop NUL bytes from every string, nested — Postgres text refuses \x00
    (box 30 Sep 2026: one 2022 PTR's text layer killed the back-fill)."""
    if isinstance(value, str):
        return value.replace("\x00", "").replace("\\u0000", "")
    if isinstance(value, dict):
        return {k: strip_nul(v) for k, v in value.items()}
    if isinstance(value, list):
        return [strip_nul(v) for v in value]
    return value


def _insert_trades(conn: Any, rows: list[dict]) -> int:
    """Insert trade rows (one commit per document). Returns rows inserted
    (ON CONFLICT duplicates count as 0 via cur.rowcount)."""
    if not rows:
        return 0
    inserted = 0
    cur = conn.cursor()
    try:
        for row in rows:
            cur.execute(TRADE_INSERT_SQL, strip_nul(row))
            inserted += max(cur.rowcount, 0)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
    return inserted


def _stub_row(
    politician_id: Optional[int],
    filing_date: date,
    source_doc_id: str,
    source_url: str,
    reason: str,
    extra: Optional[dict] = None,
) -> dict:
    """One needs_review placeholder row for an unparseable / paper filing."""
    payload = {"reason": reason}
    if extra:
        payload.update(extra)
    return {
        "politician_id": politician_id,
        "ticker": None,
        "asset_description": None,
        "asset_type": None,
        "transaction_type": None,
        "transaction_date": None,
        "filing_date": filing_date,
        "amount_low": None,
        "amount_high": None,
        "owner": None,
        "source_doc_id": source_doc_id,
        "source_url": source_url,
        "needs_review": True,
        "raw_payload": json.dumps(payload, default=str),
    }


# ════════════════════════════════════════════════════════════════════════════
# Seed politicians
# ════════════════════════════════════════════════════════════════════════════
def run_seed_politicians() -> int:
    """Download legislators-current.json and upsert ~540 rows into
    congress.politicians. Returns the number of rows upserted."""
    client = CongressClient()
    conn = None
    try:
        raw = json.loads(client.get_text(LEGISLATORS_URL))
        committee_map = _fetch_committee_map(client)
        people: list[dict] = []
        for entry in raw:
            bioguide = (entry.get("id") or {}).get("bioguide")
            terms = entry.get("terms") or []
            if not bioguide or not terms:
                continue
            name = entry.get("name") or {}
            full_name = name.get("official_full") or " ".join(
                part for part in (name.get("first"), name.get("last")) if part
            )
            if not full_name:
                continue
            last_term = terms[-1]  # chamber/party/state come from the LAST term
            chamber = {"rep": "house", "sen": "senate"}.get(last_term.get("type"))
            if chamber is None:
                continue
            people.append(
                {
                    "bioguide_id": bioguide,
                    "full_name": full_name,
                    "chamber": chamber,
                    "party": last_term.get("party"),
                    "state": last_term.get("state"),
                    "photo_url": PHOTO_URL_TPL.format(bioguide=bioguide),
                    # None (feed down) → COALESCE keeps the stored value;
                    # a fetched map always states a list, [] for no seats.
                    "committees": None
                    if committee_map is None
                    else json.dumps(committee_map.get(bioguide, [])),
                }
            )
        slugs = assign_slugs(people)
        for person, slug in zip(people, slugs):
            person["slug"] = slug

        conn = get_connection()
        cur = conn.cursor()
        try:
            # Reconcile batch slugs against rows already in the DB: a departed
            # member (absent from legislators-current.json) may still own a
            # colliding slug, which would violate UNIQUE(slug) and roll back
            # the whole seed. Fall back to -state, then -bioguide (unique).
            cur.execute("SELECT slug, bioguide_id FROM congress.politicians")
            owned = {s: b for s, b in cur.fetchall()}
            for person in people:
                slug = person["slug"]
                if owned.get(slug) not in (None, person["bioguide_id"]):
                    fallback = f"{slug}-{(person.get('state') or '').lower()}".rstrip("-")
                    if owned.get(fallback) not in (None, person["bioguide_id"]):
                        fallback = f"{slug}-{person['bioguide_id'].lower()}"
                    person["slug"] = fallback
            for person in people:
                cur.execute(POLITICIAN_UPSERT_SQL, person)
            # Anyone not in the current-legislators file has left office.
            cur.execute(
                "UPDATE congress.politicians SET in_office = false"
                " WHERE bioguide_id != ALL(%s)",
                ([p["bioguide_id"] for p in people],),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
        logger.info("seed-politicians: upserted %s rows", len(people))
        return len(people)
    finally:
        client.close()
        if conn is not None:
            conn.close()


# ════════════════════════════════════════════════════════════════════════════
# House flow
# ════════════════════════════════════════════════════════════════════════════
def _parse_house_pdf(pdf_bytes: bytes) -> list[dict]:
    """Extract transaction rows from a House PTR PDF with pdfplumber.
    Returns [] for scanned / unparseable filings (caller inserts a stub)."""
    return _parse_house_pdf_with_reason(pdf_bytes)[0]


def _parse_house_pdf_with_reason(pdf_bytes: bytes) -> tuple[list[dict], str]:
    """(rows, reason) — the reason names the shape when nothing parsed:
    scanned_paper · unparseable_pdf · empty_pdf · pdf_error."""
    import pdfplumber  # lazy import — tests of the pure functions don't need it

    rows: list[dict] = []
    text_chars = images = tables = 0
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text_chars += len(page.extract_text() or "")
                images += len(page.images or [])
                page_tables = page.extract_tables() or []
                tables += len(page_tables)
                for table in page_tables:
                    rows.extend(parse_house_table(table))
    except Exception as e:
        logger.warning("pdfplumber failed: %s", e)
        return [], "pdf_error"
    return rows, ("" if rows else classify_unparsed_pdf(text_chars, images, tables))


HOUSE_FIRST_PTR_YEAR = 2012   # the STOCK Act: PTRs (FilingType P) exist in the Clerk's indexes from 2012


def parse_years(spec: str) -> list[int]:
    """`2012-2026` · `2013,2019` · `2026` → sorted years, each ≥ 2012 (the plan's
    B1: "all time" means the whole published history, and the published
    history of House PTRs starts with the STOCK Act)."""
    years: set[int] = set()
    for part in (spec or "").split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            if not (a.strip().isdigit() and b.strip().isdigit()):
                raise ValueError(f"bad --years part {part!r}")
            lo, hi = sorted((int(a), int(b)))
            years.update(range(lo, hi + 1))
        elif part.isdigit():
            years.add(int(part))
        else:
            raise ValueError(f"bad --years part {part!r}")
    if not years:
        raise ValueError("--years is empty")
    bad = sorted(y for y in years if y < HOUSE_FIRST_PTR_YEAR)
    if bad:
        raise ValueError(f"House PTR indexes start in {HOUSE_FIRST_PTR_YEAR}; refusing {bad}")
    return sorted(years)


def run_house_years(years: list[int]) -> dict:
    """The back-fill (plan B1): one year at a time, oldest first, each resumable
    by `_existing_doc_ids` (a PDF already stored is never fetched twice), the
    parse rate stated per year so a bad year is a finding, not a silent gap.
    PDF bytes are never stored."""
    client = CongressClient()
    conn = None
    total = {"years": sorted(years), "per_year": {}, "ptrs": 0, "skipped": 0, "fetched": 0,
             "parsed": 0, "stubbed": 0, "inserted": 0}
    try:
        conn = get_connection()
        total["errors"] = {}
        for year in sorted(years):
            try:
                c = run_house_year(year, client=client, conn=conn)
            except Exception as e:  # noqa: BLE001 — one bad year never aborts the rest
                logger.error("house %s: failed (%s) — continuing", year, e)
                total["errors"][year] = str(e)
                continue
            docs = c["fetched"] or 0
            c["parse_rate"] = (docs - c["stubbed"]) / docs if docs else None
            total["per_year"][year] = c
            for k in ("ptrs", "skipped", "fetched", "parsed", "stubbed", "inserted"):
                total[k] += c.get(k, 0)
            logger.info("house %s: ptrs=%s skipped=%s fetched=%s parsed=%s stubbed=%s inserted=%s parse_rate=%s",
                        year, c["ptrs"], c["skipped"], c["fetched"], c["parsed"], c["stubbed"], c["inserted"],
                        "n/a" if c["parse_rate"] is None else f"{c['parse_rate']:.0%}")
        return total
    finally:
        client.close()
        if conn is not None:
            conn.close()


def _stub_docs(conn: Any, reasons: tuple[str, ...]) -> list[dict]:
    """The stub rows whose reason is in `reasons` — one per filing PDF:
    (id, source_doc_id, source_url, politician_id, filing_date, first, last)."""
    cur = conn.cursor()
    try:
        cur.execute(
            """SELECT id, source_doc_id, source_url, politician_id, filing_date, raw_payload
                 FROM congress.congress_trades
                WHERE transaction_type IS NULL AND ticker IS NULL AND amount_low IS NULL
                  AND source_url LIKE 'https://disclosures-clerk.house.gov/%%'
                ORDER BY filing_date, id""")
        out = []
        for row in cur.fetchall():
            payload = row[5] if isinstance(row[5], dict) else (json.loads(row[5]) if row[5] else {})
            # A true stub carries a `reason`; a thin parsed row (a real
            # transaction with no type/ticker/amount) does not and is left alone.
            if payload.get("reason") in reasons:
                out.append({"id": row[0], "source_doc_id": row[1], "source_url": row[2], "politician_id": row[3],
                            "filing_date": row[4], "first": payload.get("first", ""), "last": payload.get("last", "")})
        return out
    finally:
        cur.close()


def run_house_reparse(reasons: tuple[str, ...] = ("unparseable_pdf", "pdf_error"), limit: int = 0) -> dict:
    """Re-read the stub filings an older parser could not read (B1 step 9).
    Never a DELETE: when the PDF now parses, the stub row itself becomes the
    first transaction (UPDATE in place — the row id survives) and the rest
    are inserted; a still-unreadable PDF keeps its stub and gains the named
    reason. Resumable and idempotent."""
    client = CongressClient()
    conn = None
    counts = {"stubs": 0, "reparsed": 0, "rows": 0, "still_stub": 0}
    try:
        conn = get_connection()
        stubs = _stub_docs(conn, reasons)
        if limit:
            stubs = stubs[:limit]
        counts["stubs"] = len(stubs)
        ref_tickers = _load_ref_tickers(conn)
        for st in stubs:
            try:
                pdf_bytes = client.get(st["source_url"])
            except Exception as e:  # noqa: BLE001
                logger.warning("reparse: fetch failed for %s (%s)", st["source_doc_id"], e)
                continue
            parsed, reason = _parse_house_pdf_with_reason(pdf_bytes)
            cur = conn.cursor()
            try:
                if not parsed:
                    counts["still_stub"] += 1
                    cur.execute("UPDATE congress.congress_trades SET raw_payload = raw_payload::jsonb || %s::jsonb WHERE id = %s",
                                (json.dumps({"reason": reason}), st["id"]))
                    conn.commit()
                    continue
                rows = _rows_from_parsed(parsed, st["politician_id"], st["filing_date"], st["source_doc_id"], st["source_url"],
                                         st["first"], st["last"], ref_tickers)
                first, rest = rows[0], rows[1:]
                cur.execute(
                    """UPDATE congress.congress_trades SET ticker=%(ticker)s, asset_description=%(asset_description)s,
                           asset_type=%(asset_type)s, transaction_type=%(transaction_type)s, transaction_date=%(transaction_date)s,
                           amount_low=%(amount_low)s, amount_high=%(amount_high)s, owner=%(owner)s, needs_review=%(needs_review)s,
                           raw_payload=%(raw_payload)s
                     WHERE id = %(id)s""", strip_nul(dict(first, id=st["id"])))
                for r in rest:
                    cur.execute(TRADE_INSERT_SQL, strip_nul(r))
                conn.commit()
                counts["reparsed"] += 1
                counts["rows"] += len(rows)
            except Exception as e:  # noqa: BLE001
                conn.rollback()
                logger.warning("reparse: %s failed (%s)", st["source_doc_id"], e)
            finally:
                cur.close()
        logger.info("reparse: stubs=%(stubs)s reparsed=%(reparsed)s rows=%(rows)s still_stub=%(still_stub)s", counts)
        return counts
    finally:
        client.close()
        if conn is not None:
            conn.close()


def _rows_from_parsed(parsed: list[dict], politician_id, filing_date, source_id: str, pdf_url: str,
                      first: str, last: str, ref_tickers) -> list[dict]:
    """The trade rows for one parsed filing — the one shape both the year
    runner and the re-parse write."""
    rows: list[dict] = []
    for tx in parsed:
        ticker = normalize_ticker(extract_ticker(tx["asset"]), ref_tickers)
        asset_type_m = re.search(r"\[([A-Z]{2,3})\]", tx["asset"])
        rows.append(
            {
                "politician_id": politician_id,
                "ticker": ticker,
                "asset_description": tx["asset"],
                "asset_type": asset_type_m.group(1) if asset_type_m else None,
                "transaction_type": tx["transaction_type"],
                "transaction_date": tx["transaction_date"],
                "filing_date": filing_date,
                "amount_low": tx["amount_low"],
                "amount_high": tx["amount_high"],
                "owner": normalize_owner(tx["owner"]),
                "source_doc_id": source_id,
                "source_url": pdf_url,
                "needs_review": (
                    politician_id is None
                    or tx["transaction_type"] is None
                    or tx["transaction_date"] is None
                ),
                "raw_payload": json.dumps(
                    {"cells": tx["raw"], "last": last, "first": first},
                    default=str,
                ),
            }
        )
    return rows


def run_house(year: Optional[int] = None) -> dict:
    """House PTR flow for one year (the nightly cron's shape). Returns counts dict."""
    client = CongressClient()
    conn = None
    try:
        conn = get_connection()
        return run_house_year(year or date.today().year, client=client, conn=conn)
    finally:
        client.close()
        if conn is not None:
            conn.close()


def run_house_year(year: int, client: Any, conn: Any) -> dict:
    """One year's index → every new PTR PDF → rows or a stub. The caller owns
    the client and the connection (the back-fill shares them across years)."""
    zip_url = HOUSE_ZIP_URL_TPL.format(year=year)
    counts = {"year": year, "ptrs": 0, "skipped": 0, "fetched": 0, "parsed": 0, "stubbed": 0, "inserted": 0}

    logger.info("house: downloading %s", zip_url)
    zip_bytes = client.get(zip_url)
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    xml_names = [n for n in zf.namelist() if n.lower().endswith("fd.xml")]
    if not xml_names:
        logger.error("house: no FD.xml inside %s", zip_url)
        return counts
    members = parse_house_index_xml(zf.read(xml_names[0]))
    ptrs = [m for m in members if m["filing_type"] == "P"]
    existing = _existing_doc_ids(conn)
    new_ptrs = [m for m in ptrs if f"house:{m['doc_id']}" not in existing]
    counts["ptrs"] = len(ptrs)
    counts["skipped"] = len(ptrs) - len(new_ptrs)
    logger.info(
        "house: index has %s PTRs, %s new (year=%s)", len(ptrs), len(new_ptrs), year
    )

    name_index = _politician_name_index(conn)
    ref_tickers = _load_ref_tickers(conn)

    for m in new_ptrs:
        doc_id = m["doc_id"]
        pdf_url = HOUSE_PDF_URL_TPL.format(year=year, doc_id=doc_id)
        # Chamber-prefixed: House DocIDs and Senate ids share one TEXT column.
        source_id = f"house:{doc_id}"
        try:
            pdf_bytes = client.get(pdf_url)
        except Exception as e:
            logger.warning("house: fetch failed for %s (%s) — skipping", doc_id, e)
            continue
        counts["fetched"] += 1
        politician_id = match_politician(name_index, m["first"], m["last"])
        filing_date = m["filing_date"] or date.today()

        parsed, reason = _parse_house_pdf_with_reason(pdf_bytes)
        if not parsed:
            counts["stubbed"] += 1
            counts[reason] = counts.get(reason, 0) + 1   # the shape, per year: scanned_paper · unparseable_pdf · …
            counts["inserted"] += _insert_trades(
                conn,
                [
                    _stub_row(
                        politician_id,
                        filing_date,
                        source_id,
                        pdf_url,
                        reason,
                        extra={"last": m["last"], "first": m["first"]},
                    )
                ],
            )
            continue

        rows = _rows_from_parsed(parsed, politician_id, filing_date, source_id, pdf_url, m["first"], m["last"], ref_tickers)
        counts["parsed"] += len(rows)
        counts["inserted"] += _insert_trades(conn, rows)

    logger.info(
        "house: fetched=%(fetched)s parsed=%(parsed)s "
        "stubbed=%(stubbed)s inserted=%(inserted)s",
        counts,
    )
    return counts


# ════════════════════════════════════════════════════════════════════════════
# Senate flow
# ════════════════════════════════════════════════════════════════════════════
_CSRF_INPUT_RE = re.compile(
    r"name=['\"]csrfmiddlewaretoken['\"][^>]*value=['\"]([^'\"]+)['\"]"
)
_HREF_RE = re.compile(r"href=['\"]([^'\"]+)['\"]")


def _strip_html(text: str) -> str:
    return _clean_cell(_HTML_TAG_RE.sub(" ", text or ""))


def _senate_handshake(client: CongressClient) -> None:
    """GET the search home page (sets csrftoken), then POST the prohibition
    agreement so the session is allowed to hit the DataTables endpoint."""
    home_html = client.get_text(SENATE_HOME_URL)
    m = _CSRF_INPUT_RE.search(home_html)
    token = m.group(1) if m else client.cookie("csrftoken")
    client.post_text(
        SENATE_HOME_URL,
        data={"prohibition_agreement": "1", "csrfmiddlewaretoken": token},
        headers={"Referer": SENATE_HOME_URL},
    )


def _senate_search_reports(client: CongressClient, days: int) -> list[list[str]]:
    """Page through the DataTables endpoint for PTRs (report type 11)
    submitted in the trailing ``days``. Returns the raw data rows."""
    start_date = (date.today() - timedelta(days=days)).strftime("%m/%d/%Y") + " 00:00:00"
    rows: list[list[str]] = []
    start = 0
    for page in range(SENATE_MAX_PAGES):
        payload = {
            "draw": str(page + 1),
            "start": str(start),
            "length": str(SENATE_PAGE_SIZE),
            "report_types": "[11]",
            "filer_types": "[]",
            "submitted_start_date": start_date,
            "submitted_end_date": "",
            "candidate_state": "",
            "senator_state": "",
            "office_id": "",
            "first_name": "",
            "last_name": "",
        }
        body = client.post_text(
            SENATE_DATA_URL,
            data=payload,
            headers={
                "Referer": SENATE_SEARCH_REFERER,
                "X-CSRFToken": client.cookie("csrftoken"),
                "X-Requested-With": "XMLHttpRequest",
            },
        )
        data = json.loads(body)
        page_rows = data.get("data") or []
        rows.extend(page_rows)
        total = int(data.get("recordsTotal") or 0)
        start += SENATE_PAGE_SIZE
        if not page_rows or start >= total:
            break
    return rows


def senate_report_from_search_row(row: list) -> Optional[dict]:
    """One DataTables search row → the report record both Senate paths share:
    {report_id, ptr_link, filer_first, filer_last, filer_office, filed_date
    (ISO string or None), kind ("efiled"|"paper"), transactions: []}.
    None when the row carries no report link."""
    if len(row) < 5:
        return None
    link_m = _HREF_RE.search(str(row[3]))
    if not link_m:
        return None
    href = link_m.group(1)
    report_id = next((seg for seg in reversed(href.split("/")) if seg), "")
    if not report_id:
        return None
    filed = parse_us_date(_strip_html(str(row[4])))
    return {
        "report_id": report_id,
        "ptr_link": href if href.startswith("http") else SENATE_BASE + href,
        "filer_first": _strip_html(str(row[0])),
        "filer_last": _strip_html(str(row[1])),
        "filer_office": _strip_html(str(row[2])),
        "filed_date": filed.isoformat() if filed else None,
        "kind": "paper" if "/view/paper/" in href else "efiled",
        "transactions": [],
    }


def _as_date(value: Any) -> Optional[date]:
    """A date, or an ISO date string (the JSON leg), → date."""
    if value is None or isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def store_senate_reports(conn: Any, reports: list[dict]) -> dict:
    """The ONE Senate store path (live fetch and the US-fetched JSON both end
    here). Idempotent on the chamber-prefixed report id: a report whose
    ``senate:<id>`` is already stored is skipped. Paper filings store one
    link-only stub; an e-filed report with no parsed transactions stores an
    'unparseable_html' stub. Returns counts."""
    counts = {"reports": len(reports), "skipped": 0, "parsed": 0, "stubbed": 0, "inserted": 0}
    if not reports:
        return counts
    existing = _existing_doc_ids(conn)
    name_index = _politician_name_index(conn)
    ref_tickers = _load_ref_tickers(conn)

    for rep in reports:
        source_id = f"senate:{rep['report_id']}"
        if source_id in existing:
            counts["skipped"] += 1
            continue
        existing.add(source_id)
        first, last = rep.get("filer_first") or "", rep.get("filer_last") or ""
        source_url = rep["ptr_link"]
        filing_date = _as_date(rep.get("filed_date")) or date.today()
        politician_id = match_politician(name_index, first, last)
        trades = rep.get("transactions") or []

        if rep.get("kind") == "paper" or not trades:
            reason = "paper_filing" if rep.get("kind") == "paper" else "unparseable_html"
            counts["stubbed"] += 1
            counts["inserted"] += _insert_trades(
                conn,
                [
                    _stub_row(
                        politician_id, filing_date, source_id, source_url,
                        reason, extra={"first": first, "last": last},
                    )
                ],
            )
            continue

        rows: list[dict] = []
        for tx in trades:
            tx_date = _as_date(tx.get("transaction_date"))
            rows.append(
                {
                    "politician_id": politician_id,
                    "ticker": normalize_ticker(tx.get("ticker"), ref_tickers),
                    "asset_description": tx.get("asset_name"),
                    "asset_type": tx.get("asset_type") or None,
                    "transaction_type": tx.get("transaction_type"),
                    "transaction_date": tx_date,
                    "filing_date": filing_date,
                    "amount_low": tx.get("amount_low"),
                    "amount_high": tx.get("amount_high"),
                    "owner": normalize_owner(tx.get("owner")),
                    "source_doc_id": source_id,
                    "source_url": source_url,
                    "needs_review": (
                        politician_id is None
                        or tx.get("transaction_type") is None
                        or tx_date is None
                    ),
                    "raw_payload": json.dumps(
                        {"cells": tx.get("raw"), "first": first, "last": last},
                        default=str,
                    ),
                }
            )
        counts["parsed"] += len(rows)
        counts["inserted"] += _insert_trades(conn, rows)
    return counts


def fetch_senate_reports(
    client: CongressClient,
    days: int,
    skip_ids: Optional[set[str]] = None,
    reuse: Optional[dict[str, dict]] = None,
) -> tuple[list[dict], int]:
    """Handshake → search → one GET per e-filed PTR → parsed transactions.
    ``skip_ids`` (bare report ids) are left out without a fetch (the live
    path's already-stored set); ``reuse`` maps report id → a report already
    read in an earlier run, whose transactions are kept without a refetch.
    A 403 anywhere raises httpx.HTTPStatusError (the caller decides); any
    other failure on one PTR page skips that report so the next run retries
    it. Returns (reports, pages fetched)."""
    skip_ids = skip_ids or set()
    reuse = reuse or {}
    _senate_handshake(client)
    search_rows = _senate_search_reports(client, days)
    logger.info("senate: %s PTR filings in last %s days", len(search_rows), days)
    reports: list[dict] = []
    fetched = 0
    for row in search_rows:
        rep = senate_report_from_search_row(row)
        if rep is None or rep["report_id"] in skip_ids:
            continue
        if rep["kind"] == "efiled":
            prev = reuse.get(rep["report_id"])
            if prev and prev.get("transactions"):
                rep["transactions"] = prev["transactions"]
            else:
                try:
                    ptr_html = client.get_text(rep["ptr_link"])
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 403:
                        raise
                    logger.warning("senate: fetch failed for %s (%s) — skipping", rep["report_id"], e)
                    continue
                except Exception as e:  # noqa: BLE001 — one bad page never stops the run
                    logger.warning("senate: fetch failed for %s (%s) — skipping", rep["report_id"], e)
                    continue
                fetched += 1
                rep["transactions"] = parse_senate_ptr_html(ptr_html)
        reports.append(rep)
    return reports, fetched


def run_senate(days: int = 30, client: Optional[CongressClient] = None) -> dict:
    """Senate PTR flow over a trailing window (live fetch). Returns counts dict."""
    client = client or CongressClient()
    conn = None
    try:
        conn = get_connection()
        stored = {d.split(":", 1)[1] for d in _existing_doc_ids(conn) if d.startswith("senate:")}
        reports, fetched = fetch_senate_reports(client, days, skip_ids=stored)
        counts = {"fetched": fetched, **store_senate_reports(conn, reports)}
        logger.info(
            "senate: fetched=%(fetched)s parsed=%(parsed)s "
            "stubbed=%(stubbed)s inserted=%(inserted)s",
            counts,
        )
        return counts
    finally:
        client.close()
        if conn is not None:
            conn.close()


# The US-fetched copy (pipelines/congress/efd_fetch.py on GitHub Actions):
# efdsearch.senate.gov answers 403 to the box (EU) and to Melbourne, 200 to a
# US runner. The runner publishes public records only; the box reads them here.
EFD_DATA_REPO_DEFAULT = "danielstergiou/efd-senate-ptrs"
EFD_STALE_AFTER = timedelta(hours=12)


def default_efd_data_url() -> str:
    repo = os.environ.get("EFD_DATA_REPO") or EFD_DATA_REPO_DEFAULT
    return f"https://raw.githubusercontent.com/{repo}/main/efd/latest.json"


def load_efd_payload(url: str) -> dict:
    """The fetcher's JSON from a URL (or a local path, for a dry run)."""
    if url.startswith(("http://", "https://")):
        resp = httpx.get(url, timeout=60.0, follow_redirects=True)
        resp.raise_for_status()
        return resp.json()
    with open(url, encoding="utf-8") as fh:
        return json.load(fh)


def run_senate_from_url(url: str) -> dict:
    """Store the reports the US fetcher published. Same store path as the
    live leg (store_senate_reports), idempotent on report id."""
    payload = load_efd_payload(url)
    reports = payload.get("reports") or []
    fetched_at = payload.get("fetched_at")
    try:
        age = datetime.now(tz=timezone.utc) - datetime.fromisoformat(str(fetched_at).replace("Z", "+00:00"))
        if age > EFD_STALE_AFTER:
            logger.warning("senate: the eFD copy at %s is stale (fetched_at=%s)", url, fetched_at)
    except ValueError:
        logger.warning("senate: the eFD copy at %s has no readable fetched_at (%r)", url, fetched_at)
    conn = get_connection()
    try:
        counts = store_senate_reports(conn, reports)
    finally:
        conn.close()
    logger.info(
        "senate (from %s, fetched_at=%s): reports=%s skipped=%s parsed=%s stubbed=%s inserted=%s",
        url, fetched_at, counts["reports"], counts["skipped"], counts["parsed"],
        counts["stubbed"], counts["inserted"],
    )
    return counts


# ════════════════════════════════════════════════════════════════════════════
# CLI dispatch
# ════════════════════════════════════════════════════════════════════════════
def _main(argv: list[str]) -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(
        prog="python -m pipelines.congress.congress_trades",
        description=(
            "Congressional PTR scraper. With no flags, runs the House and "
            "Senate flows incrementally (current year / 30-day lookback)."
        ),
    )
    parser.add_argument(
        "--seed-politicians",
        action="store_true",
        help="download legislators-current.json and upsert congress.politicians",
    )
    parser.add_argument("--house", action="store_true", help="run the House PTR flow")
    parser.add_argument("--senate", action="store_true", help="run the Senate PTR flow")
    parser.add_argument(
        "--year", type=int, default=None, help="House filing year (default: current)"
    )
    parser.add_argument(
        "--reparse-stubs", action="store_true",
        help="House: re-read the stub filings an older parser could not read (UPDATE in place, never a delete)",
    )
    parser.add_argument(
        "--years", default=None,
        help="House back-fill: a range or list of filing years, e.g. 2012-2026 or 2013,2019 "
             "(one year at a time, resumable — stored PDFs are never re-fetched)",
    )
    parser.add_argument(
        "--days", type=int, default=30, help="Senate lookback in days (default: 30)"
    )
    parser.add_argument(
        "--senate-from-url", nargs="?", const="", default=None, metavar="URL",
        help="Senate: store the reports the US fetcher (efd_fetch.py on GitHub Actions) "
             "published, instead of the live eFD fetch. URL (or a local path) defaults to "
             "$EFD_DATA_URL, else raw.githubusercontent.com/<$EFD_DATA_REPO or "
             f"{EFD_DATA_REPO_DEFAULT}>/main/efd/latest.json. Implies --senate.",
    )
    args = parser.parse_args(argv)

    # The box cron runs the bare module; EFD_DATA_URL switches its Senate leg
    # to the US-fetched copy without touching the cron line.
    senate_url: Optional[str] = None
    if args.senate_from_url is not None:
        senate_url = args.senate_from_url or os.environ.get("EFD_DATA_URL") or default_efd_data_url()
    elif os.environ.get("EFD_DATA_URL"):
        senate_url = os.environ["EFD_DATA_URL"]

    ran_any = False
    if args.seed_politicians:
        run_seed_politicians()
        ran_any = True

    do_house = args.house
    do_senate = args.senate or args.senate_from_url is not None
    if not args.seed_politicians and not args.house and not do_senate:
        do_house = do_senate = True  # default: both flows, incremental

    if args.reparse_stubs:
        run_house_reparse()
        ran_any = True
        do_house = do_house and (args.years or args.year)
        do_senate = False
    if do_house:
        if args.years:
            run_house_years(parse_years(args.years))
        else:
            run_house(year=args.year)
        ran_any = True
    if do_senate and senate_url:
        try:
            run_senate_from_url(senate_url)
        except (httpx.HTTPError, OSError, ValueError) as e:
            # Same posture as the live leg's 403: a missing/unreadable copy is a
            # WARNING, never a failed House run. The fetcher's own workflow
            # fails loudly on GitHub; the ingester logs the staleness here.
            logger.warning("senate: could not read the eFD copy at %s (%s); skipping the Senate leg", senate_url, e)
        ran_any = True
    elif do_senate:
        try:
            run_senate(days=args.days)
        except httpx.HTTPStatusError as e:
            # efdsearch.senate.gov has answered 403 to every client (box AND
            # laptop, browser UA or not) since 2026-08-09 — an upstream bot
            # wall, not our IP. A traceback here failed the whole cron and
            # silenced the td-congress healthcheck for a month while the House
            # leg was fine. Degrade to a WARNING; the leg resumes by itself the
            # day the site answers again.
            if e.response.status_code == 403:
                logger.warning(
                    "senate: eFD returned 403 at %s; skipping the Senate leg "
                    "(upstream bot wall since 2026-08-09)", e.request.url,
                )
            else:
                raise
        ran_any = True

    return 0 if ran_any else 2


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv[1:]))
