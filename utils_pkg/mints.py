# utils_pkg/mints.py
from __future__ import annotations

"""
utils_pkg/mints.py — Mint normalization + strict pubkey extraction (P0 LOCK-IN)
==============================================================================

Single source of truth for turning “whatever the feed hands us” into a canonical
Solana mint pubkey string (base58, STRICT 32-byte decode).

LOCK-IN requirements
--------------------
✅ ZERO network at import time
✅ STRICT validation: only accept strings that decode to exactly 32 bytes base58
✅ Robust extraction from:
   - symbols (SOL/USDC/USDT/BONK…)
   - raw pubkeys
   - pump.fun / dexscreener / birdeye / raydium / random URLs
   - JSON/log blobs containing multiple pubkeys (deterministic best pick)
✅ Heuristics to avoid “pairAddress/poolAddress” being mistaken as mint
✅ Deterministic and safe: no random choices, no brittle parsing

Public API
----------
- normalize_mint(x) -> str            (raises ValueError)
- try_normalize_mint(x) -> str|None   (never raises)
- is_base58_pubkey(x) -> bool         (strict 32-byte base58)
- extract_pubkeys(blob) -> list[str]  (all strict pubkeys found, deduped, stable)

Optional helpers (safe)
-----------------------
- normalize_mint_pair(obj) -> (base_mint|None, quote_mint|None)
- is_known_mint(mint) -> bool
- is_stable_mint(mint) -> bool
"""

import re
from collections.abc import Iterable, Sequence
from functools import lru_cache
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

# ──────────────────────────────────────────────────────────────────────────────
# Canonical mints (Solana mainnet)
# ──────────────────────────────────────────────────────────────────────────────
SOL_MINT = "So11111111111111111111111111111111111111112"  # wSOL pseudo-mint
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
BONK_MINT = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"

KNOWN_MINTS = {
    SOL_MINT,
    USDC_MINT,
    USDT_MINT,
    BONK_MINT,
}

STABLE_MINTS = {
    USDC_MINT,
    USDT_MINT,
}

# Known bad/typo mints seen in the wild (auto-fix to canonical)
_BAD_MINT_FIXUPS: dict[str, str] = {
    # Example typo seen in logs/feeds
    "EPjFWdd5AufqSSqeMzQ1NzxypaC8G4weEGKzwyTDt1v": USDC_MINT,
}

# Common symbol aliases (case-insensitive; we normalize symbols heavily)
_SYM_TO_MINT: dict[str, str] = {
    "SOL": SOL_MINT,
    "WSOL": SOL_MINT,
    "W SOL": SOL_MINT,
    "W-SOL": SOL_MINT,
    "W_SOL": SOL_MINT,
    "WRAPPED SOL": SOL_MINT,
    "USDC": USDC_MINT,
    "USD COIN": USDC_MINT,
    "USDCOIN": USDC_MINT,
    "USDC CCTP": USDC_MINT,
    "USDT": USDT_MINT,
    "TETHER": USDT_MINT,
    "BONK": BONK_MINT,
}

# ──────────────────────────────────────────────────────────────────────────────
# Strict base58 32-byte validation (pure python, cached)
# ──────────────────────────────────────────────────────────────────────────────
_B58_ALPH = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_B58SET = set(_B58_ALPH)
_B58IDX = {c: i for i, c in enumerate(_B58_ALPH)}

# Typical Solana pubkey strings are ~32..44 chars. We allow slightly wider,
# but STRICTLY require decoded byte length == 32.
_MIN_B58_LEN = 32
_MAX_B58_LEN = 60

# Broad "base58-looking" finder for blobs (URLs, logs, JSON, etc.)
_B58_CHUNK_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{20,90}")

# Quick JSON-ish key/value finder without json.loads (logs often aren’t valid JSON)
_JSON_TOKEN_RE = re.compile(
    r'(?P<k>"?[A-Za-z_][A-Za-z0-9_]*"?)\s*:\s*(?P<v>"[^"]{16,200}"|[1-9A-HJ-NP-Za-km-z]{20,90})'
)

# Keys that *usually* hold actual token mint addresses
_HIGH_SIGNAL_KEYS = (
    "mint",
    "token",
    "address",
    "tokenaddress",
    "mintaddress",
    "inputmint",
    "outputmint",
    "basemint",
    "quotemint",
    "token_mint",
    "coin_mint",
    "ca",  # “contract address” in some feeds
)

# Keys that might be pubkeys but are often *NOT* mints (pair/pool/program/wallet)
_LOW_SIGNAL_KEYS = (
    "pairaddress",
    "pooladdress",
    "ammid",
    "market",
    "program",
    "programid",
    "authority",
    "owner",
    "wallet",
    "lp",
    "lpmint",
)

# When given a dict input, check these keys first (in order).
_DICT_SIGNAL_KEYS = (
    # Most likely mint keys
    "mint",
    "address",
    "token",
    "tokenAddress",
    "token_address",
    "mintAddress",
    "mint_address",
    "inputMint",
    "outputMint",
    "baseMint",
    "quoteMint",
    "ca",
    # Low-signal fallbacks (only if nothing else works)
    "lpMint",
    "lp_mint",
    "pairAddress",
    "poolAddress",
)

# Some feeds tack suffix words onto base58-ish strings; we strip conservatively.
_SUFFIX_WORDS = (
    "pumpfun",
    "pump",
    "raydium",
    "dexscreener",
    "birdeye",
    "solana",
    "token",
    "coin",
    "mint",
)


# ──────────────────────────────────────────────────────────────────────────────
# Small helpers
# ──────────────────────────────────────────────────────────────────────────────
def _as_text(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    if isinstance(x, bytes | bytearray):
        try:
            return x.decode("utf-8", errors="ignore")
        except Exception:
            return str(x)
    try:
        return str(x)
    except Exception:
        return ""


def _apply_fixups(s: str) -> str:
    ss = (s or "").strip()
    return _BAD_MINT_FIXUPS.get(ss, ss)


def _strip_punct(s: str) -> str:
    return (s or "").strip(" \t\r\n'\"<>[](){}.,;:")


@lru_cache(maxsize=200_000)
def _b58decode(s: str) -> bytes:
    """
    Minimal base58 decode (cached).
    Used only for strict 32-byte check.

    Raises ValueError for invalid chars.
    """
    ss = (s or "").strip()
    if not ss:
        return b""
    n = 0
    for ch in ss:
        i = _B58IDX.get(ch)
        if i is None:
            raise ValueError("invalid base58 char")
        n = n * 58 + i
    raw = n.to_bytes((n.bit_length() + 7) // 8, "big") if n else b""
    pad = len(ss) - len(ss.lstrip("1"))
    return (b"\x00" * pad) + raw


@lru_cache(maxsize=300_000)
def _is_base58_32(s: str) -> bool:
    ss = (s or "").strip()
    if not ss:
        return False
    if len(ss) < _MIN_B58_LEN or len(ss) > _MAX_B58_LEN:
        return False
    # fast reject
    for ch in ss:
        if ch not in _B58SET:
            return False
    try:
        return len(_b58decode(ss)) == 32
    except Exception:
        return False


def is_base58_pubkey(x: Any) -> bool:
    """Public strict validator: True iff `x` decodes to exactly 32 bytes base58."""
    return _is_base58_32(_apply_fixups(_strip_punct(_as_text(x))))


def _normalize_symbol(sym: str) -> str | None:
    """
    Normalize common symbols & variants to canonical mints.
    """
    s = (sym or "").strip().upper()
    if not s:
        return None

    # Normalize separators and whitespace
    s = s.replace("/", " ").replace("-", " ").replace("_", " ")
    s = " ".join(s.split()).strip()

    if s in _SYM_TO_MINT:
        return _SYM_TO_MINT[s]
    # Try without spaces
    s2 = s.replace(" ", "")
    return _SYM_TO_MINT.get(s2)


def _clean_candidate(cand: str) -> str:
    """
    Conservative cleanup:
      - unquote URL encoding
      - strip punctuation
      - if delimiters exist, keep base58 prefix
      - strip known suffix words if remaining chars are base58
      - apply known fixups
    """
    s = _strip_punct(unquote((cand or "").strip()))
    if not s:
        return s

    # If it starts with base58 and then hits a delimiter, keep prefix.
    prefix = []
    for ch in s:
        if ch in _B58SET:
            prefix.append(ch)
        else:
            break
    if len(prefix) >= 20:
        s = "".join(prefix)

    # Strip suffix words only if remaining stays base58-only
    low = s.lower()
    changed = True
    while changed:
        changed = False
        for suf in _SUFFIX_WORDS:
            if low.endswith(suf) and len(s) > len(suf) + _MIN_B58_LEN:
                s2 = s[: -len(suf)]
                if s2 and all((c in _B58SET) for c in s2):
                    s = s2
                    low = s.lower()
                    changed = True
                break

    # Cap length to avoid pathological huge strings
    if len(s) > _MAX_B58_LEN:
        s = s[:_MAX_B58_LEN]

    return _apply_fixups(s)


def _yield_url_tokens(text: str) -> Iterable[tuple[str, int]]:
    """
    Parse URL-ish text and yield candidate tokens with priority scores.
    Higher score = higher confidence.
    """
    t = (text or "").strip()
    if not t:
        return

    u = urlparse(t)
    if u.scheme and u.netloc:
        # Query params: highest signal
        qs = parse_qs(u.query or "", keep_blank_values=False)
        for k, vals in qs.items():
            kk = (k or "").strip().lower()
            if not vals:
                continue
            if kk in _HIGH_SIGNAL_KEYS:
                base = 400
            elif kk in _LOW_SIGNAL_KEYS:
                base = 60
            else:
                base = 160
            for v in vals:
                if v:
                    yield v, base

        # Fragment sometimes contains the token
        if u.fragment:
            for seg in re.split(r"[/?#&=]", u.fragment):
                if seg:
                    yield seg, 120

        # Path segments: medium signal (pump.fun often embeds mint in path)
        segs = [s for s in (u.path or "").split("/") if s]
        for i, seg in enumerate(segs):
            # pump.fun patterns: /coin/<mint> or /token/<mint>
            low = seg.lower()
            if low in ("coin", "token"):
                # next segment gets boosted by outer loop too (order tie-break)
                yield seg, 10
            yield seg, 140 + min(i, 10)
    else:
        # URL-ish split
        for seg in re.split(r"[/?#&=\s]", t):
            if seg:
                yield seg, 90


def _yield_jsonish_tokens(text: str) -> Iterable[tuple[str, int]]:
    """
    Light JSON-ish extraction for logs containing "mint":"..." or inputMint: ...
    """
    t = (text or "").strip()
    if not t:
        return
    for m in _JSON_TOKEN_RE.finditer(t):
        k = (m.group("k") or "").strip().strip('"').lower()
        v = (m.group("v") or "").strip().strip('"')
        if not v:
            continue
        if k in _HIGH_SIGNAL_KEYS:
            score = 320
        elif k in _LOW_SIGNAL_KEYS:
            score = 50
        else:
            score = 110
        yield v, score


def extract_pubkeys(blob: Any) -> list[str]:
    """
    Extract ALL strict 32-byte base58 pubkeys from a blob/string.
    Deduped, stable order:
      1) High-signal URL parsing
      2) JSON-ish extraction
      3) Broad scan
    """
    t = _as_text(blob).strip()
    if not t:
        return []

    seen = set()
    out: list[str] = []

    for tok, _score in _yield_url_tokens(t):
        c = _clean_candidate(tok)
        if _is_base58_32(c) and c not in seen:
            seen.add(c)
            out.append(c)

    for tok, _score in _yield_jsonish_tokens(t):
        c = _clean_candidate(tok)
        if _is_base58_32(c) and c not in seen:
            seen.add(c)
            out.append(c)

    for cand in _B58_CHUNK_RE.findall(t):
        c = _clean_candidate(cand)
        if _is_base58_32(c) and c not in seen:
            seen.add(c)
            out.append(c)

        # If the blob chunk is very long, the tail might contain a mint
        if len(cand) > _MAX_B58_LEN:
            tail = _clean_candidate(cand[-_MAX_B58_LEN:])
            if _is_base58_32(tail) and tail not in seen:
                seen.add(tail)
                out.append(tail)

    return out


def _extract_scored_candidates(text: str) -> list[tuple[str, int, int]]:
    """
    Internal: produce (pubkey, score, order_index) for deterministic best-pick.
    Higher score is better; ties broken by later order_index.
    """
    t = (text or "").strip()
    if not t:
        return []

    scored: list[tuple[str, int, int]] = []
    order = 0

    for tok, score in _yield_url_tokens(t):
        c = _clean_candidate(tok)
        if _is_base58_32(c):
            scored.append((c, int(score), order))
            order += 1

    for tok, score in _yield_jsonish_tokens(t):
        c = _clean_candidate(tok)
        if _is_base58_32(c):
            scored.append((c, int(score), order))
            order += 1

    for cand in _B58_CHUNK_RE.findall(t):
        c = _clean_candidate(cand)
        if _is_base58_32(c):
            scored.append((c, 30, order))
            order += 1
        if len(cand) > _MAX_B58_LEN:
            tail = _clean_candidate(cand[-_MAX_B58_LEN:])
            if _is_base58_32(tail):
                scored.append((tail, 25, order))
                order += 1

    # Dedup: keep best (score, order)
    best: dict[str, tuple[int, int]] = {}
    for pk, sc, idx in scored:
        prev = best.get(pk)
        if prev is None:
            best[pk] = (sc, idx)
        else:
            psc, pidx = prev
            if sc > psc or (sc == psc and idx > pidx):
                best[pk] = (sc, idx)

    out = [(pk, sc, idx) for pk, (sc, idx) in best.items()]
    out.sort(key=lambda x: (x[1], x[2]))  # best is last
    return out


def _pick_best_candidate(scored: Sequence[tuple[str, int, int]]) -> str | None:
    """
    Deterministic selection when multiple pubkeys are present:

    Preferences:
      1) Prefer not-low-signal (i.e., higher score already captures this)
      2) Prefer non-SOL if both exist (Jupiter is often passed token mints)
      3) Prefer non-stables if both exist (when scanning memes, base mint matters)
         BUT do not force this if only stable exists.
      4) Prefer highest score; tie-break by latest order
    """
    if not scored:
        return None

    # Highest score already pushes "mint/inputMint/outputMint" above pool/pair ids.
    ordered = list(scored)

    # Prefer non-SOL
    non_sol = [t for t in ordered if t[0] != SOL_MINT]
    pool1 = non_sol if non_sol else ordered

    # Prefer non-stable (only if we have at least one non-stable)
    non_stable = [t for t in pool1 if t[0] not in STABLE_MINTS]
    pool2 = non_stable if non_stable else pool1

    return pool2[-1][0] if pool2 else None


@lru_cache(maxsize=250_000)
def _normalize_text(text: str) -> str:
    """
    Cached normalizer for TEXT inputs only.
    """
    s = (text or "").strip()
    if not s:
        raise ValueError("empty mint")

    # 0) exact fixups
    s = _apply_fixups(s)

    # 1) symbol alias?
    aliased = _normalize_symbol(s)
    if aliased:
        return aliased

    # 2) exact pubkey?
    c0 = _clean_candidate(s)
    if _is_base58_32(c0):
        return c0

    # 3) scored extraction from url/json/blob
    scored = _extract_scored_candidates(s)
    best = _pick_best_candidate(scored)
    if best and _is_base58_32(best):
        return best

    # 4) fallback: stable list extractor
    cands = extract_pubkeys(s)
    if cands:
        # Reuse pick logic by scoring them uniformly low
        scored2 = [(c, 10, i) for i, c in enumerate(cands)]
        best2 = _pick_best_candidate(scored2)
        if best2:
            return best2

    raise ValueError(f"cannot normalize mint from input: {s!r}")


def normalize_mint(x: Any) -> str:
    """
    Return canonical 32-byte base58 mint (string).

    Accepts:
      - Symbols: SOL/WSOL/USDC/USDT/BONK...
      - Exact pubkey strings
      - pump.fun / dexscreener / birdeye URLs
      - Any blob containing one or more pubkeys
      - dict/list inputs (tries high-signal keys / elements first)
    """
    if x is None:
        raise ValueError("empty mint")

    # dict input: try signal keys first
    if isinstance(x, dict):
        # 1) high-signal keys in order
        for k in _DICT_SIGNAL_KEYS:
            if k in x and x.get(k) is not None:
                try:
                    return normalize_mint(x.get(k))
                except Exception:
                    pass

        # 2) if dict contains nested pair-like objects, try to find base/quote mints
        base, quote = normalize_mint_pair(x)
        if base:
            return base
        if quote:
            return quote

        # 3) scan values deterministically by insertion order
        last_err: Exception | None = None
        for v in x.values():
            if v is None:
                continue
            try:
                return normalize_mint(v)
            except Exception as e:
                last_err = e
                continue
        raise ValueError(f"cannot normalize mint from dict input: {last_err!r}")

    # list/tuple/set: scan items
    if isinstance(x, list | tuple | set):
        last_err: Exception | None = None
        for it in x:
            try:
                return normalize_mint(it)
            except Exception as e:
                last_err = e
                continue
        raise ValueError(f"cannot normalize mint from sequence input: {last_err!r}")

    s = _as_text(x)
    return _normalize_text(s)


def try_normalize_mint(x: Any) -> str | None:
    """Non-throwing version of normalize_mint()."""
    try:
        return normalize_mint(x)
    except Exception:
        return None


def normalize_mint_pair(obj: Any) -> tuple[str | None, str | None]:
    """
    Best-effort extraction of (base_mint, quote_mint) from common shapes.
    Never raises.
    """
    try:
        if isinstance(obj, dict):
            # common keys for base/quote
            base = try_normalize_mint(
                obj.get("baseMint") or obj.get("base_mint") or obj.get("base")
            )
            quote = try_normalize_mint(
                obj.get("quoteMint") or obj.get("quote_mint") or obj.get("quote")
            )
            # Jupiter-ish keys
            if base is None:
                base = try_normalize_mint(obj.get("inputMint"))
            if quote is None:
                quote = try_normalize_mint(obj.get("outputMint"))
            return base, quote
    except Exception:
        pass
    return None, None


def is_known_mint(mint: str) -> bool:
    m = (mint or "").strip()
    return m in KNOWN_MINTS


def is_stable_mint(mint: str) -> bool:
    m = (mint or "").strip()
    return m in STABLE_MINTS


__all__ = [
    "BONK_MINT",
    "KNOWN_MINTS",
    "SOL_MINT",
    "STABLE_MINTS",
    "USDC_MINT",
    "USDT_MINT",
    "extract_pubkeys",
    "is_base58_pubkey",
    "is_known_mint",
    "is_stable_mint",
    "normalize_mint",
    "normalize_mint_pair",
    "try_normalize_mint",
]
