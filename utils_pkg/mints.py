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
✅ Heuristics to avoid “pairAddress/poolAddress/programId/wallet” being mistaken as mint
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
from collections.abc import Iterator, Mapping, Sequence
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

KNOWN_MINTS = frozenset({SOL_MINT, USDC_MINT, USDT_MINT, BONK_MINT})
STABLE_MINTS = frozenset({USDC_MINT, USDT_MINT})

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
_B58SET = frozenset(_B58_ALPH)
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

# "Labelled" patterns commonly seen in Telegram / logs: "CA: <mint>" etc.
_LABELLED_RE = re.compile(
    r"(?i)\b(?:ca|mint|token|address|contract)\b\s*[:=]\s*([1-9A-HJ-NP-Za-km-z]{20,90})"
)

# Keys that *usually* hold actual token mint addresses
_HIGH_SIGNAL_KEYS = frozenset(
    {
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
    }
)

# Keys that might be pubkeys but are often *NOT* mints (pair/pool/program/wallet)
_LOW_SIGNAL_KEYS = frozenset(
    {
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
    }
)

# When given a dict input, check these keys first (in order).
_DICT_SIGNAL_KEYS: tuple[str, ...] = (
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
_SUFFIX_WORDS: tuple[str, ...] = (
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

# Well-known program IDs that should never be treated as token mints.
# (We still treat them as valid PUBKEYS for extract_pubkeys/is_base58_pubkey.)
_NON_MINT_PUBKEYS = frozenset(
    {
        "11111111111111111111111111111111",  # system program
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",  # SPL token program
        "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",  # ATA program
        "ComputeBudget111111111111111111111111111111",  # compute budget program
        "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr",  # memo program
        "SysvarRent111111111111111111111111111111111",  # sysvar rent
        "SysvarC1ock11111111111111111111111111111111",  # sysvar clock (variant)
    }
)


# ──────────────────────────────────────────────────────────────────────────────
# Small helpers
# ──────────────────────────────────────────────────────────────────────────────
def _as_text(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    if isinstance(x, (bytes, bytearray, memoryview)):
        try:
            b = bytes(x)
            return b.decode("utf-8", errors="ignore")
        except Exception:
            return ""
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
def _is_strict_pubkey(s: str) -> bool:
    """
    True iff s is base58 and decodes to exactly 32 bytes.
    """
    ss = (s or "").strip()
    if not ss:
        return False
    if len(ss) < _MIN_B58_LEN or len(ss) > _MAX_B58_LEN:
        return False
    for ch in ss:
        if ch not in _B58SET:
            return False
    try:
        return len(_b58decode(ss)) == 32
    except Exception:
        return False


def _is_mint_pubkey(s: str) -> bool:
    """
    Strict pubkey that is not a known program/sysvar id.
    """
    return _is_strict_pubkey(s) and s not in _NON_MINT_PUBKEYS


def is_base58_pubkey(x: Any) -> bool:
    """Public strict validator: True iff `x` decodes to exactly 32 bytes base58."""
    return _is_strict_pubkey(_apply_fixups(_strip_punct(_as_text(x))))


def _normalize_symbol(sym: str) -> str | None:
    """
    Normalize common symbols & variants to canonical mints.
    """
    s = (sym or "").strip().upper()
    if not s:
        return None

    s = s.replace("/", " ").replace("-", " ").replace("_", " ")
    s = " ".join(s.split()).strip()

    if s in _SYM_TO_MINT:
        return _SYM_TO_MINT[s]
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
    prefix_chars: list[str] = []
    for ch in s:
        if ch in _B58SET:
            prefix_chars.append(ch)
        else:
            break
    if len(prefix_chars) >= 20:
        s = "".join(prefix_chars)

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

    if len(s) > _MAX_B58_LEN:
        s = s[:_MAX_B58_LEN]

    return _apply_fixups(s)


def _key_score(key: str) -> int:
    kk = (key or "").strip().lower()
    if kk in _HIGH_SIGNAL_KEYS:
        return 420
    if kk in _LOW_SIGNAL_KEYS:
        return 80
    # Heuristic: keys containing "mint" or "token" are usually relevant
    if "mint" in kk or "token" in kk or "address" in kk:
        return 220
    return 160


def _yield_url_tokens(text: str) -> Iterator[tuple[str, int]]:
    """
    Parse URL-ish text and yield candidate tokens with priority scores.
    Higher score = higher confidence.
    """
    t = (text or "").strip()
    if not t:
        return

    u = urlparse(t)
    host = (u.netloc or "").lower()

    if u.scheme and u.netloc:
        # Query params: highest signal
        qs = parse_qs(u.query or "", keep_blank_values=False)
        for k, vals in qs.items():
            if not vals:
                continue
            base = _key_score(k)
            for v in vals:
                if v:
                    yield v, base

        # Fragment sometimes contains the token
        if u.fragment:
            for seg in re.split(r"[/?#&=\s]", u.fragment):
                if seg:
                    yield seg, 150

        # Path segments: medium signal
        segs = [s for s in (u.path or "").split("/") if s]

        # Domain-aware scoring adjustments
        is_dexscreener = "dexscreener.com" in host
        is_pumpfun = "pump.fun" in host
        is_birdeye = "birdeye.so" in host

        base_path_score = 140 if not is_dexscreener else 70  # dexscreener path is often PAIR addr

        for i, seg in enumerate(segs):
            low = seg.lower()

            # pump.fun: /coin/<mint> or /token/<mint>
            if is_pumpfun and low in ("coin", "token"):
                # next segment is very likely the mint; boost by yielding marker too
                yield seg, 10

            # birdeye: /token/<mint>
            if is_birdeye and low == "token":
                yield seg, 10

            # Always yield segment itself with a score
            yield seg, base_path_score + min(i, 10) * 2

            # If we saw "coin/token", the next segment is typically the mint; add a boosted yield
            if low in ("coin", "token") and i + 1 < len(segs):
                nxt = segs[i + 1]
                boost = 360 if (is_pumpfun or is_birdeye) else 240
                yield nxt, boost
    else:
        # URL-ish split
        for seg in re.split(r"[/?#&=\s]", t):
            if seg:
                yield seg, 90


def _yield_jsonish_tokens(text: str) -> Iterator[tuple[str, int]]:
    """
    Light JSON-ish extraction for logs containing "mint":"..." or inputMint: ...
    """
    t = (text or "").strip()
    if not t:
        return
    for m in _JSON_TOKEN_RE.finditer(t):
        k = (m.group("k") or "").strip().strip('"')
        v = (m.group("v") or "").strip().strip('"')
        if not v:
            continue
        yield v, _key_score(k)


def _yield_labelled_tokens(text: str) -> Iterator[tuple[str, int]]:
    """
    Extract candidates from patterns like "CA: <pubkey>".
    """
    t = (text or "").strip()
    if not t:
        return
    for m in _LABELLED_RE.finditer(t):
        v = (m.group(1) or "").strip()
        if v:
            yield v, 280


def extract_pubkeys(blob: Any) -> list[str]:
    """
    Extract ALL strict 32-byte base58 pubkeys from a blob/string.
    Deduped, stable order:
      1) High-signal URL parsing
      2) JSON-ish extraction
      3) Labelled extraction (CA:/mint:)
      4) Broad scan
    """
    t = _as_text(blob).strip()
    if not t:
        return []

    seen: set[str] = set()
    out: list[str] = []

    def _push(raw: str) -> None:
        c = _clean_candidate(raw)
        if _is_strict_pubkey(c) and c not in seen:
            seen.add(c)
            out.append(c)

    for tok, _score in _yield_url_tokens(t):
        _push(tok)

    for tok, _score in _yield_jsonish_tokens(t):
        _push(tok)

    for tok, _score in _yield_labelled_tokens(t):
        _push(tok)

    for cand in _B58_CHUNK_RE.findall(t):
        _push(cand)
        # If the blob chunk is very long, the tail might contain a pubkey
        if len(cand) > _MAX_B58_LEN:
            _push(cand[-_MAX_B58_LEN:])

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

    def _add(raw: str, score: int) -> None:
        nonlocal order
        c = _clean_candidate(raw)
        if _is_mint_pubkey(c):  # normalize_mint should avoid program/sysvar ids
            scored.append((c, int(score), order))
            order += 1

    for tok, score in _yield_url_tokens(t):
        _add(tok, score)

    for tok, score in _yield_jsonish_tokens(t):
        _add(tok, score)

    for tok, score in _yield_labelled_tokens(t):
        _add(tok, score)

    for cand in _B58_CHUNK_RE.findall(t):
        _add(cand, 30)
        if len(cand) > _MAX_B58_LEN:
            _add(cand[-_MAX_B58_LEN:], 25)

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
      1) Prefer higher-scored candidates (mint/inputMint/outputMint beats pool/pair/program/wallet)
      2) Prefer non-SOL if both exist (feeds often include SOL alongside the target)
      3) Prefer non-stables if both exist (memecoin scanning: base mint matters)
      4) Tie-break by latest appearance
    """
    if not scored:
        return None

    ordered = list(scored)

    # Prefer non-SOL
    non_sol = [t for t in ordered if t[0] != SOL_MINT]
    pool1 = non_sol if non_sol else ordered

    # Prefer non-stable if we have at least one non-stable
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

    # 2) exact mint pubkey?
    c0 = _clean_candidate(s)
    if _is_mint_pubkey(c0):
        return c0

    # 3) scored extraction from url/json/blob
    scored = _extract_scored_candidates(s)
    best = _pick_best_candidate(scored)
    if best and _is_mint_pubkey(best):
        return best

    # 4) fallback: extract all strict pubkeys and pick deterministically among mint-like ones
    cands = extract_pubkeys(s)
    mint_cands = [c for c in cands if _is_mint_pubkey(c)]
    if mint_cands:
        scored2 = [(c, 10, i) for i, c in enumerate(mint_cands)]
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

    # Mapping input: try signal keys first (deterministic order)
    if isinstance(x, Mapping):
        for k in _DICT_SIGNAL_KEYS:
            if k in x and x.get(k) is not None:
                try:
                    return normalize_mint(x.get(k))
                except Exception:
                    pass

        base, quote = normalize_mint_pair(x)
        if base:
            return base
        if quote:
            return quote

        last_err: Exception | None = None
        for v in x.values():  # insertion order in py3.7+
            if v is None:
                continue
            try:
                return normalize_mint(v)
            except Exception as e:
                last_err = e
        raise ValueError(f"cannot normalize mint from mapping input: {last_err!r}")

    # Sequence input: scan items deterministically
    if isinstance(x, (list, tuple)):
        last_err_seq: Exception | None = None
        for it in x:
            try:
                return normalize_mint(it)
            except Exception as e:
                last_err_seq = e
        raise ValueError(f"cannot normalize mint from sequence input: {last_err_seq!r}")

    # Set input: enforce deterministic order (sets are unordered)
    if isinstance(x, set):
        last_err_set: Exception | None = None
        items = sorted((_as_text(v) for v in x), key=lambda z: z)
        for it in items:
            try:
                return normalize_mint(it)
            except Exception as e:
                last_err_set = e
        raise ValueError(f"cannot normalize mint from set input: {last_err_set!r}")

    return _normalize_text(_as_text(x))


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
        if isinstance(obj, Mapping):
            base = try_normalize_mint(
                obj.get("baseMint") or obj.get("base_mint") or obj.get("base")
            )
            quote = try_normalize_mint(
                obj.get("quoteMint") or obj.get("quote_mint") or obj.get("quote")
            )
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
