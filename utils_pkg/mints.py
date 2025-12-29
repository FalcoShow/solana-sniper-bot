# utils_pkg/mints.py
from __future__ import annotations

"""
utils_pkg/mints.py — V3+++ God-Tier Mint Normalizer (LOCK-IN)
=============================================================

Single source of truth for “whatever the feed hands us” -> canonical Solana mint.

LOCK-IN requirements
--------------------
✅ ZERO network at import time
✅ STRICT validation: only accept strings that decode to exactly 32 bytes base58
✅ Robust extraction from:
   - symbols (SOL/USDC/USDT/BONK…)
   - raw pubkeys
   - pump.fun / dexscreener / birdeye / raydium / random URLs
   - JSON/log blobs containing multiple pubkeys (deterministic best pick)
✅ Prevents garbage mints going into Jupiter (fewer 400s, safer routing)

Public API
----------
- normalize_mint(x) -> str            (raises ValueError if cannot normalize)
- try_normalize_mint(x) -> str|None   (never raises)
- is_base58_pubkey(x) -> bool         (strict 32-byte base58)
- extract_pubkeys(blob) -> list[str]  (all strict pubkeys found, deduped, stable)
"""

import re
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, unquote, urlparse

# ──────────────────────────────────────────────────────────────────────────────
# Canonical mints (Solana mainnet)
# ──────────────────────────────────────────────────────────────────────────────
SOL_MINT = "So11111111111111111111111111111111111111112"  # wSOL pseudo-mint
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"  # canonical USDC
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"  # canonical USDT
BONK_MINT = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"  # canonical BONK

# Known bad/typo mints seen in the wild (auto-fix to canonical)
_BAD_MINT_FIXUPS: Dict[str, str] = {
    # Example typo seen in logs/feeds
    "EPjFWdd5AufqSSqeMzQ1NzxypaC8G4weEGKzwyTDt1v": USDC_MINT,
}

# Common symbol aliases (case-insensitive)
_SYM_TO_MINT: Dict[str, str] = {
    "SOL": SOL_MINT,
    "WSOL": SOL_MINT,
    "W-SOL": SOL_MINT,
    "W_SOL": SOL_MINT,
    SOL_MINT.upper(): SOL_MINT,
    "USDC": USDC_MINT,
    "USDC.SOL": USDC_MINT,
    "USDC_CCTP": USDC_MINT,
    "USDC.CCTP": USDC_MINT,
    "USDCOIN": USDC_MINT,
    "USD COIN": USDC_MINT,
    "USD-COIN": USDC_MINT,
    "USDT": USDT_MINT,
    "TETHER": USDT_MINT,
    "BONK": BONK_MINT,
}

# “high-signal” keys when parsing URL query / JSON-ish blobs
_HIGH_SIGNAL_KEYS = (
    "mint",
    "token",
    "address",
    "inputmint",
    "outputmint",
    "basemint",
    "quotemint",
    "tokenaddress",
    "mintaddress",
)

# Also treat these dict keys as high-signal in python dict inputs
_DICT_SIGNAL_KEYS = (
    "mint",
    "address",
    "token",
    "tokenAddress",
    "token_address",
    "inputMint",
    "outputMint",
    "baseMint",
    "quoteMint",
    "pairAddress",
    "poolAddress",
)

# ──────────────────────────────────────────────────────────────────────────────
# Base58 strict pubkey validation (no deps)
# ──────────────────────────────────────────────────────────────────────────────
_B58_ALPH = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_B58SET = set(_B58_ALPH)
_B58IDX = {c: i for i, c in enumerate(_B58_ALPH)}

# Typical Solana pubkey string length range is ~32..44. We allow a little wider
# but STRICTLY require decode length == 32 bytes.
_MIN_B58_LEN = 32
_MAX_B58_LEN = 50

# Broad “base58-looking” finder for blobs (URLs, logs, JSON, etc.)
_B58_CHUNK_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{20,90}")

# JSON-ish “key: value” finder (quick heuristic without json.loads)
_JSON_TOKEN_RE = re.compile(
    r'(?P<k>"?[A-Za-z_][A-Za-z0-9_]*"?)\s*:\s*(?P<v>"[^"]{16,160}"|[1-9A-HJ-NP-Za-km-z]{20,90})'
)

# Words/suffixes some feeds tack on to otherwise-base58-ish strings.
_SUFFIX_WORDS = (
    "pumpfun",
    "pump",
    "raydium",
    "rayd",
    "dexscreener",
    "dex",
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
    """Best-effort coercion to string WITHOUT importing solana deps."""
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    if isinstance(x, (bytes, bytearray)):
        try:
            return x.decode("utf-8", errors="ignore")
        except Exception:
            return str(x)
    try:
        return str(x)
    except Exception:
        return ""


@lru_cache(maxsize=120_000)
def _b58decode(s: str) -> bytes:
    """Minimal base58 decode. Used only for strict 32-byte check (cached)."""
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


@lru_cache(maxsize=240_000)
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
    return _is_base58_32((_as_text(x) or "").strip())


def _apply_fixups(s: str) -> str:
    ss = (s or "").strip()
    return _BAD_MINT_FIXUPS.get(ss, ss)


def _strip_punct(s: str) -> str:
    return (s or "").strip(" \t\r\n'\"<>[](){}.,;:")


def _clean_candidate(cand: str) -> str:
    """
    Best-effort cleanup:
      - strip whitespace and URL escaping
      - remove obvious trailing punctuation
      - keep base58 prefix if token includes delimiters
      - strip suffix words (pump/raydium/dex...) if it stays base58-only
      - apply known bad->good fixups
    """
    s = _strip_punct(unquote((cand or "").strip()))
    if not s:
        return s

    # If token has delimiters, keep base58 prefix portion.
    prefix_chars: List[str] = []
    for ch in s:
        if ch in _B58SET:
            prefix_chars.append(ch)
        else:
            break
    if prefix_chars:
        pref = "".join(prefix_chars)
        if len(pref) >= 20:
            s = pref

    # Strip suffix words only if the remaining is still base58 characters.
    low = s.lower()
    changed = True
    while changed:
        changed = False
        for suf in _SUFFIX_WORDS:
            if low.endswith(suf) and len(s) > len(suf) + _MIN_B58_LEN:
                s2 = s[: -len(suf)]
                ok = True
                for ch in s2:
                    if ch not in _B58SET:
                        ok = False
                        break
                if ok:
                    s = s2
                    low = s.lower()
                    changed = True
                break

    if len(s) > _MAX_B58_LEN:
        s = s[:_MAX_B58_LEN]

    # Fixups (exact) + also fixup if cleaned equals a known bad mint
    s = _apply_fixups(s)
    return s


def _yield_url_tokens(text: str) -> Iterable[Tuple[str, int]]:
    """
    Parse URLs and yield likely tokens with a PRIORITY score.
    Higher score = higher confidence candidate.
    """
    t = (text or "").strip()
    if not t:
        return

    u = urlparse(t)
    if u.scheme and u.netloc:
        # Query params (highest signal)
        qs = parse_qs(u.query or "", keep_blank_values=False)
        for k, vals in qs.items():
            kk = (k or "").strip().lower()
            if not vals:
                continue
            base_score = 300 if kk in _HIGH_SIGNAL_KEYS else 120
            for v in vals:
                if v:
                    yield v, base_score

        # Fragment sometimes contains token
        if u.fragment:
            for seg in re.split(r"[/?#&=]", u.fragment):
                if seg:
                    yield seg, 90

        # Path segments
        segs = [s for s in (u.path or "").split("/") if s]
        for i, seg in enumerate(segs):
            yield seg, 60 + i
    else:
        # URL-ish split
        for seg in re.split(r"[/?#&=\s]", t):
            if seg:
                yield seg, 40


def _yield_jsonish_tokens(text: str) -> Iterable[Tuple[str, int]]:
    """
    Very light JSON-ish token extraction for logs that contain:
      {"mint":"..."} or "inputMint": "..." etc.
    """
    t = (text or "").strip()
    if not t:
        return
    for m in _JSON_TOKEN_RE.finditer(t):
        k = (m.group("k") or "").strip().strip('"').lower()
        v = (m.group("v") or "").strip().strip('"')
        if not v:
            continue
        score = 220 if k in _HIGH_SIGNAL_KEYS else 70
        yield v, score


def extract_pubkeys(blob: Any) -> List[str]:
    """
    Extract ALL strict 32-byte base58 pubkeys from a blob/string.
    Deduped, stable order (high-signal first, then broad scan).
    """
    t = _as_text(blob).strip()
    if not t:
        return []

    seen = set()
    out: List[str] = []

    # 1) High-signal tokens from URL parsing
    for tok, _score in _yield_url_tokens(t):
        c = _clean_candidate(tok)
        if _is_base58_32(c) and c not in seen:
            seen.add(c)
            out.append(c)

    # 2) JSON-ish extraction
    for tok, _score in _yield_jsonish_tokens(t):
        c = _clean_candidate(tok)
        if _is_base58_32(c) and c not in seen:
            seen.add(c)
            out.append(c)

    # 3) Broad scan
    for cand in _B58_CHUNK_RE.findall(t):
        c = _clean_candidate(cand)
        if _is_base58_32(c) and c not in seen:
            seen.add(c)
            out.append(c)

        if len(cand) > _MAX_B58_LEN:
            tail = _clean_candidate(cand[-_MAX_B58_LEN :])
            if _is_base58_32(tail) and tail not in seen:
                seen.add(tail)
                out.append(tail)

    return out


def _extract_scored_candidates(text: str) -> List[Tuple[str, int, int]]:
    """
    Internal: produce (pubkey, score, order_index) for best-pick selection.
    Returned list is sorted ascending by (score, order) so “best” is the last item.
    """
    t = (text or "").strip()
    if not t:
        return []

    scored: List[Tuple[str, int, int]] = []
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
            scored.append((c, 25, order))
            order += 1
        if len(cand) > _MAX_B58_LEN:
            tail = _clean_candidate(cand[-_MAX_B58_LEN :])
            if _is_base58_32(tail):
                scored.append((tail, 20, order))
                order += 1

    # Dedup while preserving best score + latest order
    best: Dict[str, Tuple[int, int]] = {}
    for pk, sc, idx in scored:
        prev = best.get(pk)
        if prev is None:
            best[pk] = (sc, idx)
        else:
            psc, pidx = prev
            if sc > psc or (sc == psc and idx > pidx):
                best[pk] = (sc, idx)

    out = [(pk, sc, idx) for pk, (sc, idx) in best.items()]
    out.sort(key=lambda x: (x[1], x[2]))
    return out


def _pick_best_candidate(scored: Sequence[Tuple[str, int, int]]) -> Optional[str]:
    """
    Deterministic selection when multiple pubkeys are present:
      - Prefer non-SOL over SOL if both exist
      - Prefer highest score; tie-break by latest position
    """
    if not scored:
        return None
    non_sol = [t for t in scored if t[0] != SOL_MINT]
    pool = non_sol if non_sol else list(scored)
    return pool[-1][0] if pool else None


def _normalize_symbol(sym: str) -> Optional[str]:
    s = (sym or "").strip().upper()
    if not s:
        return None
    # normalize a few separators
    s = s.replace("/", " ").replace("-", " ").replace("_", " ").strip()
    # also try no-space variant
    if s in _SYM_TO_MINT:
        return _SYM_TO_MINT[s]
    s2 = s.replace(" ", "")
    return _SYM_TO_MINT.get(s2)


@lru_cache(maxsize=220_000)
def _normalize_text(text: str) -> str:
    """
    Cached normalizer for TEXT inputs only.
    """
    s = (text or "").strip()
    if not s:
        raise ValueError("empty mint")

    # 0) Exact fixups
    s = _apply_fixups(s)

    # 1) Symbol alias?
    aliased = _normalize_symbol(s)
    if aliased:
        return aliased

    # 2) Exact pubkey?
    c0 = _clean_candidate(s)
    if _is_base58_32(c0):
        return c0

    # 3) Extract from URL/blob with scoring
    scored = _extract_scored_candidates(s)
    best = _pick_best_candidate(scored)
    if best and _is_base58_32(best):
        return best

    # 4) Last resort: stable list extractor
    cands = extract_pubkeys(s)
    if cands:
        non_sol = [c for c in cands if c != SOL_MINT]
        return non_sol[-1] if non_sol else cands[-1]

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
        for k in _DICT_SIGNAL_KEYS:
            if k in x and x.get(k) is not None:
                try:
                    return normalize_mint(x.get(k))
                except Exception:
                    pass
        # else scan values (deterministically by insertion order)
        for v in x.values():
            if v is None:
                continue
            try:
                return normalize_mint(v)
            except Exception:
                continue
        raise ValueError("cannot normalize mint from dict input")

    # list/tuple/set: scan items
    if isinstance(x, (list, tuple, set)):
        last_err: Optional[Exception] = None
        for it in x:
            try:
                return normalize_mint(it)
            except Exception as e:  # noqa: BLE001
                last_err = e
                continue
        raise ValueError(f"cannot normalize mint from sequence input: {last_err!r}")

    s = _as_text(x)
    return _normalize_text(s)


def try_normalize_mint(x: Any) -> Optional[str]:
    """Non-throwing version of normalize_mint()."""
    try:
        return normalize_mint(x)
    except Exception:
        return None


__all__ = [
    "normalize_mint",
    "try_normalize_mint",
    "extract_pubkeys",
    "is_base58_pubkey",
    "SOL_MINT",
    "USDT_MINT",
    "USDC_MINT",
    "BONK_MINT",
]
