from __future__ import annotations

"""
utils_pkg/mints.py — Mint normalization + strict pubkey extraction (P0 LOCK-IN)
==============================================================================

Single source of truth for turning “whatever the feed hands us” into a canonical
Solana token mint pubkey string (base58, STRICT 32-byte decode).

LOCK-IN requirements
--------------------
✅ ZERO network at import time
✅ STRICT validation: only accept strings that decode to exactly 32 bytes base58
✅ Robust extraction from:
   - symbols (SOL/USDC/USDT/BONK…)
   - raw pubkeys
   - pump.fun / birdeye / raydium / random URLs (query/path)
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

import contextlib
import re
from collections.abc import Iterable, Iterator, Mapping, Sequence
from functools import lru_cache
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

__version__ = "3.0.1"


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

_MIN_B58_LEN = 32
_MAX_B58_LEN = 60

_B58_CHUNK_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{20,90}")

_JSON_TOKEN_RE = re.compile(
    r'(?P<k>"?[A-Za-z_][A-Za-z0-9_]*"?)\s*:\s*(?P<v>"[^"]{16,200}"|[1-9A-HJ-NP-Za-km-z]{20,90})'
)

_LABELLED_RE = re.compile(
    r"(?i)\b(?:ca|mint|token|address|contract)\b\s*[:=]\s*([1-9A-HJ-NP-Za-km-z]{20,90})"
)

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
        "ca",
    }
)

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

# High-signal mapping keys only (avoid pair/pool by default).
_DICT_SIGNAL_KEYS: tuple[str, ...] = (
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
    "base_mint",
    "quote_mint",
    "ca",
)

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

_NON_MINT_PUBKEYS = frozenset(
    {
        "11111111111111111111111111111111",
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
        "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
        "ComputeBudget111111111111111111111111111111",
        "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr",
        "SysvarRent111111111111111111111111111111111",
        "SysvarC1ock11111111111111111111111111111111",
    }
)

# Safety threshold: below this, candidates are too ambiguous to accept as mint.
_MIN_ACCEPT_SCORE = 150


# ──────────────────────────────────────────────────────────────────────────────
# Small helpers
# ──────────────────────────────────────────────────────────────────────────────
def _as_text(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    if isinstance(x, (bytes, bytearray, memoryview)):
        with contextlib.suppress(Exception):
            return bytes(x).decode("utf-8", errors="ignore")
        return ""
    with contextlib.suppress(Exception):
        return str(x)
    return ""


def _apply_fixups(s: str) -> str:
    ss = (s or "").strip()
    return _BAD_MINT_FIXUPS.get(ss, ss)


def _strip_punct(s: str) -> str:
    return (s or "").strip(" \t\r\n'\"<>[](){}.,;:")


@lru_cache(maxsize=200_000)
def _b58decode(s: str) -> bytes:
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
    ss = (s or "").strip()
    if not ss:
        return False
    if len(ss) < _MIN_B58_LEN or len(ss) > _MAX_B58_LEN:
        return False
    for ch in ss:
        if ch not in _B58SET:
            return False
    with contextlib.suppress(Exception):
        return len(_b58decode(ss)) == 32
    return False


def _is_mint_pubkey(s: str) -> bool:
    return _is_strict_pubkey(s) and s not in _NON_MINT_PUBKEYS


def is_base58_pubkey(x: Any) -> bool:
    return _is_strict_pubkey(_apply_fixups(_strip_punct(_as_text(x))))


def _normalize_symbol(sym: str) -> str | None:
    s = (sym or "").strip().upper()
    if not s:
        return None
    s = s.replace("/", " ").replace("-", " ").replace("_", " ")
    s = " ".join(s.split()).strip()
    if s in _SYM_TO_MINT:
        return _SYM_TO_MINT[s]
    return _SYM_TO_MINT.get(s.replace(" ", ""))


def _clean_candidate(raw: str) -> str:
    s = _strip_punct(unquote((raw or "").strip()))
    if not s:
        return s

    prefix: list[str] = []
    for ch in s:
        if ch in _B58SET:
            prefix.append(ch)
        else:
            break
    if len(prefix) >= 20:
        s = "".join(prefix)

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
    if "mint" in kk or "token" in kk or "address" in kk:
        return 220
    return 160


def _yield_url_tokens(text: str) -> Iterator[tuple[str, int, str]]:
    t = (text or "").strip()
    if not t:
        return

    u = urlparse(t)
    host = (u.netloc or "").lower()

    is_dexscreener = "dexscreener.com" in host
    is_pumpfun = "pump.fun" in host
    is_birdeye = "birdeye.so" in host

    if u.scheme and u.netloc:
        qs = parse_qs(u.query or "", keep_blank_values=False)
        for k, vals in qs.items():
            if not vals:
                continue
            sc = _key_score(k)
            for v in vals:
                if v:
                    yield v, sc, "url_qs"

        if u.fragment:
            for seg in re.split(r"[/?#&=\s]", u.fragment):
                if seg:
                    yield seg, 150, "url_frag"

        segs = [s for s in (u.path or "").split("/") if s]

        for i, seg in enumerate(segs):
            low = seg.lower()

            if is_pumpfun and low in ("coin", "token") and i + 1 < len(segs):
                yield segs[i + 1], 360, "url_pumpfun_path"
                continue

            if is_birdeye and low == "token" and i + 1 < len(segs):
                yield segs[i + 1], 320, "url_birdeye_path"
                continue

            if low == "token" and i + 1 < len(segs):
                yield segs[i + 1], 240, "url_token_path"
                continue

            if is_dexscreener:
                yield seg, 60, "url_dexscreener_path"
            else:
                yield seg, 140 + min(i, 10) * 2, "url_path"
    else:
        for seg in re.split(r"[/?#&=\s]", t):
            if seg:
                yield seg, 90, "urlish_split"


def _yield_jsonish_tokens(text: str) -> Iterator[tuple[str, int, str]]:
    t = (text or "").strip()
    if not t:
        return
    for m in _JSON_TOKEN_RE.finditer(t):
        k = (m.group("k") or "").strip().strip('"')
        v = (m.group("v") or "").strip().strip('"')
        if v:
            yield v, _key_score(k), "jsonish"


def _yield_labelled_tokens(text: str) -> Iterator[tuple[str, int, str]]:
    t = (text or "").strip()
    if not t:
        return
    for m in _LABELLED_RE.finditer(t):
        v = (m.group(1) or "").strip()
        if v:
            yield v, 280, "labelled"


def extract_pubkeys(blob: Any) -> list[str]:
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

    for tok, _sc, _src in _yield_url_tokens(t):
        _push(tok)

    for tok, _sc, _src in _yield_jsonish_tokens(t):
        _push(tok)

    for tok, _sc, _src in _yield_labelled_tokens(t):
        _push(tok)

    for cand in _B58_CHUNK_RE.findall(t):
        _push(cand)
        if len(cand) > _MAX_B58_LEN:
            _push(cand[-_MAX_B58_LEN:])

    return out


def _extract_scored_candidates(text: str) -> list[tuple[str, int, int, str]]:
    t = (text or "").strip()
    if not t:
        return []

    scored: list[tuple[str, int, int, str]] = []
    order = 0

    def _add(raw: str, score: int, src: str) -> None:
        nonlocal order
        c = _clean_candidate(raw)
        if _is_mint_pubkey(c):
            scored.append((c, int(score), order, src))
            order += 1

    for tok, score, src in _yield_url_tokens(t):
        _add(tok, score, src)

    for tok, score, src in _yield_jsonish_tokens(t):
        _add(tok, score, src)

    for tok, score, src in _yield_labelled_tokens(t):
        _add(tok, score, src)

    for cand in _B58_CHUNK_RE.findall(t):
        _add(cand, 30, "broad")
        if len(cand) > _MAX_B58_LEN:
            _add(cand[-_MAX_B58_LEN:], 25, "broad_tail")

    best: dict[str, tuple[int, int, str]] = {}
    for pk, sc, idx, src in scored:
        prev = best.get(pk)
        if prev is None:
            best[pk] = (sc, idx, src)
        else:
            psc, pidx, _psrc = prev
            if sc > psc or (sc == psc and idx > pidx):
                best[pk] = (sc, idx, src)

    out = [(pk, sc, idx, src) for pk, (sc, idx, src) in best.items()]
    out.sort(key=lambda x: (x[1], x[2]))
    return out


def _pick_best_candidate(scored: Sequence[tuple[str, int, int, str]]) -> tuple[str, int] | None:
    if not scored:
        return None

    ordered = list(scored)

    non_sol = [t for t in ordered if t[0] != SOL_MINT]
    pool1 = non_sol if non_sol else ordered

    non_stable = [t for t in pool1 if t[0] not in STABLE_MINTS]
    pool2 = non_stable if non_stable else pool1

    best = pool2[-1]
    return best[0], int(best[1])


@lru_cache(maxsize=250_000)
def _normalize_text(text: str) -> str:
    s = (text or "").strip()
    if not s:
        raise ValueError("empty mint")

    s = _apply_fixups(s)

    aliased = _normalize_symbol(s)
    if aliased:
        return aliased

    c0 = _clean_candidate(s)
    if _is_mint_pubkey(c0):
        return c0

    scored = _extract_scored_candidates(s)
    picked = _pick_best_candidate(scored)
    if picked is not None:
        best, best_score = picked
        if best_score >= _MIN_ACCEPT_SCORE:
            return best

    cands = extract_pubkeys(s)
    mint_cands = [c for c in cands if _is_mint_pubkey(c)]
    if len(mint_cands) == 1:
        return mint_cands[0]

    raise ValueError(f"cannot normalize mint from input: {s!r}")


def normalize_mint(x: Any) -> str:
    if x is None:
        raise ValueError("empty mint")

    if isinstance(x, Mapping):
        for k in _DICT_SIGNAL_KEYS:
            if k in x and x.get(k) is not None:
                with contextlib.suppress(Exception):
                    return normalize_mint(x.get(k))

        lower_map: dict[str, Any] = {}
        with contextlib.suppress(Exception):
            lower_map = {str(k).lower(): v for k, v in x.items()}

        for k in _DICT_SIGNAL_KEYS:
            v = lower_map.get(k.lower())
            if v is not None:
                with contextlib.suppress(Exception):
                    return normalize_mint(v)

        with contextlib.suppress(Exception):
            base_token = x.get("baseToken") or x.get("base_token")
            quote_token = x.get("quoteToken") or x.get("quote_token")
            if isinstance(base_token, Mapping):
                v = base_token.get("address") or base_token.get("mint")
                if v is not None:
                    return normalize_mint(v)
            if isinstance(quote_token, Mapping):
                v = quote_token.get("address") or quote_token.get("mint")
                if v is not None:
                    return normalize_mint(v)

        base, quote = normalize_mint_pair(x)
        if base:
            return base
        if quote:
            return quote

        last_err: Exception | None = None
        for v in x.values():
            if v is None:
                continue
            try:
                return normalize_mint(v)
            except Exception as e:
                last_err = e
        raise ValueError(f"cannot normalize mint from mapping input: {last_err!r}")

    if isinstance(x, (list, tuple)):
        last_err_seq: Exception | None = None
        for it in x:
            try:
                return normalize_mint(it)
            except Exception as e:
                last_err_seq = e
        raise ValueError(f"cannot normalize mint from sequence input: {last_err_seq!r}")

    if isinstance(x, Iterable) and not isinstance(x, (str, bytes, bytearray, memoryview, Mapping)):
        last_err_it: Exception | None = None
        for it in x:
            try:
                return normalize_mint(it)
            except Exception as e:
                last_err_it = e
        raise ValueError(f"cannot normalize mint from iterable input: {last_err_it!r}")

    return _normalize_text(_as_text(x))


def try_normalize_mint(x: Any) -> str | None:
    try:
        return normalize_mint(x)
    except Exception:
        return None


def normalize_mint_pair(obj: Any) -> tuple[str | None, str | None]:
    with contextlib.suppress(Exception):
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

            bt = obj.get("baseToken") or obj.get("base_token")
            qt = obj.get("quoteToken") or obj.get("quote_token")
            if base is None and isinstance(bt, Mapping):
                base = try_normalize_mint(bt.get("address") or bt.get("mint"))
            if quote is None and isinstance(qt, Mapping):
                quote = try_normalize_mint(qt.get("address") or qt.get("mint"))

            return base, quote
    return None, None


def is_known_mint(mint: str) -> bool:
    return (mint or "").strip() in KNOWN_MINTS


def is_stable_mint(mint: str) -> bool:
    return (mint or "").strip() in STABLE_MINTS


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
