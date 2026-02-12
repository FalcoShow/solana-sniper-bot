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

__version__ = "3.1.0"


# ──────────────────────────────────────────────────────────────────────────────
# Canonical mints (Solana mainnet)
# ──────────────────────────────────────────────────────────────────────────────
SOL_MINT = "So11111111111111111111111111111111111111112"  # wSOL pseudo-mint
WSOL_MINT = SOL_MINT  # compat alias

USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
BONK_MINT = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"

KNOWN_MINTS = frozenset({SOL_MINT, USDC_MINT, USDT_MINT, BONK_MINT})
STABLE_MINTS = frozenset({USDC_MINT, USDT_MINT})


# Known bad/typo mints seen in the wild (auto-fix to canonical)
_BAD_MINT_FIXUPS: dict[str, str] = {
    # example typo -> canonical
    "EPjFWdd5AufqSSqeMzQ1NzxypaC8G4weEGKzwyTDt1v": USDC_MINT,
}


# Common symbol aliases (case-insensitive; we normalize symbols heavily)
_SYM_TO_MINT: dict[str, str] = {
    "SOL": SOL_MINT,
    "WSOL": SOL_MINT,
    "WRAPPED SOL": SOL_MINT,
    "WRAPPEDSOL": SOL_MINT,
    "W SOL": SOL_MINT,
    "W-SOL": SOL_MINT,
    "W_SOL": SOL_MINT,
    "USDC": USDC_MINT,
    "USD COIN": USDC_MINT,
    "USDCOIN": USDC_MINT,
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

# Generic “base58-ish chunk” finder (we validate strictly later)
_B58_CHUNK_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{20,90}")

# JSON-ish key:value token extractor (cheap heuristic; still strict-validated later)
_JSON_TOKEN_RE = re.compile(
    r'(?P<k>"?[A-Za-z_][A-Za-z0-9_]*"?)\s*:\s*(?P<v>"[^"]{16,200}"|[1-9A-HJ-NP-Za-km-z]{20,90})'
)

# Labelled text patterns: "mint: <addr>", "ca=<addr>", etc.
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
    # common alt keys:
    "coinMint",
    "coin_mint",
)

# Known non-mint pubkeys/programs/sysvars we must not accept as “mint”
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
_MIN_ACCEPT_SCORE = 160


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
    # strip only common wrappers; do NOT remove base58 characters
    return (s or "").strip(" \t\r\n'\"<>[](){}.,;:")


@lru_cache(maxsize=250_000)
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
    """
    Strict “32-byte base58 pubkey” validator (does NOT exclude programs/sysvars).
    """
    s = _apply_fixups(_strip_punct(_as_text(x)))
    return _is_strict_pubkey(s)


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
    """
    Clean a “maybe pubkey” token without risky heuristics.

    - strips wrappers/quotes
    - unquotes percent-encoding
    - if token contains non-base58 chars, keeps only the leading base58 run
    """
    s = _strip_punct(unquote((raw or "").strip()))
    if not s:
        return ""

    # If there are non-base58 chars, take the leading base58 run (common in logs)
    prefix: list[str] = []
    for ch in s:
        if ch in _B58SET:
            prefix.append(ch)
        else:
            break
    if len(prefix) >= 20:
        s = "".join(prefix)

    # Trim absurdly long tokens (still strict validated later)
    if len(s) > 90:
        s = s[:90]

    return _apply_fixups(s)


def _key_score(key: str) -> int:
    kk = (key or "").strip().lower()
    if kk in _HIGH_SIGNAL_KEYS:
        return 450
    if kk in _LOW_SIGNAL_KEYS:
        return 90
    if "mint" in kk or "token" in kk or "address" in kk or kk in {"ca"}:
        return 240
    return 160


def _yield_url_tokens(text: str) -> Iterator[tuple[str, int, str]]:
    t = (text or "").strip()
    if not t:
        return

    u = urlparse(t)
    host = (u.netloc or "").lower()
    path = (u.path or "").lower()

    is_pumpfun = "pump.fun" in host
    is_birdeye = "birdeye.so" in host
    is_dexscreener_api = "api.dexscreener.com" in host and "/tokens/" in path

    if u.scheme and u.netloc:
        # Query string values
        qs = parse_qs(u.query or "", keep_blank_values=False)
        for k, vals in qs.items():
            if not vals:
                continue
            sc = _key_score(k)
            for v in vals:
                if v:
                    yield v, sc, "url_qs"

        # Special: dexscreener API tokens endpoint: /latest/dex/tokens/{mint}
        if is_dexscreener_api:
            segs = [s for s in (u.path or "").split("/") if s]
            for i, seg in enumerate(segs):
                if seg.lower() == "tokens" and i + 1 < len(segs):
                    yield segs[i + 1], 420, "url_dexscreener_tokens"
                    break

        # Fragment values
        if u.fragment:
            for seg in re.split(r"[/?#&=\s]", u.fragment):
                if seg:
                    yield seg, 160, "url_frag"

        # Path segments
        segs = [s for s in (u.path or "").split("/") if s]
        for i, seg in enumerate(segs):
            low = seg.lower()

            # pump.fun: /coin/<mint>
            if is_pumpfun and low in {"coin", "token"} and i + 1 < len(segs):
                yield segs[i + 1], 380, "url_pumpfun_path"
                continue

            # birdeye: /token/<mint>
            if is_birdeye and low == "token" and i + 1 < len(segs):
                yield segs[i + 1], 340, "url_birdeye_path"
                continue

            # generic /token/<mint>
            if low == "token" and i + 1 < len(segs):
                yield segs[i + 1], 250, "url_token_path"
                continue

            # Otherwise low-ish signal
            yield seg, 140 + min(i, 12) * 2, "url_path"
    else:
        # “url-ish” string without scheme
        for seg in re.split(r"[/?#&=\s]", t):
            if seg:
                yield seg, 100, "urlish_split"


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
            yield v, 300, "labelled"


def _iter_text_blobs(blob: Any, *, _depth: int = 0, _seen: set[int] | None = None) -> Iterator[str]:
    """
    Deterministic traversal of nested inputs for extract_pubkeys().
    """
    if _seen is None:
        _seen = set()
    if _depth > 6:
        return

    oid = id(blob)
    if oid in _seen:
        return
    _seen.add(oid)

    if blob is None:
        return

    if isinstance(blob, (str, bytes, bytearray, memoryview)):
        s = _as_text(blob).strip()
        if s:
            yield s
        return

    if isinstance(blob, Mapping):
        # stable order: sort keys by string
        for k in sorted((str(x) for x in blob.keys())):
            with contextlib.suppress(Exception):
                v = blob.get(k)  # type: ignore[arg-type]
                yield from _iter_text_blobs(v, _depth=_depth + 1, _seen=_seen)
        return

    if isinstance(blob, (list, tuple)):
        for it in blob:
            yield from _iter_text_blobs(it, _depth=_depth + 1, _seen=_seen)
        return

    if isinstance(blob, Iterable):
        # avoid iterating huge/unsafe iterables
        with contextlib.suppress(Exception):
            for i, it in enumerate(blob):
                if i >= 50:
                    break
                yield from _iter_text_blobs(it, _depth=_depth + 1, _seen=_seen)
        return

    s2 = _as_text(blob).strip()
    if s2:
        yield s2


def extract_pubkeys(blob: Any) -> list[str]:
    """
    Find ALL strict 32-byte base58 pubkeys in the given blob (string/URL/log/JSON-ish),
    stable order, deduped.
    """
    seen: set[str] = set()
    out: list[str] = []

    def _push(raw: str) -> None:
        c = _clean_candidate(raw)
        if _is_strict_pubkey(c) and c not in seen:
            seen.add(c)
            out.append(c)

    for text in _iter_text_blobs(blob):
        for tok, _sc, _src in _yield_url_tokens(text):
            _push(tok)
        for tok, _sc, _src in _yield_jsonish_tokens(text):
            _push(tok)
        for tok, _sc, _src in _yield_labelled_tokens(text):
            _push(tok)
        for cand in _B58_CHUNK_RE.findall(text):
            _push(cand)

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

    # broad scan is intentionally low-signal
    for cand in _B58_CHUNK_RE.findall(t):
        _add(cand, 40, "broad")

    # Keep best score per pubkey
    best: dict[str, tuple[int, int, str]] = {}
    for pk, sc, idx, src in scored:
        prev = best.get(pk)
        if prev is None:
            best[pk] = (sc, idx, src)
        else:
            psc, pidx, _ = prev
            if sc > psc or (sc == psc and idx > pidx):
                best[pk] = (sc, idx, src)

    out = [(pk, sc, idx, src) for pk, (sc, idx, src) in best.items()]
    out.sort(key=lambda x: (x[1], x[2]))  # ascending; best is last
    return out


def _pick_best_candidate(scored: Sequence[tuple[str, int, int, str]]) -> tuple[str, int] | None:
    if not scored:
        return None

    ordered = list(scored)

    # Prefer non-SOL over SOL if both present (feeds often include SOL alongside target mint)
    non_sol = [t for t in ordered if t[0] != SOL_MINT]
    pool1 = non_sol if non_sol else ordered

    # Prefer non-stable over stable if both present (avoid accidentally choosing USDC/USDT)
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

    # last-resort: if exactly one mint-like pubkey exists in the blob, accept it
    cands = extract_pubkeys(s)
    mint_cands = [c for c in cands if _is_mint_pubkey(c)]
    if len(mint_cands) == 1:
        return mint_cands[0]

    raise ValueError(f"cannot normalize mint from input: {s!r}")


def normalize_mint(x: Any) -> str:
    """
    Normalize arbitrary input into a canonical mint pubkey string.

    Raises ValueError if unable to determine a mint deterministically.
    """
    if x is None:
        raise ValueError("empty mint")

    # Mapping: check signal keys first, then recurse values deterministically
    if isinstance(x, Mapping):
        # direct keys (as-is)
        for k in _DICT_SIGNAL_KEYS:
            if k in x and x.get(k) is not None:
                with contextlib.suppress(Exception):
                    return normalize_mint(x.get(k))

        # case-insensitive scan of keys
        lower_map: dict[str, Any] = {}
        with contextlib.suppress(Exception):
            lower_map = {str(k).lower(): v for k, v in x.items()}

        for k in _DICT_SIGNAL_KEYS:
            v = lower_map.get(k.lower())
            if v is not None:
                with contextlib.suppress(Exception):
                    return normalize_mint(v)

        # nested common shapes: baseToken/quoteToken
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

        # deterministic recurse over values in stable key order
        last_err: Exception | None = None
        for k in sorted((str(kk) for kk in x.keys())):
            with contextlib.suppress(Exception):
                v = x.get(k)  # type: ignore[arg-type]
                if v is None:
                    continue
                try:
                    return normalize_mint(v)
                except Exception as e:  # noqa: BLE001
                    last_err = e
        raise ValueError(f"cannot normalize mint from mapping input: {last_err!r}")

    # Sequences: try each element
    if isinstance(x, (list, tuple)):
        last_err_seq: Exception | None = None
        for it in x:
            try:
                return normalize_mint(it)
            except Exception as e:  # noqa: BLE001
                last_err_seq = e
        raise ValueError(f"cannot normalize mint from sequence input: {last_err_seq!r}")

    # Generic iterables (but not strings/bytes)
    if isinstance(x, Iterable) and not isinstance(x, (str, bytes, bytearray, memoryview, Mapping)):
        last_err_it: Exception | None = None
        for it in x:
            try:
                return normalize_mint(it)
            except Exception as e:  # noqa: BLE001
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
    "WSOL_MINT",
    "extract_pubkeys",
    "is_base58_pubkey",
    "is_known_mint",
    "is_stable_mint",
    "normalize_mint",
    "normalize_mint_pair",
    "try_normalize_mint",
]
