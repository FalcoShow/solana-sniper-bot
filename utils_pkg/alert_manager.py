# utils_pkg/alert_manager.py
# Unified alerting with cooldowns, dedupe, and multi-channel fanout (Telegram + Discord).
# Designed to be called from sniper loops, risk_manager, scanners, etc.

from __future__ import annotations

import asyncio
import os
import hashlib
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime

# Core I/O primitives
from .core_utils import (
    send_telegram,
    send_discord_alert,   # newer helper (title, fields) or legacy variant (see _send_discord_safe)
    convert_sol_to_usd,
)

__all__ = ["AlertManager"]

logger = logging.getLogger("alert_manager")
if not logger.handlers:
    # don't clobber global logging config if the app already configured it
    logging.basicConfig(level=logging.INFO)

# ───────────────────────────────────────────────────────────────────────────────
# Env toggles (safe defaults)
# ───────────────────────────────────────────────────────────────────────────────
ENABLE_ALERTS: bool = os.getenv("ENABLE_ALERTS", "true").lower() == "true"
ENABLE_TELEGRAM_ALERTS: bool = os.getenv("ENABLE_TELEGRAM_ALERTS", "true").lower() == "true"
ENABLE_DISCORD_ALERTS: bool = os.getenv("ENABLE_DISCORD_ALERTS", "true").lower() == "true"

# Global default cooldown (seconds) if a specific type is not provided at init
GLOBAL_ALERT_COOLDOWN_SECONDS: int = int(os.getenv("ALERT_COOLDOWN_SECONDS", "900"))  # 15 minutes default

# Key prefix in Redis
COOLDOWN_PREFIX = "alert_cooldown"


def _hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", "ignore")).hexdigest()[:16]


class AlertManager:
    """
    Alert fanout with Redis-backed cooldown + dedupe.

    Parameters
    ----------
    redis : aioredis.Redis | redis.asyncio.Redis | Any
        Async Redis client.
    cooldowns : Dict[str, int]
        Per-type cooldowns (in seconds): e.g. {"buy": 300, "sell": 120, "arbitrage": 600}
        Fallback to GLOBAL_ALERT_COOLDOWN_SECONDS if a type is missing.

    Usage
    -----
    am = AlertManager(redis, {"buy": 300, "sell": 180})
    await am.send_buy_alert(...)
    await am.send_sell_alert(...)
    await am.send_arbitrage_alert(...)
    await am.queue_alert("custom text", kind="status")
    """

    def __init__(self, redis, cooldowns: Optional[Dict[str, int]] = None):
        self.redis = redis
        self.cooldowns = cooldowns or {}
        self.enabled = ENABLE_ALERTS
        self.enable_telegram = ENABLE_TELEGRAM_ALERTS
        self.enable_discord = ENABLE_DISCORD_ALERTS

    # ────────────────────────────────────────────────────────────────────────
    # Cooldown / dedupe helpers
    # ────────────────────────────────────────────────────────────────────────
    async def _allowed(self, kind: str, key: str, ttl: Optional[int] = None) -> bool:
        """
        kind : "buy" | "sell" | "arbitrage" | "status" | ...
        key  : token_mint or message hash to dedupe
        ttl  : override cooldown in seconds
        """
        if not self.redis:
            # No Redis? allow but log (not ideal; prefer having redis)
            logger.debug("AlertManager._allowed: redis missing, allowing alert")
            return True

        cooldown = int(ttl or self.cooldowns.get(kind, GLOBAL_ALERT_COOLDOWN_SECONDS))
        redis_key = f"{COOLDOWN_PREFIX}:{kind}:{key}"
        try:
            exists = await self.redis.exists(redis_key)
            if exists:
                return False
            # Set TTL so subsequent calls within window are blocked
            await self.redis.set(redis_key, "1", ex=cooldown)
            return True
        except Exception as e:
            logger.warning(f"_allowed redis error for {redis_key}: {e}")
            return True  # fail-open to avoid losing critical alerts

    async def _allowed_for_message(self, kind: str, message: str, ttl: Optional[int] = None) -> bool:
        return await self._allowed(kind, _hash(message), ttl=ttl)

    # ────────────────────────────────────────────────────────────────────────
    # Discord safe wrapper (supports both helper signatures)
    # ────────────────────────────────────────────────────────────────────────
    async def _send_discord_safe(self, title: str, fields: List[Dict[str, Any]]):
        """
        Attempts to call `send_discord_alert(title, fields)` (new),
        and falls back to older signature if the project still has it.
        """
        if not self.enable_discord:
            return
        try:
            # Preferred modern helper that accepts (title, fields)
            await send_discord_alert(title, fields)  # type: ignore[arg-type]
        except TypeError:
            # Fallback: try legacy variant, if present. We synthesize the legacy params.
            try:
                token = next((f["value"] for f in fields if f.get("name") in ("Token", "Mint", "Symbol")), "N/A")
                price = 0.0
                fraud_score = 0.0
                dex_link = next((f["value"] for f in fields if f.get("name") in ("Link", "Dex", "URL")), "")
                sol_value = None
                for f in fields:
                    if f.get("name") in ("Price", "Current Price", "Entry Price"):
                        try:
                            price = float(str(f["value"]).split()[0])
                        except Exception:
                            pass
                    if f.get("name") in ("Risk Score", "Fraud Score"):
                        try:
                            fraud_score = float(str(f["value"]).split()[0])
                        except Exception:
                            pass
                    if f.get("name") in ("Amount", "Balance", "Size (SOL)"):
                        try:
                            sol_value = float(str(f["value"]).split()[0])
                        except Exception:
                            pass

                await send_discord_alert(token, price, fraud_score, dex_link, sol_value)  # type: ignore[misc]
            except Exception as e:
                logger.warning(f"Discord fallback failed: {e}")
        except Exception as e:
            logger.warning(f"Discord send error: {e}")

    # ────────────────────────────────────────────────────────────────────────
    # Public alert APIs
    # ────────────────────────────────────────────────────────────────────────
    async def send_buy_alert(
        self,
        symbol: str,
        entry_price: float,
        fraud_score: float,
        dex_link: str,
        *,
        token_mint: Optional[str] = None,
        sol_value: Optional[float] = None,
        extra: Optional[Dict[str, Any]] = None,
        ttl: Optional[int] = None,
    ):
        if not self.enabled:
            return
        token_key = token_mint or symbol
        msg = f"✅ BUY: {symbol} @ {entry_price:.6f} — risk:{fraud_score:.2f}\n{dex_link}"
        if not await self._allowed("buy", token_key, ttl=ttl):
            return

        # Telegram
        if self.enable_telegram:
            if sol_value is not None:
                usd = await convert_sol_to_usd(sol_value)
                msg += f"\n💵 Est. Size: {sol_value:.4f} SOL (${usd:.2f})"
            await send_telegram(msg)

        # Discord
        fields = [
            {"name": "Symbol", "value": symbol, "inline": True},
            {"name": "Entry Price", "value": f"{entry_price:.6f}", "inline": True},
            {"name": "Risk Score", "value": f"{fraud_score:.2f}", "inline": True},
            {"name": "Link", "value": dex_link, "inline": False},
        ]
        if token_mint:
            fields.insert(1, {"name": "Mint", "value": token_mint, "inline": True})
        if sol_value is not None:
            usd = await convert_sol_to_usd(sol_value)
            fields.append({"name": "Size (SOL)", "value": f"{sol_value:.4f}", "inline": True})
            fields.append({"name": "Est USD", "value": f"${usd:.2f}", "inline": True})
        if extra:
            for k, v in extra.items():
                fields.append({"name": str(k), "value": str(v), "inline": True})

        await self._send_discord_safe("🟢 Token Sniped", fields)

    async def send_sell_alert(
        self,
        symbol: str,
        current_price: float,
        profit_value: float,
        dex_link: str,
        *,
        token_mint: Optional[str] = None,
        sol_value: Optional[float] = None,
        reason: Optional[str] = None,
        ttl: Optional[int] = None,
    ):
        if not self.enabled:
            return
        token_key = token_mint or symbol
        msg = f"🔴 SELL: {symbol} @ {current_price:.6f}"
        if reason:
            msg += f"\nReason: {reason}"
        msg += f"\nProfit: {profit_value:.6f} (in base units)\n{dex_link}"

        if not await self._allowed("sell", token_key, ttl=ttl):
            return

        # Telegram
        if self.enable_telegram:
            if sol_value is not None:
                usd = await convert_sol_to_usd(sol_value)
                msg += f"\n💵 Size: {sol_value:.4f} SOL (~${usd:.2f})"
            await send_telegram(msg)

        # Discord
        fields = [
            {"name": "Symbol", "value": symbol, "inline": True},
            {"name": "Current Price", "value": f"{current_price:.6f}", "inline": True},
            {"name": "Profit (base)", "value": f"{profit_value:.6f}", "inline": True},
            {"name": "Link", "value": dex_link, "inline": False},
        ]
        if token_mint:
            fields.insert(1, {"name": "Mint", "value": token_mint, "inline": True})
        if reason:
            fields.append({"name": "Reason", "value": reason, "inline": False})
        if sol_value is not None:
            usd = await convert_sol_to_usd(sol_value)
            fields.append({"name": "Size (SOL)", "value": f"{sol_value:.4f}", "inline": True})
            fields.append({"name": "Est USD", "value": f"${usd:.2f}", "inline": True})

        await self._send_discord_safe("🔴 Position Closed", fields)

    async def send_arbitrage_alert(
        self,
        title: str,
        description: str,
        *,
        token_mint: Optional[str] = None,
        ttl: Optional[int] = None,
        link: Optional[str] = None,
        est_edge_pct: Optional[float] = None,
    ):
        if not self.enabled:
            return
        # Use message hash for dedupe
        dedupe_key = _hash(f"{title}:{token_mint}:{description}")
        if not await self._allowed("arbitrage", dedupe_key, ttl=ttl):
            return

        # Telegram
        if self.enable_telegram:
            msg = f"⚡ {title}\n{description}"
            if est_edge_pct is not None:
                msg += f"\nEdge: {est_edge_pct:.2f}%"
            if link:
                msg += f"\n{link}"
            await send_telegram(msg)

        # Discord
        fields = [
            {"name": "Details", "value": description, "inline": False},
        ]
        if token_mint:
            fields.insert(0, {"name": "Mint", "value": token_mint, "inline": True})
        if est_edge_pct is not None:
            fields.append({"name": "Edge", "value": f"{est_edge_pct:.2f}%", "inline": True})
        if link:
            fields.append({"name": "Link", "value": link, "inline": False})
        await self._send_discord_safe("⚡ Arbitrage Opportunity", fields)

    async def queue_alert(
        self,
        message: str,
        *,
        kind: str = "status",
        ttl: Optional[int] = None,
        dedupe: bool = True,
    ):
        """
        Send a one-off message (status / devops / heads-up).
        Dedupe by message hash during the cooldown window if dedupe=True.
        """
        if not self.enabled:
            return

        if dedupe:
            if not await self._allowed_for_message(kind, message, ttl=ttl):
                return
        else:
            # no dedupe; still respect per-kind cooldown using a generalized key
            _key = _hash(f"{kind}:{datetime.utcnow().isoformat()}")
            await self._allowed(kind, _key, ttl=ttl)

        if self.enable_telegram:
            await send_telegram(message)

        fields = [{"name": "Message", "value": message, "inline": False}]
        await self._send_discord_safe("ℹ️ Status", fields)

    # ────────────────────────────────────────────────────────────────────────
    # Convenience wrappers for external callers (compat)
    # ────────────────────────────────────────────────────────────────────────
    async def alert_allowed(self, token_mint: str, alert_type: str, ttl: Optional[int] = None) -> bool:
        """
        Backwards-compat for existing code paths that check allowance outside.
        """
        return await self._allowed(alert_type, token_mint, ttl=ttl)
