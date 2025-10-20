#!/usr/bin/env bash
# start.sh — Render entrypoint
# Robust boot with DSN normalization, local Jupiter relay & sanity checks.

set -Eeuo pipefail

log(){ printf '[start] %s\n' "$*"; }

log "booting…"

# ---------------------------------------------------------
# Wallet bootstrap (optional)
# ---------------------------------------------------------
mkdir -p /opt/render/.secrets
chmod 700 /opt/render/.secrets

if [[ -n "${WALLET_JSON:-}" && ! -f /opt/render/.secrets/wallet.json ]]; then
  printf '%s' "$WALLET_JSON" > /opt/render/.secrets/wallet.json
  chmod 600 /opt/render/.secrets/wallet.json
fi

export WALLET_PRIVATE_KEY="${WALLET_PRIVATE_KEY:-/opt/render/.secrets/wallet.json}"

# ---------------------------------------------------------
# Local data dirs
# ---------------------------------------------------------
mkdir -p data /tmp
chmod 755 data || true

# ---------------------------------------------------------
# Choose Python (prefer venv)
# ---------------------------------------------------------
PYTHON="python"
if [[ -x ".venv/bin/python" ]]; then
  PYTHON=".venv/bin/python"
fi
log "python: $(${PYTHON} -V 2>&1)"

# ---------------------------------------------------------
# DSN normalizers (belt & suspenders)
#   - SQLAlchemy DSNs must use postgresql+asyncpg://... ?sslmode=require
#   - asyncpg DSNs must use postgresql://... ?sslmode=require
#   - Accepts either, rewrites and exports back.
# ---------------------------------------------------------
normalize_sqlalchemy_dsn() {
  # in: $1  out: normalized SQLAlchemy async URL
  local v="$1"
  # ensure +asyncpg driver
  if [[ "$v" =~ ^postgresql:// ]]; then
    v="${v/postgresql:\/\//postgresql+asyncpg://}"
  fi
  # never allow a rogue ?ssl=true
  v="${v//ssl=true/sslmode=require}"
  # ensure sslmode=require present
  if [[ "$v" == *\?* ]]; then
    [[ "$v" == *sslmode=* ]] || v="${v}&sslmode=require"
  else
    v="${v}?sslmode=require"
  fi
  printf '%s' "$v"
}

normalize_asyncpg_dsn() {
  # in: $1  out: normalized asyncpg DSN (no +asyncpg)
  local v="$1"
  # strip +asyncpg if present
  v="${v/postgresql+asyncpg:\/\//postgresql://}"
  # never allow ssl=true
  v="${v//ssl=true/sslmode=require}"
  # ensure sslmode=require present
  if [[ "$v" == *\?* ]]; then
    [[ "$v" == *sslmode=* ]] || v="${v}&sslmode=require"
  else
    v="${v}?sslmode=require"
  fi
  printf '%s' "$v"
}

# Required envs (let’s be explicit and helpful)
: "${DATABASE_URL:?DATABASE_URL must be set (e.g. postgresql+asyncpg://user:pass@host/db?sslmode=require)}"
: "${PG_DSN_INTERNAL:?PG_DSN_INTERNAL must be set (e.g. postgresql+asyncpg://user:pass@host/db?sslmode=require)}"
: "${PG_DSN_EXTERNAL:?PG_DSN_EXTERNAL must be set (e.g. postgresql+asyncpg://user:pass@host/db?sslmode=require)}"
: "${PG_DSN:?PG_DSN must be set for asyncpg use (e.g. postgresql://user:pass@host/db?sslmode=require)}"
# Accept PNL_DSN as alias; keep both in sync for consumers
PNL_DSN="${PNL_DSN:-$PG_DSN}"

# Normalize in place
DATABASE_URL="$(normalize_sqlalchemy_dsn "${DATABASE_URL}")"
PG_DSN_INTERNAL="$(normalize_sqlalchemy_dsn "${PG_DSN_INTERNAL}")"
PG_DSN_EXTERNAL="$(normalize_sqlalchemy_dsn "${PG_DSN_EXTERNAL}")"
PG_DSN="$(normalize_asyncpg_dsn "${PG_DSN}")"
PNL_DSN="$(normalize_asyncpg_dsn "${PNL_DSN}")"

export DATABASE_URL PG_DSN_INTERNAL PG_DSN_EXTERNAL PG_DSN PNL_DSN

# Extra guard: print short masked DSNs for logs
_short_host () { printf '%s' "$1" | sed -E 's#^.*@([^/?]+).*$#\1#'; }
log "DB(SQLA): ${DATABASE_URL%%\?*}?…"
log "DB(asyncpg): ${PNL_DSN%%\?*}?…"

# Optional runtime sanity: try connecting via asyncpg quickly (non-fatal)
${PYTHON} - <<'PY' || true
import os, asyncio, sys
try:
    import asyncpg
except Exception as e:
    print("[start] asyncpg not installed; skipping DSN probe", flush=True)
    sys.exit(0)

dsn = os.environ.get("PNL_DSN") or os.environ.get("PG_DSN")
async def go():
    try:
        conn = await asyncpg.connect(dsn)
        print("[start] asyncpg DSN OK", flush=True)
        await conn.close()
    except Exception as e:
        print(f"[start] asyncpg DSN PROBE FAILED: {e}", flush=True)

asyncio.run(go())
PY

# ---------------------------------------------------------
# Optional local Jupiter relay
#   Using a local relay avoids DNS/CF hiccups and Brotli decode warnings.
# ---------------------------------------------------------
RELAY_BIND="${RELAY_BIND:-127.0.0.1}"
RELAY_PORT="${RELAY_PORT:-18080}"

if [[ -f "./jup_relay.py" ]]; then
  log "starting local Jupiter relay…"
  pkill -f '[j]up_relay.py' || true
  nohup "${PYTHON}" -u ./jup_relay.py \
    > /tmp/jup_relay.log 2>&1 &
  echo $! > /tmp/jup_relay.pid

  # Health check (up to ~12s)
  for _ in {1..12}; do
    if curl -fsS "http://${RELAY_BIND}:${RELAY_PORT}/__debug" >/dev/null 2>&1; then
      log "relay up on ${RELAY_BIND}:${RELAY_PORT}"
      export JUP_BASE_URL="http://${RELAY_BIND}:${RELAY_PORT}"
      export JUP_ALT_BASE_URLS=""
      break
    fi
    sleep 1
  done

  if [[ "${JUP_BASE_URL:-}" != "http://${RELAY_BIND}:${RELAY_PORT}" ]]; then
    log "relay failed to start; first lines:"
    (head -n 120 /tmp/jup_relay.log || true) | sed 's/^/[relay] /'
    log "falling back to public Jupiter"
    export JUP_BASE_URL="${JUP_BASE_URL:-https://quote-api.jup.ag}"
    export JUP_ALT_BASE_URLS=""
  fi
else
  log "jup_relay.py not found; using public Jupiter API"
  : "${JUP_BASE_URL:=https://quote-api.jup.ag}"
  export JUP_BASE_URL
  export JUP_ALT_BASE_URLS="${JUP_ALT_BASE_URLS:-}"
fi

log "JUP_BASE_URL: ${JUP_BASE_URL}"

# ---------------------------------------------------------
# Positions storage guard (sqlite-only mode shortcut)
# ---------------------------------------------------------
if [[ "${PF_POSITIONS_BACKEND:-sqlite}" == "sqlite" ]]; then
  export POSITIONS_PG_ENABLED=0
  export POSITIONS_PG_DSN=""
fi

# ---------------------------------------------------------
# Final launch
# ---------------------------------------------------------
exec "${PYTHON}" -m scripts.main
