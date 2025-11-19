#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
log(){ printf '[start] %s %s\n' "$(ts)" "$*"; }

log "──────────────────────────────"
log "   Solana Sniper Bootstrap"
log "──────────────────────────────"

# =====================================================================
# 0) LOAD ENV FILES (Render → .env.extra → .env)
# =====================================================================
if [[ -f /etc/secrets/.env.extra ]]; then
  log "Loading environment secret file: /etc/secrets/.env.extra"
  set -a
  # shellcheck disable=SC1091
  . /etc/secrets/.env.extra
  set +a
else
  log "No /etc/secrets/.env.extra found"
fi

# Optional project .env override
set -a
if [[ -f .env ]]; then
  # shellcheck disable=SC1091
  . ./.env
  log "Loaded local .env"
else
  log "No local .env (expected on Render)"
fi
set +a


# =====================================================================
# 1) Jupiter API normalization
# =====================================================================
: "${JUP_BASE_URL:=https://api.jup.ag}"
: "${JUP_ALT_BASE_URLS:=https://api.jup.ag}"
: "${JUPITER_SWAP_API:=${JUP_BASE_URL}}"
export JUP_BASE_URL JUP_ALT_BASE_URLS JUPITER_SWAP_API

log "Jupiter base URL: $JUP_BASE_URL"

if command -v curl >/dev/null 2>&1; then
  log "Probing Jupiter..."
  curl -sS -m 5 -D - \
    -H "x-api-key: ${JUP_PRO_KEY:-${JUP_API_KEY:-}}" \
    "${JUP_BASE_URL%/}/swap/v1/quote?inputMint=So11111111111111111111111111111111111111112&outputMint=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v&amount=5000000&slippageBps=200" \
    -o /dev/null | head -c 0 || true
fi


# =====================================================================
# 2) WALLET SETUP
# =====================================================================
mkdir -p /opt/render/.secrets
chmod 700 /opt/render/.secrets || true

if [[ -n "${WALLET_JSON:-}" && ! -f /opt/render/.secrets/wallet.json ]]; then
  printf '%s' "$WALLET_JSON" > /opt/render/.secrets/wallet.json
  chmod 600 /opt/render/.secrets/wallet.json || true
  log "Created wallet.json from WALLET_JSON"
fi

export WALLET_PRIVATE_KEY="${WALLET_PRIVATE_KEY:-/opt/render/.secrets/wallet.json}"


# =====================================================================
# 3) Local data dirs
# =====================================================================
mkdir -p /opt/render/data /tmp
chmod 755 /opt/render/data || true


# =====================================================================
# 4) Select Python
# =====================================================================
PYTHON="python"
[[ -x ".venv/bin/python" ]] && PYTHON=".venv/bin/python"
log "Using python: $($PYTHON -V)"


# =====================================================================
# 5) DATABASE DSN NORMALIZATION
# =====================================================================
normalize_sqlalchemy_dsn() {
  local v="$1"
  [[ "$v" =~ ^postgresql:// ]] && v="${v/postgresql:\/\//postgresql+asyncpg://}"
  v="${v//ssl=true/sslmode=require}"
  if [[ "$v" != *sslmode=* ]]; then
    if [[ "$v" == *\?* ]]; then
      v="${v}&sslmode=require"
    else
      v="${v}?sslmode=require"
    fi
  fi
  printf '%s' "$v"
}

normalize_asyncpg_dsn() {
  local v="$1"
  v="${v/postgresql+asyncpg:\/\//postgresql://}"
  v="${v//ssl=true/sslmode=require}"
  if [[ "$v" != *sslmode=* ]]; then
    if [[ "$v" == *\?* ]]; then
      v="${v}&sslmode=require"
    else
      v="${v}?sslmode=require"
    fi
  fi
  printf '%s' "$v"
}

: "${DATABASE_URL:?DATABASE_URL missing}"
: "${PG_DSN_INTERNAL:?PG_DSN_INTERNAL missing}"
: "${PG_DSN_EXTERNAL:?PG_DSN_EXTERNAL missing}"
: "${PG_DSN:?PG_DSN missing}"

PNL_DSN="${PNL_DSN:-$PG_DSN}"

DATABASE_URL="$(normalize_sqlalchemy_dsn "${DATABASE_URL}")"
PG_DSN_INTERNAL="$(normalize_sqlalchemy_dsn "${PG_DSN_INTERNAL}")"
PG_DSN_EXTERNAL="$(normalize_sqlalchemy_dsn "${PG_DSN_EXTERNAL}")"
PG_DSN="$(normalize_asyncpg_dsn "${PG_DSN}")"
PNL_DSN="$(normalize_asyncpg_dsn "${PNL_DSN}")"

export DATABASE_URL PG_DSN_INTERNAL PG_DSN_EXTERNAL PG_DSN PNL_DSN

log "Database SQLA  : ${DATABASE_URL%%\?*}?..."
log "Database asyncpg: ${PG_DSN%%\?*}?..."


# =====================================================================
# 6) OPTIONAL Jupiter relay (rarely used)
# =====================================================================
: "${RELAY_ENABLED:=0}"
if [[ "${RELAY_ENABLED}" == "1" ]]; then
  log "Starting Jupiter relay..."
  export RELAY_BIND RELAY_PORT RELAY_UPSTREAM RELAY_FORCE_IPV4
  nohup "${PYTHON}" ./jup_relay.py > /tmp/jup_relay.log 2>&1 &
  sleep 2
  if curl -fs "http://127.0.0.1:8081/__debug" >/dev/null 2>&1; then
    log "Relay OK, switching JUP endpoints"
    export JUP_BASE_URL="http://127.0.0.1:8081"
    export JUP_ALT_BASE_URLS="http://127.0.0.1:8081"
    export JUPITER_SWAP_API="http://127.0.0.1:8081"
  else
    log "Relay failed, falling back to public Jupiter"
  fi
fi


# =====================================================================
# 7) POSITIONS BACKEND GUARD
# =====================================================================
if [[ "${PF_POSITIONS_BACKEND:-pg}" != "pg" ]]; then
  export POSITIONS_PG_ENABLED=0
  export POSITIONS_PG_DSN=""
fi


# =====================================================================
# 8) OPTIONAL PRECHECKS
# =====================================================================
: "${START_PRECHECKS:=0}"
if [[ "${START_PRECHECKS}" == "1" ]]; then
  log "Running router precheck..."
  "${PYTHON}" -m scripts.check_router || log "check_router failed"
  log "Running canary trade precheck..."
  "${PYTHON}" -m scripts.canary_trade || log "canary_trade failed"
fi


# =====================================================================
# 9) FINAL LAUNCH
# =====================================================================
log "Launching sniper main…"
exec "${PYTHON}" -m scripts.main
