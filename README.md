# Solana Sniper Bot — God-Tier Overhaul (V3+++ / Phase 0–7 LOCK-IN)

Production-first Solana trading system designed for **Render + local parity**, with a **single canonical entrypoint** (`./start.sh`) that:
- safely loads env (no eval by default),
- materializes wallet secrets safely,
- normalizes Postgres DSNs (SQLAlchemy async + asyncpg),
- synthesizes `REDIS_URL` from Render Key Value wiring (optional),
- runs **fail-fast gates** (`check_env` + `preflight` + optional `test_pg`),
- then starts the main orchestrator module.

> **Canonical rule:** If it can run, it must run through `./start.sh`.

---

## Table of Contents
- [Quick Start (Local)](#quick-start-local)
- [Render Deployment (Blueprint)](#render-deployment-blueprint)
- [Environment Variables](#environment-variables)
- [Operational Commands](#operational-commands)
- [Debug Playbook](#debug-playbook)
- [Repo Conventions](#repo-conventions)
- [Security Notes](#security-notes)

---

## Quick Start (Local)

### 1) Requirements
- Python runtime matching your project (Render uses `runtime: python` and `runtime.txt` if present).
- **Bash** (recommended):
  - macOS/Linux: built-in
  - Windows: **Git Bash** or **WSL** (recommended).
    (The canonical launcher is `start.sh`.)

### 2) Create venv + install deps
```bash
python -m venv .venv
# macOS/Linux:
source .venv/bin/activate
# Windows PowerShell (optional if you’re not using Git Bash):
# .\.venv\Scripts\Activate.ps1

python -m pip install --upgrade pip
pip install -r requirements.txt
