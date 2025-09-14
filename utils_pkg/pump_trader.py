# utils_pkg/pump_trader.py
import os, json, requests

PP_API_KEY = os.getenv("PF_PP_KEY")

def build_buy_tx_via_portal(mint: str, payer_pubkey: str, *,
                            amount_sol: float = 0.01, slippage_bps: int = 1000,
                            priority_sol: float = 0.0005) -> str:
    url = "https://pumpportal.fun/api/trade"  # adjust if your builder differs
    headers = {
        "content-type": "application/json",
        "x-api-key": PP_API_KEY or "",         # some setups require header
    }
    body = {
        "apiKey": PP_API_KEY,                  # some setups require in body
        "publicKey": payer_pubkey,
        "action": "buy",
        "mint": mint,
        "amount": amount_sol,
        "slippage": slippage_bps,
        "priorityFee": priority_sol,
        "pool": "pump",
    }
    r = requests.post(url, headers=headers, data=json.dumps(body), timeout=20)
    r.raise_for_status()
    js = r.json()
    tx_b64 = js.get("transaction") or js.get("tx")
    if not tx_b64:
        raise RuntimeError(f"Builder response missing tx: {js}")
    return tx_b64
