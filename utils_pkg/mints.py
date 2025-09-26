# utils/mints.py
def normalize_mint(m: str) -> str:
    if not m:
        return m
    for suf in ("pump", "bonk"):
        if m.endswith(suf):
            m = m[:-len(suf)]
    # Some streams append a 4-char salt; most Solana mints are 32 bytes -> up to 44 base58 chars
    return m[:44]
