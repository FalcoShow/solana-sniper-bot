import os, json, numpy as np, pathlib
pathlib.Path(os.getenv("DATA_DIR","data")).mkdir(parents=True, exist_ok=True)

# last 30 “daily” returns (dummy); feel free to tweak
seed = {
  "daytrader": [0.002, -0.001, 0.003, 0.0, 0.0015]*6,
  "sniper":    [0.004, -0.003, 0.002, 0.001, -0.001]*6,
  "perps":     [0.0]*30
}
with open(os.path.join(os.getenv("DATA_DIR","data"), "trailing_returns.json"), "w") as f:
    json.dump(seed, f)

np.save(os.path.join(os.getenv("DATA_DIR","data"), "equity_returns.npy"),
        np.array([0.001, -0.0005, 0.002, 0.0008]*80, dtype=float))

print("Seeded allocator cache.")
