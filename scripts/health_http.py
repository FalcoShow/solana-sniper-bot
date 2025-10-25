# scripts/health_http.py
from fastapi import FastAPI
import uvicorn, os

app = FastAPI()

@app.get("/health")
def health():
    return {"ok": True, "dryrun": os.getenv("DRYRUN"), "paused": os.getenv("PAUSE_TRADING")}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
