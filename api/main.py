from fastapi import FastAPI , HTTPException
from datetime import datetime
from zoneinfo import ZoneInfo
import redis
import json
import os

app = FastAPI()

REDIS_HOST = os.getenv("REDIS_HOST_EXTERNAL","redis")
REDIS_PORT = int(os.getenv("REDIS_PORT",6379))

def to_istanbul_time(dt_str):
    dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))

    return dt.astimezone(ZoneInfo("Europe/Istanbul")).isoformat()


r = redis.Redis(
    host=REDIS_HOST,
    port = REDIS_PORT,
    decode_responses=True
)

@app.get("/latest-candle/{symbol}")
def get_latest_candle(symbol:str):
    key =f"latest_candle:{symbol.upper()}"

    data = r.get(key)

    if not data:
        raise HTTPException(status_code =404,detail="Symbol not found")
    
    return json.loads(data)

@app.get("/symbols")
def get_symbols():
    keys = r.keys("latest_candle:*")
    symbols = [key.replace("latest_candle:", "") for key in keys]
    return {"symbols": sorted(symbols)}

@app.get("/latest-price/{symbol}")
def get_latest_price(symbol: str):
    key = f"latest_candle:{symbol.upper()}"
    data = r.get(key)

    if not data:
        raise HTTPException(status_code=404, detail="Symbol not found")

    candle = json.loads(data)

    return {
        "symbol": candle["symbol"],
        "price": candle["close_price"],
        "time_utc": candle["window_end"],
        "time_tr" : to_istanbul_time(candle["window_end"]),
    }   