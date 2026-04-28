import json
import os
import time
from decimal import Decimal
from datetime import datetime

import psycopg2
import redis


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "crypto")
POSTGRES_USER = os.getenv("POSTGRES_USER", "crypto_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "crypto_pass")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS","5"))


def json_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    return str(obj)

def update_latest_candles():
    pg_conn = psycopg2.connect(
        host = POSTGRES_HOST,
        port =POSTGRES_PORT,
        dbname =POSTGRES_DB,
        user = POSTGRES_USER,
        password =POSTGRES_PASSWORD,
    )

    r = redis.Redis(
        host =REDIS_HOST,
        port = REDIS_PORT,
        decode_responses=True,
    )

    query = """
        SELECT DISTINCT ON (symbol)
            symbol,
            window_start,
            window_end,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            trade_count,
            candle_return_pct,
            price_range,
            candle_type,
            created_at
        FROM public_marts.fct_ohlcv_10s
        ORDER BY symbol, window_start DESC;
    """    
    with pg_conn.cursor() as cur:
        cur.execute(query)
        cols =[desc[0] for desc in cur.description]
        rows = cur.fetchall()
        
    for row in rows:
        data=dict(zip(cols,row))
        symbol = data["symbol"]

        key =f"latest_candle:{symbol}"
        r.set(key,json.dumps(data,default=json_default))
        
        print(f"Updated Redis key:{key}")
    
    pg_conn.close()


def main():
    print("Redis writer started..")

    while True:
        try:
            update_latest_candles()
        except Exception as e:
            print(f"Redis writer error: {e}")
        
        time.sleep(REFRESH_SECONDS)

if __name__ == "__main__":
    main()