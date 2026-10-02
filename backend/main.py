import asyncio

import json

import os

import time

import sqlite3

import gzip

from collections import defaultdict, deque

from pathlib import Path

from datetime import datetime
NSE_HOLIDAYS_2026 = {
    "2026-01-15": "Municipal Corporation Election - Maharashtra",
    "2026-01-26": "Republic Day",
    "2026-03-03": "Holi",
    "2026-03-26": "Shri Ram Navami",
    "2026-03-31": "Shri Mahavir Jayanti",
    "2026-04-03": "Good Friday",
    "2026-04-14": "Dr. Baba Saheb Ambedkar Jayanti",
    "2026-05-01": "Maharashtra Day",
    "2026-05-28": "Bakri Id",
    "2026-06-26": "Muharram",
    "2026-09-14": "Ganesh Chaturthi",
    "2026-10-02": "Mahatma Gandhi Jayanti",
    "2026-10-20": "Dussehra",
    "2026-11-10": "Diwali-Balipratipada",
    "2026-11-24": "Prakash Gurpurb Sri Guru Nanak Dev",
    "2026-12-25": "Christmas",
}


def get_nse_market_status():
    today = datetime.now().strftime("%Y-%m-%d")

    if today in NSE_HOLIDAYS_2026:
        return {
            "status": "CLOSED",
            "reason": NSE_HOLIDAYS_2026[today],
        }

    return {
        "status": "OPEN",
        "reason": "",
    }

from urllib.parse import quote

import requests

import websockets

from dotenv import load_dotenv

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException

from fastapi.middleware.cors import CORSMiddleware

from nse_client import (
    get_stock_quote,
    get_live_gainers,
    get_live_losers,
    get_index_data,
)

load_dotenv(Path(__file__).with_name(".env"))

APP_DIR = Path(__file__).resolve().parent

DB_PATH = APP_DIR / "marketflow.db"

app = FastAPI(title="MarketFlow Live API", version="3.0.0")

app.add_middleware(

    CORSMiddleware,

    allow_origins=[

    "http://localhost:5173",

    "http://127.0.0.1:5173",

    "http://localhost:5174",

    "http://127.0.0.1:5174",
    "https://marketflow-nkbr.onrender.com",

],

    allow_credentials=True,

    allow_methods=["*"],

    allow_headers=["*"],

)

clients = set()

prices = {}

history = defaultdict(lambda: deque(maxlen=300))

instrument_catalog = {}

subscribed_keys = set()

background_tasks = []

usd_inr = 85.0

last_fx_update = 0.0

# Public NSE instrument master used only to discover the NSE equity universe.

# No Upstox account, token, or authenticated API is used.

NSE_INSTRUMENTS_URL = (

    "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

)

NSE_CACHE_PATH = APP_DIR / "nse_instruments.json"

CRYPTO_SYMBOLS = {

    "BTCUSDT": {

        "name": "Bitcoin",

        "symbol": "BTC",

        "pair": "btcusdt",

        "type": "crypto",

    },

    "ETHUSDT": {

        "name": "Ethereum",

        "symbol": "ETH",

        "pair": "ethusdt",

        "type": "crypto",

    },

    "SOLUSDT": {

        "name": "Solana",

        "symbol": "SOL",

        "pair": "solusdt",

        "type": "crypto",

    },

}

STOCK_META = {

    "NSE_INDEX|Nifty 50": {

        "name": "NIFTY 50",

        "symbol": "NIFTY",

        "type": "index",

    },

    "NSE_INDEX|Nifty Bank": {

        "name": "NIFTY BANK",

        "symbol": "BANKNIFTY",

        "type": "index",

    },

    "NSE_INDEX|India VIX": {

        "name": "INDIA VIX",

        "symbol": "INDIAVIX",

        "type": "index",

    },

    "NSE_EQ|INE002A01018": {

        "name": "Reliance Industries",

        "symbol": "RELIANCE",

        "type": "stock",

    },

    "NSE_EQ|INE467B01029": {

        "name": "Tata Consultancy Services",

        "symbol": "TCS",

        "type": "stock",

    },

    "NSE_EQ|INE009A01021": {

        "name": "Infosys",

        "symbol": "INFY",

        "type": "stock",

    },

    "NSE_EQ|INE040A01034": {

        "name": "HDFC Bank",

        "symbol": "HDFCBANK",

        "type": "stock",

    },

    "NSE_EQ|INE090A01021": {

        "name": "ICICI Bank",

        "symbol": "ICICIBANK",

        "type": "stock",

    },

    "NSE_EQ|INE062A01020": {

        "name": "State Bank of India",

        "symbol": "SBIN",

        "type": "stock",

    },

    "NSE_EQ|INE154A01025": {

        "name": "ITC",

        "symbol": "ITC",

        "type": "stock",

    },

    "NSE_EQ|INE018A01030": {

        "name": "Larsen & Toubro",

        "symbol": "LT",

        "type": "stock",

    },

    "NSE_EQ|INE030A01027": {

        "name": "Hindustan Unilever",

        "symbol": "HINDUNILVR",

        "type": "stock",

    },

    "NSE_EQ|INE397D01024": {

        "name": "Bharti Airtel",

        "symbol": "BHARTIARTL",

        "type": "stock",

    },

    "NSE_EQ|INE238A01034": {

        "name": "Axis Bank",

        "symbol": "AXISBANK",

        "type": "stock",

    },

    "NSE_EQ|INE585B01010": {

        "name": "Maruti Suzuki",

        "symbol": "MARUTI",

        "type": "stock",

    },

    "NSE_EQ|INE237A01028": {

        "name": "Kotak Mahindra Bank",

        "symbol": "KOTAKBANK",

        "type": "stock",

    },

    "NSE_EQ|INE280A01028": {

        "name": "Titan Company",

        "symbol": "TITAN",

        "type": "stock",

    },

    "NSE_EQ|INE044A01036": {

        "name": "Sun Pharmaceutical",

        "symbol": "SUNPHARMA",

        "type": "stock",

    },

}

instrument_catalog.update(STOCK_META)

def load_nse_instruments():

    """Load the complete NSE equity universe from the local NSE instrument file."""

    global instrument_catalog

    try:

        if not NSE_CACHE_PATH.exists():

            print(f"NSE instrument file not found: {NSE_CACHE_PATH}")

            return

        raw = NSE_CACHE_PATH.read_bytes()

        rows = json.loads(raw)

        count = 0

        allowed_types = {

            "EQ",

            "BE",

            "BZ",

            "SM",

            "ST",

            "SZ",

            "E",

            "M",

        }

        for row in rows:

            if row.get("segment") != "NSE_EQ":

                continue

            if row.get("instrument_type") not in allowed_types:

                continue

            key = row.get("instrument_key")

            symbol = row.get("trading_symbol")

            if not key or not symbol:

                continue

            instrument_catalog[key] = {

                "name": row.get("short_name") or row.get("name") or symbol,

                "symbol": symbol,

                "type": "stock",

                "exchange": "NSE",

                "isin": row.get("isin", ""),

            }

            count += 1

        print(f"Loaded {count} NSE equity instruments from local nse_instruments.json.")

    except Exception as exc:

        print("NSE local instrument load failed:", exc)

def catalog_item(key):

    meta = instrument_catalog.get(key)

    if meta:

        return meta

    return {

        "name": key.split("|")[-1],

        "symbol": key.split("|")[-1],

        "type": "stock",

        "exchange": "NSE",

    }

def db():

    conn = sqlite3.connect(DB_PATH)

    conn.row_factory = sqlite3.Row

    conn.execute(

        """

        CREATE TABLE IF NOT EXISTS alerts (

            id INTEGER PRIMARY KEY AUTOINCREMENT,

            symbol TEXT NOT NULL,

            condition TEXT NOT NULL,

            target REAL NOT NULL,

            triggered INTEGER NOT NULL DEFAULT 0,

            created_at TEXT NOT NULL

        )

        """

    )

    conn.commit()

    return conn

def read_alerts():

    conn = db()

    rows = conn.execute("SELECT * FROM alerts ORDER BY id DESC").fetchall()

    conn.close()

    return [dict(row) for row in rows]

def make_item(

    key,

    meta,

    price,

    previous=None,

    source="NSE MCP",

    currency="INR",

):

    price = float(price)

    previous = float(previous if previous is not None else price)

    change = price - previous

    pct = (change / previous * 100) if previous else 0

    item = {

        "key": key,

        "symbol": meta.get("symbol", key.split("|")[-1]),

        "name": meta.get("name", key.split("|")[-1]),

        "type": meta.get("type", "stock"),

        "exchange": meta.get("exchange", "NSE"),

        "price": round(price, 4),

        "previousClose": round(previous, 4),

        "change": round(change, 4),

        "changePct": round(pct, 4),

        "currency": currency,

        "source": source,

        "timestamp": int(time.time() * 1000),

    }

    prices[key] = item

    history[key].append({"t": item["timestamp"], "p": item["price"]})

    return item

def extract_mcp_json(result):

    for part in getattr(result, "content", []) or []:

        raw = getattr(part, "text", None)

        if not raw:

            continue

        try:

            data = json.loads(raw)

            # NSE MCP may return the actual quote

            # as JSON text inside the "data" field.

            if isinstance(data, dict):

                nested = data.get("data")

                if isinstance(nested, str):

                    try:

                        data["data"] = json.loads(nested)

                    except Exception:

                        pass

            return data

        except Exception:

            continue

    return None

def unwrap_quote(data, symbol):

    if not isinstance(data, dict):

        return None

    # NSE MCP stock quote response:

    # {"updatedAt": "...", "stock": {...}}

    quote_data = data.get("stock")

    if isinstance(quote_data, dict):

        return quote_data

    # Other possible NSE response formats

    quote_data = data.get("data", data)

    if isinstance(quote_data, dict) and symbol in quote_data:

        quote_data = quote_data[symbol]

    if isinstance(quote_data, dict):

        if (

            "lastTradedPrice" not in quote_data

            and len(quote_data) == 1

        ):

            only_value = next(iter(quote_data.values()))

            if isinstance(only_value, dict):

                quote_data = only_value

    return quote_data if isinstance(quote_data, dict) else None 

def parse_mover_list(result):

    data = extract_mcp_json(result)

    if not isinstance(data, dict):

        return []

    # NSE MCP responses may contain the records under data.

    payload = data.get("data", data)

    if isinstance(payload, dict):

        for key in ("data", "stocks", "gainers", "losers", "results"):

            if isinstance(payload.get(key), list):

                payload = payload[key]

                break

    if not isinstance(payload, list):

        return []

    return [row for row in payload if isinstance(row, dict)]

async def broadcast(payload):

    if not clients:

        return

    message = json.dumps(payload)

    dead = []

    for ws in list(clients):

        try:

            await ws.send_text(message)

        except Exception:

            dead.append(ws)

    for ws in dead:

        clients.discard(ws)

def check_alerts(item):

    conn = db()

    rows = conn.execute(

        "SELECT * FROM alerts WHERE symbol=? AND triggered=0",

        (item["symbol"],),

    ).fetchall()

    hits = []

    for row in rows:

        hit = (

            row["condition"] == "above"

            and item["price"] >= row["target"]

        ) or (

            row["condition"] == "below"

            and item["price"] <= row["target"]

        )

        if hit:

            conn.execute(

                "UPDATE alerts SET triggered=1 WHERE id=?",

                (row["id"],),

            )

            hits.append(

                {

                    "id": row["id"],

                    "symbol": item["symbol"],

                    "name": item["name"],

                    "price": item["price"],

                    "target": row["target"],

                    "condition": row["condition"],

                }

            )

    conn.commit()

    conn.close()

    return hits

async def update_fx():

    global usd_inr, last_fx_update

    if time.time() - last_fx_update < 60:

        return usd_inr

    try:

        response = await asyncio.to_thread(

            requests.get,

            "https://open.er-api.com/v6/latest/USD",

            timeout=5,

        )

        data = response.json()

        usd_inr = float(data.get("rates", {}).get("INR", usd_inr))

        last_fx_update = time.time()

    except Exception:

        pass

    return usd_inr

async def crypto_feed():

    streams = "/".join(

        f"{item['pair']}@ticker" for item in CRYPTO_SYMBOLS.values()

    )

    url = f"wss://stream.binance.com:9443/stream?streams={streams}"

    while True:

        try:

            async with websockets.connect(

                url,

                ping_interval=20,

                ping_timeout=20,

            ) as ws:

                await broadcast(

                    {

                        "type": "status",

                        "source": "crypto",

                        "connected": True,

                    }

                )

                async for raw in ws:

                    data = json.loads(raw).get("data", {})

                    pair = data.get("s")

                    if pair not in CRYPTO_SYMBOLS:

                        continue

                    fx = await update_fx()

                    meta = CRYPTO_SYMBOLS[pair]

                    price_usd = float(data.get("c", 0))

                    open_usd = float(data.get("o", price_usd))

                    item = make_item(

                        pair,

                        meta,

                        price_usd * fx,

                        open_usd * fx,

                        "Binance live WebSocket",

                        "INR",

                    )

                    item["usdPrice"] = round(price_usd, 6)

                    item["fxUsdInr"] = round(fx, 4)

                    await broadcast({"type": "ticker", "item": item})

                    for hit in check_alerts(item):

                        await broadcast({"type": "alert", "item": hit})

        except Exception as exc:

            await broadcast(

                {

                    "type": "status",

                    "source": "crypto",

                    "connected": False,

                    "error": str(exc),

                }

            )

            await asyncio.sleep(5)

async def refresh_nse_movers():

    """
    NSE MCP does not provide a tick-by-tick subscription API in the tools
    verified for this project. Refresh current live gainers/losers instead.
    """

    while True:

        try:

            market_status = get_nse_market_status()

            if market_status["status"] == "CLOSED":
                print(
                    f"NSE market closed today: "
                    f"{market_status['reason']}"
                )
                await asyncio.sleep(300)
                continue

            gainers_result = await get_live_gainers()

            losers_result = await get_live_losers()

            movers = parse_mover_list(gainers_result)

            for row in movers:

                symbol = str(

                    row.get("symbol")

                    or row.get("trading_symbol")

                    or ""

                ).upper()

                if not symbol:

                    continue

                ltp = row.get("ltp", row.get("lastTradedPrice"))

                previous = row.get(

                    "prev_price",

                    row.get("preClosePrice"),

                )

                if ltp is None:

                    continue

                key = f"NSE_MCP|{symbol}"

                meta = {

                    "name": symbol,

                    "symbol": symbol,

                    "type": "stock",

                    "exchange": "NSE",

                }

                instrument_catalog[key] = meta

                item = make_item(

                    key,

                    meta,

                    float(ltp),

                    float(previous) if previous is not None else float(ltp),

                    "NSE MCP",

                    "INR",

                )

                await broadcast({"type": "ticker", "item": item})

                for hit in check_alerts(item):

                    await broadcast({"type": "alert", "item": hit})

            await broadcast(

                {

                    "type": "status",

                    "source": "nse",

                    "connected": True,

                    "moversUpdated": int(time.time() * 1000),

                }

            )

        except Exception as exc:

            print("NSE movers refresh failed:", exc)

            await broadcast(

                {

                    "type": "status",

                    "source": "nse",

                    "connected": False,

                    "error": str(exc),

                }

            )

        # Avoid excessive requests to the public NSE MCP.

        await asyncio.sleep(60)

async def quote_symbol(symbol):

    symbol = symbol.strip().upper()
        # Handle NSE indexes using the official NSE index endpoint.
    if symbol in {"NIFTY", "BANKNIFTY", "INDIAVIX"}:
        index_data = await get_index_data(symbol)

        if not index_data:
            return None

        index_names = {
            "NIFTY": "NIFTY 50",
            "BANKNIFTY": "NIFTY BANK",
            "INDIAVIX": "INDIA VIX",
        }

        index_keys = {
            "NIFTY": "NSE_INDEX|Nifty 50",
            "BANKNIFTY": "NSE_INDEX|Nifty Bank",
            "INDIAVIX": "NSE_INDEX|India VIX",
        }

        last_price = index_data.get("last")
        previous = index_data.get("previousClose")
        change = index_data.get("variation")
        change_pct = index_data.get("percentChange")

        if last_price is None:
            return None

        key = index_keys[symbol]

        meta = {
            "name": index_names[symbol],
            "symbol": symbol,
            "type": "index",
            "exchange": "NSE",
        }

        item = {
            "key": key,
            "name": index_names[symbol],
            "symbol": symbol,
            "type": "index",
            "exchange": "NSE",
            "price": float(last_price),
            "previousClose": float(
                previous if previous is not None else last_price
            ),
            "change": float(change or 0),
            "changePct": float(change_pct or 0),
            "currency": "INR",
            "source": "NSE Official Index",
            "timestamp": int(time.time() * 1000),
        }

        instrument_catalog[key] = meta
        prices[key] = item
        history[key].append({
            "t": item["timestamp"],
            "p": item["price"],
        })

        return item

    result = await get_stock_quote(symbol)

    data = extract_mcp_json(result)

    quote_data = unwrap_quote(data, symbol)

    if not quote_data:

        return None

    last_price = quote_data.get("lastTradedPrice")

    previous = quote_data.get("preClosePrice")

    change = quote_data.get("change")

    change_pct = quote_data.get("perChange")

    if last_price is None:

        return None

    key = f"NSE_MCP|{symbol}"

    meta = {

        "name": symbol,

        "symbol": symbol,

        "type": "stock",

        "exchange": "NSE",

    }

    instrument_catalog[key] = meta

    item = {

        "key": key,

        "symbol": symbol,

        "name": symbol,

        "type": "stock",

        "exchange": "NSE",

        "price": float(last_price),

        "previousClose": float(

            previous if previous is not None else last_price

        ),

        "change": float(change or 0),

        "changePct": float(change_pct or 0),

        "currency": "INR",

        "source": "NSE MCP",

        "timestamp": int(time.time() * 1000),

    }

    prices[key] = item

    history[key].append({"t": item["timestamp"], "p": item["price"]})

    # Also attach the live quote to the matching NSE catalog instrument.

    # The catalog uses NSE_EQ|... keys while direct MCP quotes use NSE_MCP|SYMBOL.

    for catalog_key, catalog_meta in instrument_catalog.items():

        if (

            catalog_meta.get("type") == "stock"

            and str(catalog_meta.get("symbol", "")).upper() == symbol

        ):

            prices[catalog_key] = {

                **item,

                "key": catalog_key,

                "name": catalog_meta.get("name", symbol),

                "symbol": symbol,

            }

            history[catalog_key].append({

                "t": item["timestamp"],

                "p": item["price"],

            })

            break

    return item

@app.on_event("startup")

async def startup():

    db().close()

    await asyncio.to_thread(load_nse_instruments)

    background_tasks.append(asyncio.create_task(crypto_feed()))

    background_tasks.append(asyncio.create_task(refresh_nse_movers()))

    print("MarketFlow started with NSE MCP + Binance crypto data.")

@app.on_event("shutdown")

async def shutdown():

    for task in background_tasks:

        task.cancel()

    background_tasks.clear()

@app.get("/api/health")

async def health():

    stock_count = len(

        [

            key

            for key, meta in instrument_catalog.items()

            if meta.get("type") == "stock"

        ]

    )

    market_status = get_nse_market_status()

    return {

    "ok": True,

    "cryptoLive": True,

    "nseMcpConfigured": True,

    "nseMarketStatus": market_status["status"],

    "nseMarketReason": market_status["reason"],

    "stockKeys": stock_count,

    "catalogSize": len(instrument_catalog),

    "subscribedKeys": len(subscribed_keys),

    "assetsLive": len(prices),

    "dataSource": "NSE MCP",

}
@app.get("/api/market")

async def market():

    out = []

    # Return all known NSE catalog instruments without duplicate symbols.
    # Some NSE instrument-master records can represent the same trading symbol
    # more than once. The UI should show one stock card per NSE symbol.
    seen_symbols = set()

    for key, meta in instrument_catalog.items():
        if meta.get("type") != "stock":
            continue

        symbol = str(meta.get("symbol", "")).strip().upper()
        if not symbol or symbol in seen_symbols:
            continue

        seen_symbols.add(symbol)
        live = prices.get(key)

        if live:
            out.append(live)
        else:
            out.append(
                {
                    "key": key,
                    **meta,
                    "price": None,
                    "previousClose": None,
                    "change": 0,
                    "changePct": 0,
                    "currency": "INR",
                    "source": "Waiting for NSE data",
                    "timestamp": None,
                }
            )

    # Add NSE MCP symbols discovered through search/movers, but never duplicate
    # a symbol that is already represented by the NSE catalog.
    known_symbols = {
        str(item.get("symbol", "")).strip().upper()
        for item in out
        if item.get("type") == "stock"
    }

    for key, item in prices.items():
        if item.get("type") != "stock":
            continue

        symbol = str(item.get("symbol", "")).strip().upper()
        if not symbol or symbol in known_symbols:
            continue

        out.append(item)
        known_symbols.add(symbol)

    # Live-priced instruments first, then highest percentage change.

    out.sort(

        key=lambda x: (

            x.get("price") is None,

            -float(x.get("changePct") or 0),

            x.get("symbol", ""),

        )

    )

    return out

@app.get("/api/catalog")

async def catalog(

    kind: str = "stock",

    q: str = "",

    page: int = 1,

    page_size: int = 100,

):

    rows = []

    ql = q.strip().lower()

    for key, meta in instrument_catalog.items():

        if kind and meta.get("type") != kind:

            continue

        if (

            ql

            and ql not in meta.get("symbol", "").lower()

            and ql not in meta.get("name", "").lower()

        ):

            continue

        live = prices.get(key)

        item = {"key": key, **meta}

        if live:

            item.update(

                {

                    "price": live.get("price"),

                    "previousClose": live.get("previousClose"),

                    "change": live.get("change"),

                    "changePct": live.get("changePct"),

                    "currency": live.get("currency"),

                    "source": live.get("source"),

                    "timestamp": live.get("timestamp"),

                }

            )

        else:

            item.update(

                {

                    "price": None,

                    "previousClose": None,

                    "change": 0,

                    "changePct": 0,

                    "currency": "INR",

                    "source": "Waiting for NSE data",

                    "timestamp": None,

                }

            )

        rows.append(item)

    rows.sort(

        key=lambda x: (

            x.get("price") is None,

            -float(x.get("changePct") or 0),

            x.get("symbol", ""),

        )

    )

    page = max(1, int(page))

    page_size = max(1, min(200, int(page_size)))

    total = len(rows)

    start = (page - 1) * page_size

    return {

        "items": rows[start : start + page_size],

        "total": total,

        "page": page,

        "pageSize": page_size,

        "pages": max(1, (total + page_size - 1) // page_size),

    }

@app.get("/api/search")

async def search(q: str = ""):

    q = q.strip().upper()

    if not q:

        return []

    try:

        item = await quote_symbol(q)

        if item:

            return [item]

    except Exception as exc:

        print("NSE MCP search failed:", exc)

    # Fall back to catalog matching when the symbol itself is not quoted.

    ql = q.lower()

    results = []

    for key, meta in instrument_catalog.items():

        if (

            ql in meta.get("symbol", "").lower()

            or ql in meta.get("name", "").lower()

            or ql in key.lower()

        ):

            current = prices.get(key)

            item = {"key": key, **meta}

            if current:

                item.update(

                    {

                        "price": current.get("price"),

                        "previousClose": current.get("previousClose"),

                        "change": current.get("change", 0),

                        "changePct": current.get("changePct", 0),

                        "currency": current.get("currency", "INR"),

                        "source": current.get("source", "NSE MCP"),

                        "timestamp": current.get("timestamp"),

                    }

                )

            else:

                item.update(

                    {

                        "price": None,

                        "previousClose": None,

                        "change": 0,

                        "changePct": 0,

                        "currency": "INR",

                        "source": "Waiting for NSE data",

                        "timestamp": None,

                    }

                )

            results.append(item)

            if len(results) >= 30:

                break

    return results

@app.post("/api/subscribe")

async def subscribe(payload: dict):

    key = str(payload.get("key", "")).strip()

    if not key:

        raise HTTPException(400, "Instrument key is required")

    # NSE MCP search keys are supported directly.

    if key.startswith("NSE_MCP|"):

        symbol = key.split("|", 1)[1].strip().upper()

        try:

            item = await quote_symbol(symbol)

            if item:

                subscribed_keys.add(key)

                await broadcast({"type": "ticker", "item": item})

                return item

        except Exception as exc:

            print("NSE MCP subscribe failed:", exc)

        raise HTTPException(502, "Unable to fetch NSE quote")

    # Convert a catalog instrument to its trading symbol and query NSE MCP.

    meta = catalog_item(key)

    symbol = str(meta.get("symbol", "")).strip().upper()
    if meta.get("type") == "index" and symbol:
        try:
            item = await quote_symbol(symbol)

            if item:
                subscribed_keys.add(key)
                await broadcast({"type": "ticker", "item": item})
                return item

        except Exception as exc:
            print("NSE index subscribe failed:", exc)

    
    if meta.get("type") == "stock" and symbol:

        try:

            item = await quote_symbol(symbol)

            if item:

                subscribed_keys.add(key)

                subscribed_keys.add(item["key"])

                await broadcast({"type": "ticker", "item": item})

                return item

        except Exception as exc:

            print("NSE catalog subscribe failed:", exc)

    return prices.get(

        key,

        {

            "key": key,

            **meta,

            "price": None,

            "changePct": 0,

            "currency": "INR",

            "source": "Waiting for NSE data",

        },

    )

@app.get("/api/history/{key:path}")

async def get_history(key: str, interval: str = "1"):

    # Real history accumulated by MarketFlow while running.

    if key in history and len(history[key]) >= 1:

        return list(history[key])

    if key in CRYPTO_SYMBOLS:

        symbol = key.upper()

        try:

            response = await asyncio.to_thread(

                requests.get,

                f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={quote(interval)}&limit=120",

                timeout=8,

            )

            rows = response.json()

            fx = await update_fx()

            return [

                {

                    "t": int(row[0]),

                    "p": round(float(row[4]) * fx, 4),

                }

                for row in rows

            ]

        except Exception:

            return []

    # NSE MCP tools verified for this project provide current quote data,

    # not a historical candle endpoint. Do not fabricate historical prices.

    if key.startswith("NSE_MCP|"):

        symbol = key.split("|", 1)[1].strip().upper()

        try:

            item = await quote_symbol(symbol)

            if item:

                return list(history[key])

        except Exception:

            pass

    return []

@app.post("/api/alerts")

async def create_alert(payload: dict):

    symbol = str(payload.get("symbol", "")).upper()

    condition = str(payload.get("condition", "above"))

    if condition not in ("above", "below") or not symbol:

        raise HTTPException(400, "Invalid alert")

    try:

        target = float(payload.get("target"))

    except Exception:

        raise HTTPException(400, "Target must be a number")

    conn = db()

    cur = conn.execute(

        """

        INSERT INTO alerts(symbol,condition,target,created_at)

        VALUES(?,?,?,?)

        """,

        (

            symbol,

            condition,

            target,

            datetime.now().isoformat(),

        ),

    )

    conn.commit()

    alert_id = cur.lastrowid

    row = conn.execute(

        "SELECT * FROM alerts WHERE id=?",

        (alert_id,),

    ).fetchone()

    conn.close()

    return dict(row)

@app.get("/api/alerts")

async def get_alerts():

    return read_alerts()

@app.delete("/api/alerts/{aid}")

async def delete_alert(aid: int):

    conn = db()

    conn.execute("DELETE FROM alerts WHERE id=?", (aid,))

    conn.commit()

    conn.close()

    return {"ok": True}

@app.get("/api/ai/{key:path}")

async def ai_analysis(key: str):

    pts = list(history.get(key, []))

    if len(pts) < 8:

        pts = await get_history(key)

    vals = [float(item["p"]) for item in pts]

    if len(vals) < 8:

        return {

            "signal": "WAIT",

            "score": 50,

            "confidence": 20,

            "summary": (

                "Waiting for enough real price history to analyze this "

                "instrument."

            ),

            "factors": [

                "Real market data is connected when available."

            ],

        }

    current = vals[-1]

    sma5 = sum(vals[-5:]) / 5

    sma20 = sum(vals[-20:]) / min(20, len(vals))

    momentum = (

        ((current / vals[-6]) - 1) * 100

        if len(vals) >= 6 and vals[-6]

        else 0

    )

    returns = [

        ((vals[i] / vals[i - 1]) - 1) * 100

        for i in range(1, len(vals))

        if vals[i - 1]

    ]

    recent = returns[-20:]

    mean_recent = (

        sum(recent) / len(recent)

        if recent

        else 0

    )

    vol = (

        sum((value - mean_recent) ** 2 for value in recent)

        / max(1, len(recent))

    ) ** 0.5

    score = (

        50

        + max(-25, min(25, momentum * 5))

        + (10 if current > sma5 else -10)

        + (8 if sma5 > sma20 else -8)

    )

    score = max(0, min(100, score))

    signal = (

        "BULLISH"

        if score >= 65

        else "BEARISH"

        if score <= 35

        else "NEUTRAL"

    )

    confidence = min(

        95,

        int(45 + abs(score - 50) * 0.8 + min(15, len(vals) / 10)),

    )

    factors = [

        f"5-point momentum: {momentum:+.2f}%",

        (

            "Price vs short average: "

            + ("above" if current >= sma5 else "below")

            + " SMA-5"

        ),

        (

            "Short vs medium trend: SMA-5 is "

            + ("above" if sma5 >= sma20 else "below")

            + " SMA-20"

        ),

        f"Recent volatility: {vol:.2f}%",

    ]

    summary = (

        f"Market-intelligence score is {score:.0f}/100 with a "

        f"{signal.lower()} short-term signal. This is an analytical "

        "signal, not a guaranteed forecast."

    )

    return {

        "signal": signal,

        "score": round(score, 1),

        "confidence": confidence,

        "summary": summary,

        "factors": factors,

        "sma5": sma5,

        "sma20": sma20,

        "momentum": momentum,

        "volatility": vol,

        "updatedAt": int(time.time() * 1000),

    }

@app.websocket("/ws/market")

async def market_ws(websocket: WebSocket):

    await websocket.accept()

    clients.add(websocket)

    await websocket.send_text(

        json.dumps(

            {

                "type": "snapshot",

                "items": list(prices.values()),

                "status": {

                    "cryptoLive": True,

                    "nseMcpConfigured": True,

                },

            }

        )

    )

    try:

        while True:

            await websocket.receive_text()

    except WebSocketDisconnect:

        clients.discard(websocket)

    except Exception:

        clients.discard(websocket)
