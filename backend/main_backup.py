import asyncio
import json
import os
import time
import sqlite3
import gzip
from collections import defaultdict, deque
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import quote

import requests
import websockets
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from nse_client import get_stock_quote, get_live_gainers, get_live_losers
try:
    import upstox_client
except Exception:
    upstox_client = None

load_dotenv(Path(__file__).with_name('.env'))

APP_DIR = Path(__file__).resolve().parent
DB_PATH = APP_DIR / 'marketflow.db'

app = FastAPI(title='MarketFlow Live API', version='2.0.0')
app.add_middleware(
    CORSMiddleware,
    allow_origins=['http://localhost:5173', 'http://127.0.0.1:5173'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

clients = set()
prices = {}
history = defaultdict(lambda: deque(maxlen=300))
loop_ref = None
upstox_streamer = None
usd_inr = 85.0
last_fx_update = 0.0

# Upstox maintains a daily refreshed NSE instrument master. We use it to
# discover the complete equity universe instead of hard-coding a small list.
NSE_INSTRUMENTS_URL = 'https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz'
NSE_CACHE_PATH = APP_DIR / 'nse_instruments.json'
instrument_catalog = {}
subscribed_keys = set()

CRYPTO_SYMBOLS = {
    'BTCUSDT': {'name': 'Bitcoin', 'symbol': 'BTC', 'pair': 'btcusdt', 'type': 'crypto'},
    'ETHUSDT': {'name': 'Ethereum', 'symbol': 'ETH', 'pair': 'ethusdt', 'type': 'crypto'},
    'SOLUSDT': {'name': 'Solana', 'symbol': 'SOL', 'pair': 'solusdt', 'type': 'crypto'},
}

STOCK_META = {
    'NSE_INDEX|Nifty 50': {'name': 'NIFTY 50', 'symbol': 'NIFTY', 'type': 'index'},
    'NSE_INDEX|Nifty Bank': {'name': 'NIFTY BANK', 'symbol': 'BANKNIFTY', 'type': 'index'},
    'NSE_INDEX|India VIX': {'name': 'INDIA VIX', 'symbol': 'INDIAVIX', 'type': 'index'},
    'NSE_EQ|INE002A01018': {'name': 'Reliance Industries', 'symbol': 'RELIANCE', 'type': 'stock'},
    'NSE_EQ|INE467B01029': {'name': 'Tata Consultancy Services', 'symbol': 'TCS', 'type': 'stock'},
    'NSE_EQ|INE009A01021': {'name': 'Infosys', 'symbol': 'INFY', 'type': 'stock'},
    'NSE_EQ|INE040A01034': {'name': 'HDFC Bank', 'symbol': 'HDFCBANK', 'type': 'stock'},
    'NSE_EQ|INE090A01021': {'name': 'ICICI Bank', 'symbol': 'ICICIBANK', 'type': 'stock'},
    'NSE_EQ|INE062A01020': {'name': 'State Bank of India', 'symbol': 'SBIN', 'type': 'stock'},
    'NSE_EQ|INE154A01025': {'name': 'ITC', 'symbol': 'ITC', 'type': 'stock'},
    'NSE_EQ|INE018A01030': {'name': 'Larsen & Toubro', 'symbol': 'LT', 'type': 'stock'},
    'NSE_EQ|INE030A01027': {'name': 'Hindustan Unilever', 'symbol': 'HINDUNILVR', 'type': 'stock'},
    'NSE_EQ|INE397D01024': {'name': 'Bharti Airtel', 'symbol': 'BHARTIARTL', 'type': 'stock'},
    'NSE_EQ|INE238A01034': {'name': 'Axis Bank', 'symbol': 'AXISBANK', 'type': 'stock'},
    'NSE_EQ|INE585B01010': {'name': 'Maruti Suzuki', 'symbol': 'MARUTI', 'type': 'stock'},
    'NSE_EQ|INE237A01028': {'name': 'Kotak Mahindra Bank', 'symbol': 'KOTAKBANK', 'type': 'stock'},
    'NSE_EQ|INE280A01028': {'name': 'Titan Company', 'symbol': 'TITAN', 'type': 'stock'},
    'NSE_EQ|INE044A01036': {'name': 'Sun Pharmaceutical', 'symbol': 'SUNPHARMA', 'type': 'stock'},
}

# Seed the catalog with the important instruments so the UI works even before
# the daily NSE master has been downloaded.
instrument_catalog.update(STOCK_META)

def load_nse_instruments():
    global instrument_catalog
    try:
        raw = None
        # Cache for the current process/day; refresh at most once per 6 hours.
        if NSE_CACHE_PATH.exists() and time.time() - NSE_CACHE_PATH.stat().st_mtime < 21600:
            raw = NSE_CACHE_PATH.read_bytes()
        else:
            r = requests.get(NSE_INSTRUMENTS_URL, timeout=20)
            r.raise_for_status()
            raw = gzip.decompress(r.content)
            NSE_CACHE_PATH.write_bytes(raw)
        rows = json.loads(raw)
        count = 0
        allowed_types = {'EQ', 'BE', 'BZ', 'SM', 'ST', 'SZ', 'E', 'M'}
        for row in rows:
            if row.get('segment') != 'NSE_EQ':
                continue
            if row.get('instrument_type') not in allowed_types:
                continue
            key = row.get('instrument_key')
            symbol = row.get('trading_symbol')
            if not key or not symbol:
                continue
            instrument_catalog[key] = {
                'name': row.get('short_name') or row.get('name') or symbol,
                'symbol': symbol,
                'type': 'stock',
                'exchange': 'NSE',
                'isin': row.get('isin', ''),
            }
            count += 1
        print(f'Loaded {count} NSE equity instruments from Upstox instrument master.')
    except Exception as exc:
        print('NSE instrument master load failed:', exc)

def catalog_item(key):
    meta = instrument_catalog.get(key)
    if meta:
        return meta
    return {'name': key.split('|')[-1], 'symbol': key.split('|')[-1], 'type': 'stock'}

# SQLite is only used for user-created alerts. Market data itself stays in memory.
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute('''CREATE TABLE IF NOT EXISTS alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        condition TEXT NOT NULL,
        target REAL NOT NULL,
        triggered INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL
    )''')
    conn.commit()
    return conn

def read_alerts():
    conn = db(); rows = conn.execute('SELECT * FROM alerts ORDER BY id DESC').fetchall(); conn.close()
    return [dict(r) for r in rows]

def make_item(key, meta, price, previous=None, source='live', currency='INR'):
    previous = float(previous if previous is not None else price)
    price = float(price)
    change = price - previous
    pct = (change / previous * 100) if previous else 0
    item = {
        'key': key, 'symbol': meta['symbol'], 'name': meta['name'], 'type': meta['type'],
        'price': round(price, 4), 'previousClose': round(previous, 4),
        'change': round(change, 4), 'changePct': round(pct, 4), 'currency': currency,
        'source': source, 'timestamp': int(time.time() * 1000),
    }
    prices[key] = item
    history[key].append({'t': item['timestamp'], 'p': item['price']})
    return item

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
    rows = conn.execute('SELECT * FROM alerts WHERE symbol=? AND triggered=0', (item['symbol'],)).fetchall()
    hits = []
    for row in rows:
        hit = (row['condition'] == 'above' and item['price'] >= row['target']) or (row['condition'] == 'below' and item['price'] <= row['target'])
        if hit:
            conn.execute('UPDATE alerts SET triggered=1 WHERE id=?', (row['id'],))
            hits.append({'id': row['id'], 'symbol': item['symbol'], 'name': item['name'], 'price': item['price'], 'target': row['target'], 'condition': row['condition']})
    conn.commit(); conn.close()
    return hits

async def update_fx():
    global usd_inr, last_fx_update
    if time.time() - last_fx_update < 60:
        return usd_inr
    try:
        r = await asyncio.to_thread(requests.get, 'https://open.er-api.com/v6/latest/USD', timeout=5)
        data = r.json()
        usd_inr = float(data.get('rates', {}).get('INR', usd_inr))
        last_fx_update = time.time()
    except Exception:
        pass
    return usd_inr

async def crypto_feed():
    streams = '/'.join(f"{v['pair']}@ticker" for v in CRYPTO_SYMBOLS.values())
    url = f'wss://stream.binance.com:9443/stream?streams={streams}'
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                await broadcast({'type': 'status', 'source': 'crypto', 'connected': True})
                async for raw in ws:
                    data = json.loads(raw).get('data', {})
                    pair = data.get('s')
                    if pair not in CRYPTO_SYMBOLS:
                        continue
                    fx = await update_fx()
                    meta = CRYPTO_SYMBOLS[pair]
                    # Binance ticker values are USD/USDT. Convert to INR for the UI.
                    price_usd = float(data.get('c', 0))
                    open_usd = float(data.get('o', price_usd))
                    price_inr = price_usd * fx
                    open_inr = open_usd * fx
                    item = make_item(pair, meta, price_inr, open_inr, 'Binance live WebSocket', 'INR')
                    item['usdPrice'] = round(price_usd, 6)
                    item['fxUsdInr'] = round(fx, 4)
                    await broadcast({'type': 'ticker', 'item': item})
                    for hit in check_alerts(item):
                        await broadcast({'type': 'alert', 'item': hit})
        except Exception as exc:
            await broadcast({'type': 'status', 'source': 'crypto', 'connected': False, 'error': str(exc)})
            await asyncio.sleep(5)


def parse_ltpc(feed):
    if not isinstance(feed, dict):
        return None, None
    # SDK may return ltpc directly or under a nested structure.
    if isinstance(feed.get('ltpc'), dict):
        x = feed['ltpc']; return x.get('ltp'), x.get('cp')
    for value in feed.values():
        if isinstance(value, dict):
            ltp, cp = parse_ltpc(value)
            if ltp is not None:
                return ltp, cp
    return None, None

def start_upstox(main_loop):
    global upstox_streamer, subscribed_keys
       # NSE MCP provides current quotes, not historical candles.
    # Use real price points collected by MarketFlow while running.
    try:
        if key in history and len(history[key]) > 0:
            return list(history[key])

        # Try to get one real NSE price for an NSE MCP search key.
        if key.startswith('NSE_MCP|'):
            symbol = key.split('|', 1)[1].strip().upper()

            result = await get_stock_quote(symbol)

            data = None

            for part in getattr(result, 'content', []) or []:
                raw = getattr(part, 'text', None)

                if raw:
                    try:
                        data = json.loads(raw)
                        break
                    except Exception:
                        continue

            if isinstance(data, dict):
                quote_data = data.get('data', data)

                if isinstance(quote_data, dict) and symbol in quote_data:
                    quote_data = quote_data[symbol]

                if isinstance(quote_data, dict):
                    if (
                        'lastTradedPrice' not in quote_data
                        and len(quote_data) == 1
                    ):
                        quote_data = next(iter(quote_data.values()))

                if isinstance(quote_data, dict):
                    ltp = quote_data.get('lastTradedPrice')

                    if ltp is not None:
                        point = {
                            't': int(time.time() * 1000),
                            'p': float(ltp)
                        }

                        history[key].append(point)

                        return list(history[key])

        return []

    except Exception as exc:
        print('NSE history lookup failed:', exc)
        return []

@app.post('/api/alerts')
async def create_alert(payload: dict):
    symbol = str(payload.get('symbol', '')).upper()
    condition = str(payload.get('condition', 'above'))
    if condition not in ('above', 'below') or not symbol:
        raise HTTPException(400, 'Invalid alert')
    try: target = float(payload.get('target'))
    except Exception: raise HTTPException(400, 'Target must be a number')
    conn = db(); cur = conn.execute('INSERT INTO alerts(symbol,condition,target,created_at) VALUES(?,?,?,?)', (symbol, condition, target, datetime.now().isoformat())); conn.commit(); aid = cur.lastrowid
    row = conn.execute('SELECT * FROM alerts WHERE id=?', (aid,)).fetchone(); conn.close()
    return dict(row)

@app.get('/api/alerts')
async def get_alerts(): return read_alerts()

@app.delete('/api/alerts/{aid}')
async def delete_alert(aid: int):
    conn = db(); conn.execute('DELETE FROM alerts WHERE id=?', (aid,)); conn.commit(); conn.close(); return {'ok': True}

@app.get('/api/ai/{key:path}')
async def ai_analysis(key: str):
    pts = list(history.get(key, []))
    if len(pts) < 8:
        pts = await get_history(key)
    vals = [float(x['p']) for x in pts]
    if len(vals) < 8:
        return {'signal':'WAIT','score':50,'confidence':20,'summary':'Waiting for enough live price history to analyze this instrument.','factors':['Live data is connected when available.']}
    current = vals[-1]
    sma5 = sum(vals[-5:])/5
    sma20 = sum(vals[-20:])/min(20,len(vals))
    momentum = ((current / vals[-6]) - 1) * 100 if len(vals) >= 6 and vals[-6] else 0
    returns = [((vals[i]/vals[i-1])-1)*100 for i in range(1,len(vals)) if vals[i-1]]
    vol = (sum((x - sum(returns[-20:])/min(20,len(returns)))**2 for x in returns[-20:]) / max(1,min(20,len(returns)))) ** 0.5 if returns else 0
    score = 50 + max(-25,min(25,momentum*5)) + (10 if current > sma5 else -10) + (8 if sma5 > sma20 else -8)
    score = max(0,min(100,score))
    signal = 'BULLISH' if score >= 65 else 'BEARISH' if score <= 35 else 'NEUTRAL'
    confidence = min(95, int(45 + abs(score-50)*0.8 + min(15,len(vals)/10)))
    factors = [
        f'5-point momentum: {momentum:+.2f}%',
        f'Price vs short average: {"above" if current >= sma5 else "below"} SMA-5',
        f'Short vs medium trend: SMA-5 is {"above" if sma5 >= sma20 else "below"} SMA-20',
        f'Recent volatility: {vol:.2f}%',
    ]
    summary = f'AI market-intelligence score is {score:.0f}/100 with a {signal.lower()} short-term signal. This is an analytical signal, not a guaranteed forecast.'
    return {'signal':signal,'score':round(score,1),'confidence':confidence,'summary':summary,'factors':factors,'sma5':sma5,'sma20':sma20,'momentum':momentum,'volatility':vol,'updatedAt':int(time.time()*1000)}

@app.websocket('/ws/market')
async def market_ws(websocket: WebSocket):
    await websocket.accept(); clients.add(websocket)
    await websocket.send_text(json.dumps({'type':'snapshot','items':list(prices.values()),'status':{'cryptoLive':True,'upstoxConfigured':bool(os.getenv('UPSTOX_ACCESS_TOKEN'))}}))
    try:
        while True: await websocket.receive_text()
    except WebSocketDisconnect: clients.discard(websocket)
    except Exception: clients.discard(websocket)
