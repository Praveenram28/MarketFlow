# MarketFlow — Live Market Intelligence Website

A colorful React + FastAPI market dashboard designed around real market-data APIs. It is an original UI, not a copy of any brokerage brand.

## Main features

- Full NSE equity instrument discovery from the Upstox BOD instrument master
- Real-time NSE/indices LTP through Upstox Market Data Feed V3
- Search the complete discovered NSE catalog (for example, MEESHO)
- Dynamic subscription for searched/watchlisted instruments
- Trending stocks sorted by live percentage change
- Top gainers and losers
- NIFTY 50, NIFTY BANK and India VIX
- Live BTC/ETH/SOL with USD-to-INR conversion
- Watchlist saved in browser local storage
- Automatic price alerts backed by SQLite
- Historical price chart when the API entitlement provides candles
- Rule-based AI market-intelligence analysis using momentum, SMA and volatility
- Responsive colorful UI with working Home, Stocks, Indices, Crypto, Watchlist and Alerts navigation

## Important market-data note

The website does not invent stock prices. NSE prices are supplied by Upstox when the user has valid market-data access. Upstox currently documents a 5,000-instrument LTPC individual limit for a connection. The app therefore uses the complete discovered NSE catalog for search and live subscription, while the feed handles the applicable subscription limit. Searched instruments can be subscribed dynamically.

## Setup on Windows

### 1. Backend

```powershell
cd backend
python -m venv venv
.\venv\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item .env.example .env
```

Edit `backend/.env` and add your own Upstox access token. Never paste the token into chat or commit it to GitHub.

Start the backend:

```powershell
.\venv\Scripts\python.exe -m uvicorn main:app --reload --port 8000
```

Health check:

`http://127.0.0.1:8000/api/health`

### 2. Frontend

Open a second terminal:

```powershell
cd frontend
npm.cmd install
npm.cmd run dev
```

Open `http://localhost:5173`.

## PowerShell note

If PowerShell blocks `npm.ps1` or `Activate.ps1`, use the commands above with `npm.cmd` and the venv Python executable directly.

## Data flow

Upstox instrument master → FastAPI catalog → Upstox WebSocket LTP → MarketFlow React UI.

Crypto uses Binance public WebSocket data and a live USD/INR FX lookup.

## AI analysis

The AI card is a market-intelligence module, not a guaranteed-profit or investment recommendation. It calculates short-term momentum, SMA-5/SMA-20 relationship and recent volatility from available price history.
