# Top Analyst Picks 📈

A full-stack stock analysis tool that tracks S&P 500 analyst recommendations, YoY performance, and ranks stocks by analyst consensus quality.

---

## Getting Started

### Run the app
```bash
cd "Top Analyst Picks"
./run.sh
```

Then open your browser to: **http://localhost:8000**

> **First load takes 3–5 minutes** — the app fetches live analyst data for all ~500 S&P 500 stocks from Yahoo Finance. Data is cached for 1 hour after that, so subsequent loads are instant.

### Stop the app
Press `Ctrl+C` in the terminal.

### If you get "Address already in use"
```bash
lsof -ti:8000 | xargs kill -9 && ./run.sh
```

---

## Features

### Dashboard
- **Top 20 stocks** ranked by composite score (YoY gain × analyst quality × analyst confidence)
- **Sector filter** — narrow down to any of the 11 GICS sectors
- **Sort options** — by Composite Score, YoY Gain, # Analysts, or Upside Potential
- **Min Analysts filter** — only show stocks with a minimum number of analyst opinions
- **30-day sparkline** trend chart for each stock
- **Refresh button** — clears the cache and re-fetches fresh data

### Search
Search and filter across all S&P 500 stocks by:
- **Ticker or company name** (e.g. "AAPL" or "Apple")
- **Analyst firm** (e.g. "Goldman", "Morgan Stanley", "JP Morgan")
- **Number of analysts** (min and/or max)
- **Recommendation** (Strong Buy, Buy, Hold, Underperform, Sell)
- **YoY Gain %** (minimum threshold)
- **Sector**

### Analyst Firms
- Ranked list of Wall Street firms by number of S&P 500 stocks covered
- Shows average YoY gain of each firm's covered stocks
- Click any firm to search all their recommendations

### Sector Explorer
- Visual grid of all 11 GICS sectors
- Click any sector to see its top analyst-recommended stocks

### Stock Detail (click any row)
- 2-year price history chart
- Key statistics (Market Cap, P/E, EPS, Beta, 52W High/Low, etc.)
- Analyst price target range bar (Low → Mean → High vs. current price)
- Full recommendation history by firm with price targets and grade changes

---

## Ranking Algorithm

Stocks are ranked by a **composite score** that rewards both strong performance and strong analyst backing:

```
score = YoY_gain × (1 + analyst_quality × analyst_confidence)
```

- `analyst_quality` — 1.0 for Strong Buy, 0.5 for Hold, 0.0 for Sell
- `analyst_confidence` — log-scaled by number of analysts (saturates at ~60 analysts)

This means a stock with 50% YoY gain and a Strong Buy from 50 analysts ranks significantly higher than a stock with the same gain but no analyst coverage.

---

## API Endpoints

The backend API is also accessible directly. Full docs at **http://localhost:8000/docs**

| Endpoint | Description |
|---|---|
| `GET /api/top20` | Top N stocks (params: `sector`, `sort_by`, `min_analysts`, `limit`) |
| `GET /api/search` | Advanced search (params: `q`, `analyst`, `min_analysts`, `max_analysts`, `sector`, `recommendation`, `min_yoy`, `max_yoy`) |
| `GET /api/stock/{ticker}` | Full detail for a single stock |
| `GET /api/analysts` | Analyst firm coverage stats |
| `GET /api/sectors` | List of available sectors |
| `GET /api/status` | Server and cache status |
| `POST /api/refresh` | Clear cache to force fresh data fetch |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Backend | Python, FastAPI, Uvicorn |
| Data | Yahoo Finance via yfinance + curl_cffi |
| Frontend | Vanilla JS, Tailwind CSS (CDN), Chart.js |
| Universe | S&P 500 (sourced from Wikipedia) |

---

## Project Structure

```
Top Analyst Picks/
├── main.py          ← Backend API server
├── requirements.txt ← Python dependencies
├── run.sh           ← One-command startup script
└── static/
    └── index.html   ← Single-page frontend app
```

---

## Notes

- Data is sourced live from **Yahoo Finance** (free, no API key required)
- All analyst data is cached in memory for **1 hour** to avoid rate limits
- The S&P 500 constituent list is cached for **24 hours**
- Yahoo Finance may occasionally rate-limit requests — the app retries automatically and skips stocks it cannot reach
