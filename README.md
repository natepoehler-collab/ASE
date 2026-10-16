# Asset Selection Engine (ASE)

A Windows desktop app (Electron) that ranks tradeable markets by how much real, tradeable movement each one made over the last year. It covers crypto, US stocks, forex, indices, commodities, metals and oil.

I'm not a software engineer by trade, but I am one in practice. I built ASE with Claude (Anthropic's AI) as my development partner: I designed every feature, tested every build, and drove each fix, and Claude wrote the code. It has gone through 100+ releases since June 2026, shipped directly to its users. Two other people use it, and I post updates to them through Discord. This repository is a snapshot of the current version (v1.30.40), not the full release history.

## What it does

- Pulls 1-minute price history for each market from public exchange feeds (Binance, Bitget, KuCoin, OKX, MEXC, Dukascopy, and massive.com/Polygon for US stocks).
- Walks that history and counts "moves": a run that only ends when price pulls back far enough to call it over. Two settings decide what counts:
  - **Pullback filter** (25/50/75/90%): how much of a run you'll sit through before calling it over.
  - **Move size**: the smallest move worth counting (for example 5%+ for crypto).
- Ranks every market in one table, so you can see which ones actually trend and which ones just chop.
- Extra tabs for EMA crosses, price zones, trend views and alerts. Alerts can go to a Discord channel.
- Keeps itself up to date: crypto refreshes every minute, stocks and forex hourly.

## Running it

Requires Node.js.

```
npm install
npm install electron@42 --save-dev
npx electron .
```

On first run it downloads its own price data, so the first launch takes a while.

## Keys and privacy

There are no API keys or passwords in this code. The optional massive.com API key (for US stock data) and the optional Discord webhook are pasted into the app by the user and stored only on that user's computer.

## Files

- `main.js`: app window, update schedule, alerts
- `engine.js`: data download and the move-counting engine
- `candles.js`, `pool.js`, `pool_worker.js`: price data storage and parallel processing
- `index.html`: the main table and tabs
- `chart.html`: chart window (uses TradingView's open-source Lightweight Charts)
- `seed_engine_data.json`, `zones_cache.json`: starting data so the table isn't empty on first launch
- `start.command`, `Update.command`: Mac launch and update scripts

The comments in the code record why each fix was made, including the user requests behind them.
