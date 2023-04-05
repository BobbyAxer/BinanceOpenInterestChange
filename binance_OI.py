import requests
import json
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
load_dotenv()
import os
import time
import mplfinance as mpf
import asyncio
import aiohttp

api_key_binance = os.environ.get('API_B')
api_secret_binance = os.environ.get('SECRET_B')

async def get_binance_futures_tickers():
    url = 'https://fapi.binance.com/fapi/v1/ticker/24hr'
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json()
    futures_tickers = [ticker['symbol'] for ticker in data if 'USDT' in ticker['symbol']]
    return futures_tickers
async def get_data(symbol, period, limit):
    endpoint = 'https://fapi.binance.com/futures/data/openInterestHist'
    headers = {
        'X-MBX-APIKEY': api_key_binance
    }
    params = {
        'symbol': symbol,
        'period': period,
        'limit': limit
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(endpoint, headers=headers, params=params) as response:
            data = await response.json()
    return data

async def main():
    start = time.time()
    tickers = await get_binance_futures_tickers()
    print(tickers)
    data = []

    tasks = []
    for symbol in tickers:
        task = asyncio.ensure_future(get_data(symbol, '1h', 500))
        tasks.append(task)

    responses = await asyncio.gather(*tasks)
    for symbol_data in responses:
        print(symbol_data)
        for row in symbol_data:
            # row["symbol"] = symbol
            data.append(row)


    df = pd.DataFrame(data)
    # print(df)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit='ms')
    df = df.set_index("timestamp")
    df["openInterest"] = df["sumOpenInterest"].astype(float)
    print(df)
    df["1h_pct_change"] = df.groupby("symbol")["openInterest"].pct_change(periods=1)
    df["4h_pct_change"] = df.groupby("symbol")["openInterest"].pct_change(periods=4)
    df["24h_pct_change"] = df.groupby("symbol")["openInterest"].pct_change(periods=24)
    df["3d_pct_change"] = df.groupby("symbol")["openInterest"].pct_change(periods=72)
    df["7d_pct_change"] = df.groupby("symbol")["openInterest"].pct_change(periods=168)
    df["10d_pct_change"] = df.groupby("symbol")["openInterest"].pct_change(periods=240)
    df["20d_pct_change"] = df.groupby("symbol")["openInterest"].pct_change(periods=480)

    avg_oi = round(df.groupby("symbol")["openInterest"].mean(), 2)
    four_hours = round(df.groupby("symbol")["openInterest"].apply(lambda x: x.iloc[-5]), 2)
    last_oi = round(df.groupby("symbol")["openInterest"].last(), 2)
    last_day = df.groupby("symbol")["openInterest"].apply(lambda x: x.shift(24).iloc[-1]).round(2)
    three_days = df.groupby("symbol")["openInterest"].apply(lambda x: x.shift(3 * 24).iloc[-1]).round(2)
    ten_days = df.groupby("symbol")["openInterest"].apply(lambda x: x.shift(10 * 24).iloc[-1]).round(2)
    twenty_days = df.groupby("symbol")["openInterest"].apply(lambda x: x.shift(20 * 24).iloc[-1]).round(2)

    result = pd.concat([avg_oi, last_oi, four_hours, last_day, ten_days, twenty_days,
                        df.groupby("symbol")["1h_pct_change"].last(),
                        df.groupby("symbol")["4h_pct_change"].last(),
                        df.groupby("symbol")["24h_pct_change"].last(),
                        df.groupby("symbol")["3d_pct_change"].last(),
                        df.groupby("symbol")["7d_pct_change"].last(),
                        df.groupby("symbol")["10d_pct_change"].last(),
                        df.groupby("symbol")["20d_pct_change"].last()], axis=1)

    result.columns = ["avg OI", 'last OI', '4h', '1day', '10 day', '20 day',
                      '1h_pct_change', '4h_pct_change', '24h_pct_change',"3d_pct_change", '7d_pct_change', '10d_pct_change', '20d_pct_change']
    print("Top 15 symbols with highest LAST OI:")
    print(result.nlargest(15, "1h_pct_change")[[ '1h_pct_change', "24h_pct_change"]])
    print("Top 15 symbols with lowest LAST OI:")
    print(result.nsmallest(15, "1h_pct_change")[['1h_pct_change', "24h_pct_change"]])
    print("Top 15 symbols with highest 1day OI:")
    print(result.nlargest(15, "24h_pct_change")[[ '24h_pct_change', ]])
    print("Top 15 symbols with lowest last 1day OI:")
    print(result.nsmallest(15, "24h_pct_change")[['24h_pct_change',]])
    print("Top 15 symbols with highest 3day OI:")
    print(result.nlargest(15, "3d_pct_change")[["24h_pct_change", "3d_pct_change"]])
    print("Top 15 symbols with lowest last 3dday OI:")
    print(result.nsmallest(15, "3d_pct_change")[["24h_pct_change",  "3d_pct_change"]])
    print("Top 15 symbols with highest 10day OI:")
    print(result.nlargest(15, "10d_pct_change")[["24h_pct_change", "10d_pct_change"]])
    print("Top 15 symbols with lowest last 10day OI:")
    print(result.nsmallest(15, "10d_pct_change")[["24h_pct_change",  "10d_pct_change"]])
    print("Top 15 symbols with highest 20day OI:")
    print(result.nlargest(15, "20d_pct_change")[["24h_pct_change", '10d_pct_change', "20d_pct_change"]])
    print("Top 15 symbols with lowest last 20day OI:")
    print(result.nsmallest(15, "20d_pct_change")[["24h_pct_change", '10d_pct_change', "20d_pct_change"]])


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()

