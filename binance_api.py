import bs4 as bs
from binance.client import Client
import csv
from datetime import datetime as dt
from datetime import timedelta
import numpy as np
from os import listdir, mkdir, remove
from os.path import exists, isfile, join
from pathlib import Path
import pandas as pd
import pickle
import requests
from trading_calendars import register_calendar
from trading_calendars.exchange_calendar_binance import BinanceExchangeCalendar

user_home = str(Path.home())
custom_data_path = join(user_home, '.zipline/custom_data')


def tickers():
    """
    Save Binance trading pair tickers to a pickle file
    Return a list of trading ticker pairs
    """
    cmc_binance_url = 'https://coinmarketcap.com/exchanges/binance/'
    response = requests.get(cmc_binance_url)
    if response.ok:
        soup = bs.BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'id': 'exchange-markets'})
        ticker_pairs = []

        for row in table.findAll('tr')[1:]:
            ticker_pair = row.findAll('td')[2].text
            ticker_pairs.append(ticker_pair.strip().replace('/', ''))

    if not exists(custom_data_path):
        mkdir(custom_data_path)

    with open(join(custom_data_path, 'binance_ticker_pairs.pickle'), 'wb') as f:
        pickle.dump(ticker_pairs, f)

    return ticker_pairs


def tickers_generator():
    """
    Return a tuple (sid, ticker_pair)
    """
    tickers_file = join(custom_data_path, 'binance_ticker_pairs.pickle')
    if not isfile(tickers_file):
        ticker_pairs = tickers()

    else:
        with open(tickers_file, 'rb') as f:
            ticker_pairs = pickle.load(f)[:]

    return (tuple((sid, ticker)) for sid, ticker in enumerate(ticker_pairs))


def df_generator(interval):
    client = Client("", "")
    start = '2017-7-14'  # Binance launch date
    end = dt.utcnow().strftime('%Y-%m-%d')  # Current day

    for item in tickers_generator():

        sid = item[0]
        ticker_pair = item[1]
        df = pd.DataFrame(
            columns=['date', 'open', 'high', 'low', 'close', 'volume'])

        symbol = ticker_pair
        asset_name = ticker_pair
        exchange = 'Binance'

        klines = client.get_historical_klines_generator(
            ticker_pair, interval, start, end)

        for kline in klines:
            line = kline[:]
            del line[6:]
            # Make a real copy of kline
            # Binance API forbids the change of open time
            line[0] = np.datetime64(line[0], 'ms')
            line[0] = pd.Timestamp(line[0], 'ms')
            df.loc[len(df)] = line

        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        df = df.astype({'open': 'float64', 'high': 'float64',
                        'low': 'float64', 'close': 'float64', 'volume': 'float64'})

        start_date = df.index[0]
        end_date = df.index[-1]
        first_traded = start_date
        auto_close_date = end_date + pd.Timedelta(days=1)

        # Check if there is any missing session; skip the ticker pair otherwise
        if interval == '1d' and len(df.index) - 1 != pd.Timedelta(end_date - start_date).days:
            # print('Missing sessions found in {}. Skip importing'.format(ticker_pair))
            continue
        elif interval == '1m' and timedelta(minutes=(len(df.index) + 60)) != end_date - start_date:
            # print('Missing sessions found in {}. Skip importing'.format(ticker_pair))
            continue

        yield (sid, df), symbol, asset_name, start_date, end_date, first_traded, auto_close_date, exchange


def metadata_df():
    metadata_dtype = [
        ('symbol', 'object'),
        ('asset_name', 'object'),
        ('start_date', 'datetime64[ns]'),
        ('end_date', 'datetime64[ns]'),
        ('first_traded', 'datetime64[ns]'),
        ('auto_close_date', 'datetime64[ns]'),
        ('exchange', 'object'), ]
    metadata_df = pd.DataFrame(
        np.empty(len(tickers()), dtype=metadata_dtype))

    return metadata_df


def api_to_bundle(interval='1m'):

    def ingest(environ,
               asset_db_writer,
               minute_bar_writer,
               daily_bar_writer,
               adjustment_writer,
               calendar,
               start_session,
               end_session,
               cache,
               show_progress,
               output_dir
               ):

        metadata = metadata_df()

        def minute_data_generator():
            return (sid_df for (sid_df, *metadata.iloc[sid_df[0]]) in df_generator(interval='1m'))

        def daily_data_generator():
            return (sid_df for (sid_df, *metadata.iloc[sid_df[0]]) in df_generator(interval='1d'))

        if interval == '1d':
            daily_bar_writer.write(
                daily_data_generator(), show_progress=True)
        elif interval == '1m':
            minute_bar_writer.write(
                minute_data_generator(), show_progress=True)

        # Drop the ticker rows which have missing sessions in their data sets
        metadata.dropna(inplace=True)

        asset_db_writer.write(equities=metadata)
        print(metadata)

    return ingest


register_calendar('Binance_api', BinanceExchangeCalendar())
