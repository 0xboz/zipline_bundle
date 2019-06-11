import bs4 as bs
from binance.client import Client
import csv
from datetime import datetime as dt
import numpy as np
from os import listdir, mkdir, remove
from os.path import exists, isfile, join
from pathlib import Path
import pandas as pd
import pickle
import requests
from trading_calendars import register_calendar
from trading_calendars.exchange_calendar_binance import BinanceExchangeCalendar

# Set up the directories where we are going to save those csv files
user_home = str(Path.home())
csv_data_path = join(user_home, '.zipline/custom_data/csv')
custom_data_path = join(user_home, '.zipline/custom_data')


def tickers():
    """
    Save Binance trading pair tickers to a pickle file
    Return a pickle
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


def save_csv(reload_tickers=True, interval='1m'):
    """
    Save Zipline bundle ready csv for Binance trading ticker pair
    :param reload_tickers: True or False
    :type reload_tickers: boolean
    :param interval: Default 1m. 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
    :type interval: str
    """

    if not exists(csv_data_path):
        mkdir(csv_data_path)

    if reload_tickers:
        ticker_pairs = tickers()
    else:
        ticker_pickle = join(
            custom_data_path, 'binance_ticker_pairs.pickle')
        with open(ticker_pickle, 'rb') as f:
            ticker_pairs = pickle.load(f)

    client = Client("", "")
    start = '2017-7-14'  # Binance launch date
    end = dt.utcnow().strftime('%Y-%m-%d')  # Current day
    csv_filenames = [csv_filename for csv_filename in listdir(
        csv_data_path) if isfile(join(csv_data_path, csv_filename))]

    for ticker_pair in ticker_pairs:
        filename = "Binance_{}_{}.csv".format(ticker_pair, interval)

        if csv_filenames != [] and filename in csv_filenames:
            remove(join(csv_data_path, filename))

        output = join(csv_data_path, filename)
        klines = client.get_historical_klines_generator(
            ticker_pair, interval, start, end)
        for index, kline in enumerate(klines):
            with open(output, 'a+') as f:
                writer = csv.writer(f)
                if index == 0:
                    writer.writerow(
                        ['date', 'open', 'high', 'low', 'close', 'volume'])
                # Make a real copy of kline
                # Binance API forbids the change of open time
                line = kline[:]
                del line[6:]
                line[0] = np.datetime64(line[0], 'ms')
                line[0] = pd.Timestamp(line[0], 'ms')
                writer.writerow(line)

        print('{} saved.'.format(filename))

    return [file for file in listdir(csv_data_path) if isfile(join(csv_data_path, file))]


def csv_to_bundle(reload_tickers=True, reload_csv=True, interval='1m'):

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

        if reload_csv:
            csv_filenames = save_csv(
                reload_tickers=reload_tickers, interval=interval)
        else:
            csv_filenames = [file for file in listdir(
                csv_data_path) if isfile(join(csv_data_path, file))]

        ticker_pairs = [{'exchange': pair.split('_')[0],
                         'symbol': pair.split('_')[1],
                         'interval':pair.split('_')[2].split('.')[0],
                         'file_path':join(csv_data_path, pair)}
                        for pair in csv_filenames]

        metadata_dtype = [
            ('symbol', 'object'),
            ('asset_name', 'object'),
            ('start_date', 'datetime64[ns]'),
            ('end_date', 'datetime64[ns]'),
            ('first_traded', 'datetime64[ns]'),
            ('auto_close_date', 'datetime64[ns]'),
            ('exchange', 'object'), ]
        metadata = pd.DataFrame(
            np.empty(len(ticker_pairs), dtype=metadata_dtype))

        minute_data_sets = []
        daily_data_sets = []

        for sid, ticker_pair in enumerate(ticker_pairs):
            df = pd.read_csv(ticker_pair['file_path'],
                             index_col=['date'],
                             parse_dates=['date'])

            symbol = ticker_pair['symbol']
            asset_name = ticker_pair['symbol']
            start_date = df.index[0]
            end_date = df.index[-1]
            first_traded = start_date
            auto_close_date = end_date + pd.Timedelta(days=1)
            exchange = ticker_pair['exchange']

            # Update metadata
            metadata.iloc[sid] = symbol, asset_name, start_date, end_date, first_traded, auto_close_date, exchange

            if ticker_pair['interval'] == '1m':
                minute_data_sets.append((sid, df))

            if ticker_pair['interval'] == '1d':
                daily_data_sets.append((sid, df))

        if minute_data_sets != []:
            # Dealing with missing sessions in some data sets
            for daily_data_set in daily_data_sets:
                try:
                    minute_bar_writer.write(
                        [daily_data_set], show_progress=True)
                except Exception as e:
                    print(e)

        if daily_data_sets != []:
            # Dealing with missing sessions in some data sets
            for daily_data_set in daily_data_sets:
                try:
                    daily_bar_writer.write(
                        [daily_data_set], show_progress=True)
                except Exception as e:
                    print(e)

        metadata['exchange'] = "Binance"
        asset_db_writer.write(equities=metadata)
        print(metadata)

    return ingest


register_calendar('Binance_csv', BinanceExchangeCalendar())
