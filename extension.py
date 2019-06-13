from zipline.data.bundles import register
from zipline.data.bundles.binance_api import api_to_bundle
from zipline.data.bundles.binance_csv import csv_to_bundle
from zipline.data.bundles.csvdir import csvdir_equities

register(
    'binance_api',
    api_to_bundle(interval='1d'),
    calendar_name='Binance',
)

register(
    'binance_csv',
    csv_to_bundle(reload_csv=False, interval='1d'),
    calendar_name='Binance',
)

register(
    'binance_test',
    csvdir_equities(
        ['minute'],
        '/home/bo/.zipline/custom_data',
    ),
    calendar_name='24/7'
)
