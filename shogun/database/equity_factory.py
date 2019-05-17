import warnings
import trading_calendars
from datetime import date, datetime, timedelta
import pandas as pd
from itertools import chain
from functools import partial

from pandas.errors import PerformanceWarning
from pandas.tseries.offsets import *
from pandas.tseries.holiday import (
    DateOffset,
    MO,
    TU,
    WE,
    TH,
    FR,
    SA,
    SU,
)

from sqlalchemy import create_engine, and_
import mysql.connector
from sqlalchemy import Table, MetaData, select

import os
#dirname = os.path.dirname(__file__)
from shogun.DIRNAME import dirname

platform_default = 'RIC'
default_start = pd.Timestamp('2000-01-01')

#engine = create_engine('mysql+mysqlconnector://dev:N8WWQp+4@104.131.43.89/bbDev', echo=False)
#metadata = MetaData(engine)
#issuance = Table('IssuanceTable', metadata, autoload=True, autoload_with=engine)

_instrument_timestamp_fields = frozenset({
    'start_date',
    'end_date',
})

_dividend_timestamp_fields = frozenset({
    'ex_date',
    'record_date',
    'pay_date'
})

metadata_columns = ['exchange_symbol', 'instrument_name', 'instrument_country_id',
                 'underlying_name', 'underlying_asset_class_id', 'type',
                 'settle_start', 'settle_end', 'settle_timezone', 'quote_currency_id',
                 'multiplier', 'tick_size', 'start_date', 'end_date', 'exchange_id',
                 'parent_calendar_id', 'child_calendar_id']

columns = ['exchange_symbol', 'instrument_name', 'instrument_country_id',
                 'underlying_name', 'underlying_asset_class_id', 'type',
                 'settle_start', 'settle_end', 'settle_timezone', 'quote_currency_id',
                 'multiplier', 'tick_size',  'exchange_id', 'parent_calendar_id',
                 'child_calendar_id']

equity_instrument_df = pd.DataFrame(columns = columns)
equity_metadata_df = pd.DataFrame(columns = metadata_columns)

class EquityFactory(object):
    """An Equity factory is an object that creates specific Equity
    instrument instances for writing into the Equity table.
    Parameters
    ----------
    root_symbol : str or None
        The future root id, for example 'ZN' or 'ES'.
    canonical_name : str
        The canonical name of the exchange, for example 'XNYS' or 'XASX'. If
        None this will be the same as the name.
    financial_center : str
        The financial center where exchange is located
    """

    def __init__(self):
        self._country_code = pd.read_csv(dirname + "\_CountryCode.csv", keep_default_na=False)
        self._asset_class = pd.read_csv(dirname + "\_AssetClass.csv")
        self._currency_code = pd.read_csv(dirname + "\_CurrencyCode.csv")
        self._exchange_code = pd.read_csv(dirname + "\_ExchangeCode.csv")
        self._financial_center = pd.read_csv(dirname + "\_FinancialCenter.csv")
        self._platform_symbol_mapping = pd.read_csv(dirname + "\_PlatformSymbolMapping.csv")
        self._traded_equities = pd.read_csv(dirname + "\_TradedEquityTable.csv")

    def get_traded_equities(self):
        return self._traded_equities

    def construct_us_equity_metadata(self, df):

            df['settle_start'] = '16:00'
            df['settle_end'] = '16:00'
            df['settle_method'] = 'auction'
            df['settle_timezone'] = 'exch'
            df['quote_currency_id'] = 'USD'
            df['multiplier'] = 1
            df['tick_size'] = 0.01
            df['start_date'] = default_start
            df['end_date'] = default_start
            return df

    def exchange_symbol_to_ticker(self, exchange_symbol, platform = platform_default):
        """
        Calculate platform tickers for given exchange symbol
        DataFrame
        Parameters
        ----------
        exchange_symbol : str
            exchange_symbol to find reference day for
        platform : str
            platform for which to obtain ticker
        """
        platform_symbol = self._platform_symbol_mapping[
                (self._platform_symbol_mapping['exchange_symbol'] == exchange_symbol) & \
                (self._platform_symbol_mapping['platform'] == platform)
                ].set_index('exchange_symbol').to_dict()['platform_symbol'][exchange_symbol]

        platform_tkr = platform_symbol
        return platform_tkr