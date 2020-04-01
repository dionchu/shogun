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

from sqlalchemy import create_engine, and_, or_
import mysql.connector
from sqlalchemy import Table, MetaData, select

import os
dirname = os.path.dirname(__file__)

platform_default = 'RIC'

#engine = create_engine('mysql+mysqlconnector://dev:N8WWQp+4@104.131.43.89/bbDev', echo=False)
#metadata = MetaData(engine)
#issuance = Table('IssuanceTable', metadata, autoload=True, autoload_with=engine)

_instrument_timestamp_fields = frozenset({
    'start_date',
    'end_date',
    'first_auction_date',
    'issue_date',
    'effective_date',
    'maturity_date',
})

_metadata_timestamp_fields = frozenset({
    'first_auction_date',
    'issue_date',
    'effective_date',
    'maturity_date',
})

def _convert_metadata_timestamp_fields(df):
    """
    Takes in a df of Instrument metadata columns and converts dates to pd.datetime64
    """
    dfcopy = df.copy()
    for key in _metadata_timestamp_fields:
        dfcopy[key] = pd.to_datetime(df[key])
    return dfcopy

metadata_columns = ['exchange_symbol', 'instrument_name', 'instrument_country_id',
                 'underlying_name', 'underlying_asset_class_id', 'type',
                 'face_value', 'settlement_days', 'coupon', 'day_counter',
                 'first_auction_date', 'issue_date', 'effective_date',
                 'maturity_date', 'period', 'redemption', 'settle_start',
                 'settle_end', 'settle_timezone', 'quote_currency_id',
                 'multiplier', 'tick_size', 'start_date', 'end_date', 'exchange_info', 'parent_calendar_id',
                 'child_calendar_id']

columns = ['exchange_symbol', 'instrument_name', 'instrument_country_id',
                 'underlying_name', 'underlying_asset_class_id', 'type',
                 'face_value', 'settlement_days', 'coupon', 'day_counter',
                 'first_auction_date', 'issue_date', 'effective_date',
                 'maturity_date', 'period', 'redemption', 'settle_start',
                 'settle_end', 'settle_timezone', 'quote_currency_id',
                 'multiplier', 'tick_size', 'exchange_info', 'parent_calendar_id',
                 'child_calendar_id']

treasury_instrument_df = pd.DataFrame(columns = columns)
treasury_metadata_df = pd.DataFrame(columns = metadata_columns)

class TreasuryFactory(object):
    """A Treasury Bill factory is an object that creates specific US Treasury
    instrument instances for writing into the FixedIncome table.

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
        self._engine = create_engine('mysql+mysqlconnector://dev:N8WWQp+4@104.131.43.89/bbDev', echo=False)
        self._metadata = MetaData(self._engine)
        self._issuance = Table('IssuanceTable', self._metadata, autoload=True, autoload_with=self._engine)

        self._tbill_cache = {}
        self._tbill_days = {}

    def get_outstanding_nominals(self, date, first_time = False):
        # ensure that date is a pd.Timestamp class
        if not isinstance(date,pd.Timestamp):
            date = pd.Timestamp(date)

        connection = self._engine.connect()

        if first_time:
            query = select([self._issuance]).where(and_(or_(self._issuance.columns.Type == 'NOTE',
                            self._issuance.columns.Type == 'BOND'),
                            self._issuance.columns.MaturityDate > date.strftime('%Y-%m-%d'),
                            self._issuance.columns.SecType == 'T'))
        else:
            query = select([self._issuance]).where(and_(or_(self._issuance.columns.Type == 'NOTE',
                            self._issuance.columns.Type == 'BOND'),
                            self._issuance.columns.MaturityDate > date.strftime('%Y-%m-%d'),
                            self._issuance.columns.AuctionDate < date.strftime('%Y-%m-%d'),
                            self._issuance.columns.SecType == 'T'))

        ResultProxy = connection.execute(query)
        ResultSet = ResultProxy.fetchall()
        df = pd.DataFrame(ResultSet)
        connection.close()

        df = df.rename(columns = {0: 'exchange_symbol',
                                  1: 'type',
                                  2: 'sec_type',
                                  3: 'term',
                                  4: 'coupon',
                                  5: 'amount_issued',
                                  6: 'auction_date',
                                  7: 'issue_date',
                                  8: 'maturity_date',
                                  9: 'first_int_date',
                                  10: 'dated_date',
                                  11: 'call_date'})
        # drop reissues
        df = df.loc[~df.exchange_symbol.duplicated(keep='first')]

        return df

    def construct_nominals_metadata(self, df):

        # Ensure only the first auction is retrieved
        df = df.loc[~df.exchange_symbol.duplicated(keep='first')]

        df['instrument_name'] = df['sec_type']+' '+[mat.strftime('%m/%d/%Y') for mat in df.loc[:,'maturity_date']]
        df['instrument_country_id'] = 'US'
        df['underlying_name'] = df['instrument_name']
        df['underlying_asset_class_id'] = 2
        df['settlement_days'] = 1
        df['face_value'] = 100
        df['day_counter'] = 'Act/Act'
        df['effective_date'] = df['dated_date']
        df['first_auction_date'] = df['auction_date']
        df[df.effective_date != df.effective_date]['effective_date'] == df[df.effective_date != df.effective_date]['issue_date']
        df['period'] = 'Semiannual'
        df['redemption'] = 100
        df['settle_start'] = '15:00'
        df['settle_end'] = '15:00'
        df['settle_timezone'] = 'exch'
        df['quote_currency_id'] = 'USD'
        df['multiplier'] = 0.01
        df['tick_size'] = 0.000001
        df['exchange_info'] = 'USBOND'
        df['parent_calendar_id'] = 'USBOND'
        df['child_calendar_id'] = ''

        return _convert_metadata_timestamp_fields(df[columns])

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
        if platform == "BBG":
            platform_tkr = exchange_symbol+' '+'GOVT'
        elif platform == "RIC":
            platform_tkr = exchange_symbol+'=TWEB'
        return platform_tkr
