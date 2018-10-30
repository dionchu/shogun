import warnings
import trading_calendars
from datetime import date, datetime, timedelta
import pandas as pd
from itertools import chain

from .future_contract_day import FutureContractDay
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


start_default = pd.Timestamp('1990-01-01', tz='UTC')
end_base = pd.Timestamp('today', tz='UTC')
end_default = end_base + pd.Timedelta(days=31)
platform_default = 'RIC'

MONTH_CODE = pd.Series(["F","G","H","J","K","M","N","Q","U","V","X","Z"], index=[1,2,3,4,5,6,7,8,9,10,11,12])
MONTH_CODE_WRAP = pd.Series(
    ["F","G","H","J","K","M","N","Q","U","V","X","Z","F","G","H","J","K","M","N","Q","U","V","X","Z"],
    index=[x for x in range(1,25)])
SERIAL_CODE = ["F","G","J","K","N","Q","V","X"]

import os
dirname = os.path.dirname(__file__)


def get_front_month_suffix(listed_months, date, offset = 0):

    idx = MONTH_CODE_WRAP.isin(listed_months)

    contract_month_filtered = MONTH_CODE_WRAP[idx]
    contract_year_filtered = pd.Series(list(chain(
                    [date.year + 0] * 12, [date.year + 1] * 12)),
                    index = [x for x in range(1,25)])[idx]

    filtered_idx = contract_month_filtered.index.get_loc(date.month,method='bfill')

    front_month_code = contract_month_filtered.iloc[filtered_idx + offset]
    front_month_year = contract_year_filtered.iloc[filtered_idx + offset]

    return front_month_code + str(front_month_year)[-2:]

def suffix_to_date(suffix):
    mo = MONTH_CODE[MONTH_CODE == suffix[0]].index.values[0]
    yr = 2000+int(suffix[-2:])
    return pd.Timestamp(yr,mo,1)

def query_df(df, eq_conditions = None, ne_conditions = None):
    """A wrapper function to simplify DataFrame queries

    Parameters
    ----------
    df: DataFrame object
        The DataFrame object being queried
    conditions: Python dictionary
        A dictionary mapping key values: column names,
        to values: target values
    """
    count = 0
    query = ""
    if eq_conditions:
        for k, v in eq_conditions.items():
            if count == 0:
                query += '(df[\'' + k + '\'] == \'' + v + '\')'
            else:
                query += ' & ' + '(df[\'' + k + '\'] == \'' + v + '\')'
            count += 1
    if ne_conditions:
        for k, v in ne_conditions.items():
            if count == 0:
                query += '(df[\'' + k + '\'] != \'' + v + '\')'
            else:
                query += ' & ' + '(df[\'' + k + '\'] != \'' + v + '\')'
            count += 1
    return eval('df[' + query + ']')


class FutureRootFactory(object):
    """A future root factory is an object that creates specific futures
    instrument instances for writing into the Futures table.

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
    ## what happens if there are duplicate roots?
    def __init__(self):
        self._country_code = pd.read_csv(dirname + "\_CountryCode.csv", keep_default_na=False)
        self._asset_class = pd.read_csv(dirname + "\_AssetClass.csv")
        self._currency_code = pd.read_csv(dirname + "\_CurrencyCode.csv")
        self._exchange_code = pd.read_csv(dirname + "\_ExchangeCode.csv")
        self._financial_center = pd.read_csv(dirname + "\_FinancialCenter.csv")
        self._future_contract_listing = pd.read_csv(dirname + "\_FutureRootContractListingTable.csv")
        self._future_root = pd.read_csv(dirname + "\_FutureRootTable.csv")
        self._platform_symbol_mapping = pd.read_csv(dirname + "\_PlatformSymbolMapping.csv")
        self._future_calendar_rules = pd.read_csv(dirname + "\_FutureRootContractCalendarRules.csv")

        self._root_cache = {}
        self._root_contract_days = {}
        self._root_contract_listing = {}

    def retrieve_root_info(self, root_symbol=None):
        """
        Retrieve root metadata for a given root symbol.
        """

        # Fastpath for empty request.
        if not root_symbol:
            return [{}]

        cache = self._root_cache
        root_dict = self._future_root[self._future_root['root_symbol']==root_symbol].to_dict(orient='records')
        cache[root_symbol] = root_dict
        return root_dict

    def make_root_chain(self, root_symbol, start=None, end=None, platform=platform_default):
        """
        Generate root chain for a given root symbol and range.
        """

        # Fastpath for empty request.
        if not root_symbol:
            return None

        if start is None:
            start = start_default

        if end is None:
            end = pd.Timestamp(self.get_contract_listing(root_symbol,date.today()).index[-1],tz='UTC')

        exchange_id = self._future_root[
            self._future_root['root_symbol'] == root_symbol
            ].set_index('root_symbol').to_dict()['parent_calendar_id'][root_symbol]

        root_contract_df = self._future_contract_listing[
                (self._future_contract_listing['root_symbol'] == root_symbol)
                ]

        listed_contracts = list(root_contract_df['delivery_month'])

        # If contract listings include serials, create monthly series, other quarterly
        if 'M' in list(root_contract_df['frequency']):
            date_list = pd.date_range(start, end, freq='M')
        else:
            date_list = pd.date_range(start, end, freq='Q')

        merge = pd.DataFrame({'month': date_list.month, 'year': date_list.year}, index=date_list)

        # Map months to month code
        merge['month'] = merge['month'].map(MONTH_CODE)

        # Filter for only those months listed
        idx = merge['month'].isin(listed_contracts)
        merge = merge[idx]; date_list = date_list[idx]

        # Generate instrument names
        symbol_df = self.get_platform_tickers(root_symbol,merge,platform_default)
        symbol_df.reset_index(drop=True,inplace=True)

        # Set up calendar and date rules
        self.make_future_contract_day(root_symbol)

        contract_day_list_dict = {}

        for k, v in self._root_contract_days[root_symbol].items():
            contract_day_list_dict[k] = self._root_contract_days[root_symbol][k].dates(date_list)

        contract_day_df = pd.DataFrame.from_dict(contract_day_list_dict)

        return pd.concat([symbol_df,contract_day_df],axis=1)


    def calculate_contract_days(self, exchange_symbols):
        # split symbols into root_symbol and month-year
        split_df = pd.DataFrame([x.split("_") for x in exchange_symbols])

        root_groups = split_df.groupby(split_df.columns[0])

        out=pd.DataFrame()
        for root_symbol in split_df.groupby(split_df.columns[0]).groups:

            self.make_future_contract_day(root_symbol)

            grouped_split_df = root_groups.get_group(root_symbol).reset_index(drop=True)
            date_list = pd.DatetimeIndex(grouped_split_df.iloc[:,1].apply(suffix_to_date))

            contract_day_list_dict = {}
            for k, v in self._root_contract_days[root_symbol].items():
                contract_day_list_dict[k] = self._root_contract_days[root_symbol][k].dates(date_list)

            contract_day_df = pd.DataFrame.from_dict(contract_day_list_dict)
            out = out.append(pd.concat([grouped_split_df.loc[:,0]+"_"+grouped_split_df.loc[:,1],contract_day_df], axis=1))
        out.rename(columns = {0: 'exchange_symbol'}, inplace=True)
        out.reset_index(drop=True,inplace=True)
        return out

    def get_contract_listing(self, root_symbol, date):
        # ensure that date is a pd.Timestamp class
        if not isinstance(date,pd.Timestamp):
            date = pd.Timestamp(date)

        # if already cached, bail early
        try:
            out = self._root_contract_listing[(date, root_symbol)]
            print('contract listing found: reading from cache')
            return out
        except KeyError:
            print('contract listing not found: constructing list')
            # get listing rules for root_symbol
            df = query_df(self._future_contract_listing, {'root_symbol': root_symbol})

            # Set up calendar and date rules
            self.make_future_contract_day(root_symbol)

            mask_groups = df.groupby(['front_month_reference_mask'])
            for mask in mask_groups.groups:
                if eval(mask):
                    reference_date = self.listing_reference_date(
                    date,
                    root_symbol,
                    mask,
                    df.front_month_offset.values[0],
                    eval(df.reference_month_offset.values[0]),
                    eval(df.first_trade.values[0])
                    )
                else:
                    reference_date = self.listing_reference_date(
                    date,
                    root_symbol,
                    df.delivery_month,
                    df.front_month_offset.values[0],
                    eval(df.reference_month_offset.values[0]),
                    eval(df.first_trade.values[0])
                    )

                    grouped_df = mask_groups.get_group(mask).groupby(['periods','frequency'])
                    filtered_df = pd.DataFrame()
                    for i in grouped_df.groups:
                        date_range = pd.date_range(start=reference_date, periods=i[0], freq=i[1])
                        merge = pd.DataFrame({'month': date_range.month, 'year': date_range.year}, index=date_range)
                        merge['month'] = merge['month'].map(MONTH_CODE)
                        filtered_df = filtered_df.append(merge[merge['month'].isin(grouped_df.get_group(i)['delivery_month'])])

            # create mask to filter out contracts where expiration is before contract month
            last_trade_dates = self._root_contract_days[root_symbol]['last_trade'].dates(
                                     filtered_df.index
                                     )

            out = self.get_platform_tickers(root_symbol,filtered_df[last_trade_dates >= date], platform_default)

            self._root_contract_listing[(date, root_symbol)] = out
            return out

    def make_future_contract_day(self, root_symbol):
        # if already cached, bail early
        try:
            self._root_contract_days[root_symbol]
            print('contract rules cached: reading from cache')
        except KeyError:
            print('contract rules not found: constructing rules')
            exchange_id = query_df(self._future_root,
                                    {'root_symbol': root_symbol}
                                    )['parent_calendar_id'].to_string(index=False)

            # Set up calendar and date rules
            exchange_calendar = trading_calendars.get_calendar(exchange_id)
            exchange_holidays = exchange_calendar.regular_holidays.holidays()
            class ExchangeDay(CustomBusinessDay):
                def __init__(self, n=1, normalize=False, weekmask='Mon Tue Wed Thu Fri',
                     holidays=exchange_holidays, calendar=None, offset=timedelta(0)):
                     super(ExchangeDay, self).__init__(n=n, normalize=False, weekmask='Mon Tue Wed Thu Fri',
                        holidays=holidays, calendar=None, offset=timedelta(0))

            EDay = ExchangeDay

            def previous_exchange_day(dt):
                """
                If day falls on non-exchange day, use previous exchange instead;
                """
                if dt.weekday() == 5 or dt.weekday() == 6 or dt in exchange_holidays:
                    return dt + EDay(-1)
                return dt

            def next_exchange_day(dt):
                """
                If day falls on non-exchange day, use previous exchange instead;
                """
                if dt.weekday() == 5 or dt.weekday() == 6 or dt in exchange_holidays:
                    return dt + EDay(1)
                return dt

            # Obtain relevant contract rules, calculate dates and create pandas dataframe
            contract_rules = self._future_calendar_rules[self._future_calendar_rules['root_symbol'] == root_symbol]
            contract_day_list = list(contract_rules.contract_day.unique())

            contract_rules_dict = {}

            globs = globals()
            locs = locals()

            for contract_day in contract_day_list:
                contract_field_list = query_df(contract_rules,{'contract_day': contract_day}).field.unique()

                # Extract rules
                contract_field_dict = {'day': None, 'offset': None, 'observance': None}
                for contract_field in contract_field_list:
                        contract_field_dict[contract_field] = contract_rules[
                            (contract_rules['contract_day'] == contract_day) &
                            (contract_rules['field'] == contract_field)
                            ].value.tolist()

                # Create and assign FutureContractDay dates
                _day = None if contract_field_dict['day'] is None else int(contract_field_dict['day'][0])
                _offset = None if contract_field_dict['offset'] is None else [eval(x,globs,locs) for x in contract_field_dict['offset']]
                _observance = None if contract_field_dict['observance'] is None else eval(contract_field_dict['observance'][0],globs,locs)

                contract_rules_dict[contract_day] = FutureContractDay(
                                root_symbol=root_symbol,
                                name=contract_day,
                                day = _day, # int
                                offset = _offset, # list
                                observance = _observance, # function
                                )

            self._root_contract_days[root_symbol] = contract_rules_dict

    def listing_reference_date(self, date,
                               root_symbol=None,
                               delivery_months=None,
                               front_month_offset=None,
                               reference_month_offset=None,
                               first_trade_offset = None,
                               ):
        """
        Calculate reference dates for datetimeindex dates
        Parameters
        ----------
        root_symbol : str
            root_symbol to find reference day for
        delivery_months : list
            list of month codes for valid contract months
        front_month_offset : int
            denotes offset, in months, from current date
            to reach front contract month
        reference_month_offset : list
            list of offset rules to apply too front contract month
            to reach reference date
        first_trade_offset: pd.timeseries.offset
            pd.timeseries.offset rule for first trade date, if any
        """
        front_month_contract = root_symbol + "_" + get_front_month_suffix(
                                   delivery_months, date, front_month_offset)
        front_month_contract_date = suffix_to_date(front_month_contract[-3:])
        front_month_last_trade = self._root_contract_days[root_symbol]['last_trade'].dates(
                                 front_month_contract_date
                                 )
        if first_trade_offset:
            date += first_trade_offset

        if date > front_month_last_trade:
            date = front_month_contract_date + MonthBegin(n=1)
        else:
            date = date.replace(day=1)

        if reference_month_offset:
            date = date.replace(day=2) if date.month == 1 else date
            # if we are adding a non-vectorized value
            # ignore the PerformanceWarnings:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", PerformanceWarning)
                date += reference_month_offset
        return date

    def get_platform_tickers(self, root_symbol, month_year_df, platform = 'RIC'):
        """
        Calculate exchange_symbols and platform tickers for month-and-year
        DataFrame
        Parameters
        ----------
        root_symbol : str
            root_symbol to find reference day for
        month_year_df : DataFrame
            DataFrame containing month codes in column 1 and year in column 2
        platform : str
            platform for which to obtain ticker
        """
        platform_symbol = self._platform_symbol_mapping[
                (self._platform_symbol_mapping['exchange_symbol'] == root_symbol) & \
                (self._platform_symbol_mapping['platform'] == platform)
                ].set_index('exchange_symbol').to_dict()['platform_symbol'][root_symbol]

        y_list = list(month_year_df['year'].astype(str))

        exchange_tkr = root_symbol+"_"+month_year_df['month']+[x[-2:] for x in y_list if len(x) > 2]

        if platform == "BBG":
            platform_tkr = platform_symbol+month_year_df['month']+[x[-2:] for x in y_list if len(x) > 2]
        elif platform == "RIC":
            platform_tkr = platform_symbol+month_year_df['month']+[x[-1:] for x in y_list if len(x) > 2]+"^"+[x[2] for x in y_list]

        return pd.DataFrame({'exchange_symbol': exchange_tkr.values, 'platform_symbol': platform_tkr.values,
                                 'delivery_month': month_year_df.index.month, 'delivery_year': month_year_df.index.year},
                                 index = month_year_df.index)

    def exchange_symbol_to_ticker(self, exchange_symbol, platform = platform_default):
        root_symbol, suffix = exchange_symbol.split("_")
        platform_symbol = self._platform_symbol_mapping[
                (self._platform_symbol_mapping['exchange_symbol'] == root_symbol) & \
                (self._platform_symbol_mapping['platform'] == platform)
                ].set_index('exchange_symbol').to_dict()['platform_symbol'][root_symbol]

        if platform == "BBG":
            platform_tkr = platform_symbol+suffix
        elif platform == "RIC":
            platform_tkr = platform_symbol+suffix[0]+suffix[-1]+"^"+suffix[1]

        return platform_tkr
