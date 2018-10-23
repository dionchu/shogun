import warnings
import trading_calendars
from datetime import date, datetime, timedelta
import pandas as pd

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

import os
dirname = os.path.dirname(__file__)

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

    def make_root_chain(self, root_symbol, start=start_default, end=end_default, platform=platform_default):
        """
        Generate root chain for a given root symbol and range.
        """

        # Fastpath for empty request.
        if not root_symbol:
            return None

        exchange_id = self._future_root[
            self._future_root['root_symbol'] == root_symbol
            ].set_index('root_symbol').to_dict()['parent_calendar_id'][root_symbol]

        platform_symbol = self._platform_symbol_mapping[
                (self._platform_symbol_mapping['exchange_symbol'] == root_symbol) & \
                (self._platform_symbol_mapping['platform'] == platform)
                ].set_index('exchange_symbol').to_dict()['platform_symbol'][root_symbol]

        root_contract_df = self._future_contract_listing[
                (self._future_contract_listing['root_symbol'] == root_symbol)
                ]

        listed_contracts = list(root_contract_df['delivery_month'])

        # If contract listings include serials, create monthly series, other quarterly
        if 'M' in list(root_contract_df['reference_month_offset_interval']):
            date_list = pd.date_range(start, end, freq='M')
        else:
            date_list = pd.date_range(start, end, freq='Q')

        merge = pd.concat([date_list.month.to_series(index = date_list),date_list.year.to_series(index = date_list)], axis =1)
        merge.columns = ['month','year']
        monthCode = ["F","G","H","J","K","M","N","Q","U","V","X","Z"]

        # Loop through to assign codes to months
        for i in range(0,12):
            merge.loc[merge['month'] == i+1,'month'] = monthCode[i]

        # Filter for only those months listed
        idx = merge['month'].isin(listed_contracts)
        merge = merge[idx]; date_list = date_list[idx]

        # Generate instrument names
        y_list = list(merge['year'].astype(str))

        exchange_tkr = root_symbol+"_"+merge['month']+[x[-2:] for x in y_list if len(x) > 2]

        if platform == "BBG":
            platform_tkr = platform_symbol+merge['month']+[x[-2:] for x in y_list if len(x) > 2]
        elif platform == "RIC":
            platform_tkr = platform_symbol+merge['month']+[x[-1:] for x in y_list if len(x) > 2]+"^"+[x[2] for x in y_list]

        symbol_df = pd.DataFrame({'exchange_symbol': exchange_tkr.values, 'platform_symbol': platform_tkr.values,
                                 'delivery_month': date_list.month, 'delivery_year': date_list.year})

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
        contract_day_list_dict = {}

        globs = globals()
        locs = locals()

        for contract_day in contract_day_list:
            contract_field_list = list(contract_rules.field.unique())

            # Extract rules

            for contract_field in contract_field_list:
                    contract_field_dict = {'day': None, 'offset': None, 'observance': None}
                    contract_field_dict[contract_field] = list(
                        contract_rules[(contract_rules['contract_day'] == contract_day) &
                        (contract_rules['field'] == contract_field)
                        ].value.astype(str))

            # Create and assign FutureContractDay dates
            _day = None if contract_field_dict['day'] is None else int(contract_field_dict['day'][0])
            _offset = None if contract_field_dict['offset'] is None else [eval(x,globs,locs) for x in contract_field_dict['offset']]
            _observance = None if contract_field_dict['observance'] is None else eval(contract_field_dict['observance'][0],globs,locs)

            contract_day_list_dict[contract_day] = FutureContractDay(
                            root_symbol=root_symbol,
                            name=contract_day,
                            day = _day, # int
                            offset = _offset, # list
                            observance = _observance, # function
                            reference_dates=date_list).dates

            contract_rules_dict[contract_day] = contract_field_dict

        contract_day_df = pd.DataFrame.from_dict(contract_day_list_dict)

        return pd.concat([symbol_df,contract_day_df],axis=1)
