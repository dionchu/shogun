import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
from sqlalchemy import Table, MetaData, select, inspect
from collections import namedtuple, OrderedDict

from .portfolio_metrics import Transaction, Position, Strategy
from shogun.data_portal.hdf_daily_bars import HdfDailyBarReader

#query = select([transactions]).where(and_(self._issuance.columns.Type == 'BILL',
#                self._issuance.columns.MaturityDate > date.strftime('%Y-%m-%d')))

from shogun.DIRNAME import dirname

platform_default = 'FID'

_blotter_float_fields = frozenset({
    'Agreed_Price',
    'Target',
    'Slip',
    'Multiplier',
    'Dollar_Slip',
    'Option_Expiry_Fee',
    'GiveIn_Fee',
    'Assignment_Fee',
    'NFA_Fee',
    'Exchange_Fee',
    'Clearing_Fee',
    'IB_Transaction_Fee',
    'FINRA_Sell_Fee',
    'ADM_Handling_Fee',
    'Total_Fee',
})

def _convert_blotter_float_fields(df):
    """
    Takes in a df of Instrument metadata columns and converts dates to pd.datetime64
    """
    for key in _blotter_float_fields:
        df[key] = df[key].astype(float)
    return df

class PerformanceAnalysis(object):
    """A Performance Analysis object tracks historical pnl and other
    portfolio metrics for performance analysis
    Parameters
    ----------
    start_date: str
        the start date in yyyy-mm-dd format
    end_date: str
        the end date in yyyy-mm-dd format
    """

    def __init__(self, start_date, end_date, trading_calendar, instrument_finder, portfolio_source='portfolio_ogf', strategy_dict=None, pnl_dict=None): # add , portfolio or something similar
        self.start_date = start_date
        self.end_date = end_date
        self.trading_calendar = trading_calendar
        self.instrument_finder = instrument_finder
        self._bar_reader = HdfDailyBarReader(self.trading_calendar)
        self._engine = create_engine(f'mysql+mysqlconnector://orth_mfp:N8WWQp+4@159.65.191.117/{portfolio_source}', echo=False) # portfolio_source is either portfolio_ogf, portfolio_harbourton, or something else; default should be portfolio_ogf
        # Maybe add error thing saying portfolio
        self._connection = self._engine.connect()
        self._inspector = inspect(self._engine)
        self._metadata = MetaData(self._engine)
        self._transactions = Table('Transactions', self._metadata, autoload=True, autoload_with=self._engine)
        self._blotter = self.get_blotter()
        blotter_start = min(self._blotter.Trade_Date).strftime('%Y-%m-%d')
        blotter_end = max(self._blotter.Trade_Date).strftime('%Y-%m-%d')
        self._dividend = pd.read_hdf(dirname + '\_EquityDividend.h5', where='ex_date >\'' + blotter_start + '\' & ex_date <=\'' + blotter_end + '\'')
        self._platform_symbol_mapping = pd.read_csv(dirname + '\_PlatformSymbolMapping.csv')
        if strategy_dict and pnl_dict:
            self.strategy_dict = strategy_dict
            self.pnl_dict = pnl_dict
            start_timestamp = sum(pnl_dict.values()).index[-1]+1 # This has a future deprecation warning
            self.trading_sessions = pd.date_range(start_timestamp.strftime('%Y-%m-%d'), end_date, freq=pd.offsets.BDay())
        elif (strategy_dict is not None and pnl_dict is None) or (pnl_dict is not None and strategy_dict is None):
            print("Error: both strategy_dict and pnl_dict must be provided or none")
            return
        else:
            self.strategy_dict = OrderedDict()
            self.pnl_dict = OrderedDict()
            self.trading_sessions = pd.date_range(start_date, end_date, freq=pd.offsets.BDay())
        
    def get_blotter(self):
        """
        Retrieve transaction data from portfolio transactions database
        """
        query = select([self._transactions])


        ResultProxy = self._connection.execute(query)
        ResultSet = ResultProxy.fetchall()
        df = pd.DataFrame(ResultSet)
        df.columns = [c['name'] for c in self._inspector.get_columns('Transactions')]
        self._connection.close()
        df.set_index("Transaction_ID",inplace=True)
        df = _convert_blotter_float_fields(df)
        return df

    def platform_to_exchange_symbol(self, platform_ticker, platform = platform_default):
        """
        Calculate exchange symbol for given platform tickers
        DataFrame
        Parameters
        ----------
        exchange_symbol : str
            exchange_symbol to find reference day for
        platform : str
            platform for which to obtain ticker
        """
        exchange_symbol = self._platform_symbol_mapping[
                (self._platform_symbol_mapping['platform_symbol'] == platform_ticker) & \
                (self._platform_symbol_mapping['platform'] == platform)
                ].set_index('platform_symbol').to_dict()['exchange_symbol'][platform_ticker]

        return exchange_symbol

    def translate_instrument(self, blotter_instrument, session):
        """
        Translate blotter instrument string into shogun instrument name
        """
        if re.split(r'\_', blotter_instrument)[0] == blotter_instrument:
            return blotter_instrument
        else:
            # split out fidessa ticker by underscore
            m = re.split(r'\_', blotter_instrument)
            root = self.platform_to_exchange_symbol(m[0], platform = 'FID')
            type = m[1][0]
            if type == 'F':
                # obtain everything after underscore but before period
                suffix = m[1][1:].split('.')[0]
                current_decade = int(session.strftime("%y")[0])*10
                current_year = int(session.strftime("%y")[1])
                contract_year = int(re.search('[0-9]',suffix).group(0))
                # if the last digit of year code is less than last digit of current session year
                if contract_year <  current_year:
                    suffix = suffix.replace(str(contract_year),str(contract_year+current_decade+10))
                else:
                    suffix = suffix.replace(str(contract_year),str(contract_year+current_decade))
                exchange_symbol = root + '_' + suffix
            elif type == 'O':
                # obtain everything after underscore but before period
                suffix = m[1][1:].split('.')[0][-2:]
                current_decade = int(session.strftime("%y")[0])*10
                current_year = int(session.strftime("%y")[1])
                contract_year = int(re.search('[0-9]',suffix).group(0))
                # if the last digit of year code is less than last digit of current session year
                if contract_year <  current_year:
                    suffix = suffix.replace(str(contract_year),str(contract_year+current_decade+10))
                else:
                    suffix = suffix.replace(str(contract_year),str(contract_year+current_decade))
                strike = m[1][1:].split('.')[0][:4]
                exchange_symbol = root + '_' + suffix + '_' + strike

            return exchange_symbol

    def process_transactions(self):
        strategy_list = list(self._blotter.groupby('Strategy').groups.keys())

        for strategy in strategy_list:
            sub = self._blotter.groupby('Strategy').get_group(strategy)
            strategy_tracker = Strategy()
            daily_pnl = pd.DataFrame(np.nan, index=self.trading_sessions, columns=['realized','unrealized','commission','dividend'])

            for session in self.trading_sessions:
                sub_session = sub.loc[sub['Trade_Date'] == session.strftime('%Y-%m-%d')]
                sub_div = self._dividend.loc[self._dividend['ex_date'] == session.strftime('%Y-%m-%d')]

                # Dividend must be processed before transactions
                for i in range(len(sub_div)):
                    div_row = sub_div.iloc[i]
                    div_instrument = self.instrument_finder.retrieve_instrument(sub_div.index[i])
                    strategy_tracker.process_dividend(div_instrument, div_row)

                for i in range(len(sub_session)):
                    row = sub_session.iloc[i]
                    amt = row['Trade_Volume'] if row['Buy_Sell'] == 'B' else -row['Trade_Volume']
                    exchange_symbol = self.translate_instrument(row.Instrument, session)
                    shogun_instrument = self.instrument_finder.retrieve_instrument(exchange_symbol)
                    txn = Transaction(shogun_instrument, amt, pd.Timestamp(row.Trade_Date), row['Agreed_Price'], row['Total_Fee'], row['Multiplier'])
                    strategy_tracker.execute_transaction(txn)

                strategy_tracker.refresh(session, self._bar_reader)
                daily_pnl.loc[session].realized = strategy_tracker.realized_pnl
                daily_pnl.loc[session].unrealized = strategy_tracker.unrealized_pnl
                daily_pnl.loc[session].commission = strategy_tracker.commission
                daily_pnl.loc[session].dividend = strategy_tracker.dividend

            daily_pnl['sum'] = daily_pnl.sum(axis=1)
            self.strategy_dict[strategy] = strategy_tracker
            self.pnl_dict[strategy] = daily_pnl

    def update_transactions(self):
        strategy_list = list(self._blotter.groupby('Strategy').groups.keys())

        for strategy in strategy_list:
            sub = self._blotter.groupby('Strategy').get_group(strategy)
            strategy_tracker = self.strategy_dict[strategy]
            daily_pnl = self.pnl_dict[strategy]
            daily_pnl = daily_pnl.append(pd.DataFrame(np.nan, index=self.trading_sessions, columns=['realized','unrealized','commission','dividend']))

            for session in self.trading_sessions:
                sub_session = sub.loc[sub['Trade_Date'] == session.strftime('%Y-%m-%d')]
                sub_div = self._dividend.loc[self._dividend['ex_date'] == session.strftime('%Y-%m-%d')]

                # Dividend must be processed before transactions
                for i in range(len(sub_div)):
                    div_row = sub_div.iloc[i]
                    div_instrument = self.instrument_finder.retrieve_instrument(sub_div.index[i])
                    strategy_tracker.process_dividend(div_instrument, div_row)

                for i in range(len(sub_session)):
                    row = sub_session.iloc[i]
                    amt = row['Trade_Volume'] if row['Buy_Sell'] == 'B' else -row['Trade_Volume']
                    exchange_symbol = self.translate_instrument(row.Instrument, session)
                    shogun_instrument = self.instrument_finder.retrieve_instrument(exchange_symbol)
                    txn = Transaction(shogun_instrument, amt, pd.Timestamp(row.Trade_Date), row['Agreed_Price'], row['Total_Fee'], row['Multiplier'])
                    strategy_tracker.execute_transaction(txn)

                strategy_tracker.refresh(session, self._bar_reader)
                daily_pnl.loc[session].realized = strategy_tracker.realized_pnl
                daily_pnl.loc[session].unrealized = strategy_tracker.unrealized_pnl
                daily_pnl.loc[session].commission = strategy_tracker.commission
                daily_pnl.loc[session].dividend = strategy_tracker.dividend

            daily_pnl['sum'] = daily_pnl.sum(axis=1)
            self.strategy_dict[strategy] = strategy_tracker
            self.pnl_dict[strategy] = daily_pnl