from pandas import read_hdf, DatetimeIndex
from trading_calendars import get_calendar
import pandas as pd
import os
dirname = os.path.dirname(__file__)

from shogun.utils.memoize import lazyval
from shogun.data_portal.session_bars import SessionBarReader
from shogun.data_portal.bar_reader import (
    NoDataAfterDate,
    NoDataBeforeDate,
    NoDataOnDate,
)

#class HdfDailyBarReader(SessionBarReader):
class HdfDailyBarReader(SessionBarReader):
    """
    Reader for raw pricing data in InstrumentData.h5.
    Parameters
    ----------
    instrument_hdf : pandas hdf file
        The hdf contaning the pricing data, with attrs corresponding to the
        Attributes list below.
        read_all_threshold : int
        The number of instruments at which; below, the data is read by reading a
        slice from the hdf per asset.  above, the data is read by pulling
        all of the data for all assets into memory and then indexing into that
        array for each day and asset pair.  Used to tune performance of reads
        when using a small or large number of instruments.
    Attributes
    ----------
    The table with which this loader interacts contains the following
    attributes:
    first_row : dict
        Map from asset_id -> index of first row in the dataset with that id.
    last_row : dict
        Map from asset_id -> index of last row in the dataset with that id.
    calendar_offset : dict
        Map from asset_id -> calendar index of first row.
    start_session_ns: int
        Epoch ns of the first session used in this dataset.
    end_session_ns: int
        Epoch ns of the last session used in this dataset.
    calendar_name: str
        String identifier of trading calendar used (ie, "NYSE").
    We use first_row and last_row together to quickly find ranges of rows to
    load when reading an asset's data into memory.
    We use calendar_offset and calendar to orient loaded blocks within a
    range of queried dates.
    Notes
    ------
    A Bcolz CTable is comprised of Columns and Attributes.
    The table with which this loader interacts contains the following columns:
    ['date', 'exchange_symbol', 'open', 'high', 'low', 'close', 'volume',	'open_interest'].
    The data in these columns is interpreted as follows:
    - Price columns ('open', 'high', 'low', 'close') are interpreted as
        as-traded dollar value.
    - Volume is interpreted as as-traded volume.
    - Date
    - Exchange_symbol is the exchange_symbol of the row.
    """
    def __init__(self, calendar, start_session=None, end_session=None, read_all_threshold= 3000):
        self.df = read_hdf(dirname +'\..\database\_InstrumentData.h5')
        if not start_session:
            start_session = max(pd.Timestamp(min(self.df.index.get_level_values(0)),tz='UTC'),calendar.first_session)
        if not end_session:
            end_session = min(pd.Timestamp(max(self.df.index.get_level_values(0)),tz='UTC'),calendar.last_session)
        self.calendar = calendar
        self._start_session = start_session
        self._end_session = end_session

    @property
    def trading_calendar(self):
        return self.calendar

    @property
    def sessions(self):
        return self.trading_calendar.sessions_in_range(self._start_session,self._end_session)

    @lazyval
    def first_trading_day(self):
        """
        Returns
        -------
        dt : pd.Timestamp
            The last session for which the reader can provide data.
        """
        return self.trading_calendar.first_session

    @property
    def last_available_dt(self):
        return self._end_session

    def get_last_traded_dt(self, instrument, day):
        instrument_sessions = instrument.exchange_info.calendar.sessions_in_range(
            self._start_session, self._end_session
        )

        try:
            ix = instrument_sessions.get_loc(day)
        except KeyError:
            return NaT

        return day

    def load_raw_arrays(self, columns, start_date, end_date, exchange_symbols):
        out = []
#        print(start_date)
        for exchange_symbol in exchange_symbols:
            query = "date>=" + start_date.strftime("%Y%m%d") + \
                    " & date<=" + end_date.strftime("%Y%m%d") + \
                    " & exchange_symbol=" + exchange_symbol
            result = read_hdf(dirname +'\..\database\_InstrumentData.h5',where=query)
            result_dates = DatetimeIndex(result.index.get_level_values(0), dtype='datetime64[ns, UTC]')
            result = result[~result.index.get_level_values(0).isin(result_dates.difference(self.sessions))]
            out.append(result.as_matrix(columns))
        debug_session = self.trading_calendar.sessions_in_range(start_date,end_date)
#        print(debug_session.difference(result_dates))
        return out

    def get_value(self, exchange_symbol, dt, field):
        """
        Parameters
        ----------
        exchange_symbol : Exchange Symbol
            The exchange_symbol to get.
        day : datetime64-like
            Midnight of the day for which data is requested.
        colname : string
            The price field. e.g. ('open', 'high', 'low', 'close', 'volume', 'open_interest')
        Returns
        -------
        float
            The spot price for colname of the given exchange_symbol on the given day.
            Raises a NoDataOnDate exception if the given day and exchange_symbol is before
            or after the date range of the instrument.
            Returns -1 if the day is within the date range, but the price is
            0.
        """
#        exchange_symbol = instrument.exchange_symbol
#        print(exchange_symbol)
        query = "date=" + dt.strftime("%Y%m%d") + "& exchange_symbol=" + exchange_symbol
        results = read_hdf(dirname +'\..\database\_InstrumentData.h5',where=query)
        if results.shape[0] == 0:
            raise NoDataOnDate("day={0} is outside of calendar={1}".format(
                dt, self.trading_calendar.sessions_in_range(self._start_session, self._end_session)))
        return results.iloc[0]['open']
