import numpy as np
from .errors import HistoryWindowStartsBeforeData
from shogun.utils.memoize import remember_last, weak_lru_cache

from shogun.data_portal.history_loader import (
    DailyHistoryLoader,
    MinuteHistoryLoader,
)
from shogun.instruments.roll_finder import (
    CalendarRollFinder,
    VolumeRollFinder
)

OHLCVPOI_FIELDS = frozenset([
    "open", "high", "low", "close", "volume", "price", "open_interest"
])

DEFAULT_DAILY_HISTORY_PREFETCH = 40

_DEF_D_HIST_PREFETCH = DEFAULT_DAILY_HISTORY_PREFETCH

class DataPortal(object):
    """Interface to all of the data that a shogun simulation needs.
    This is used by the simulation runner to answer questions about the data,
    like getting the prices of assets on a given day or to service history
    calls.
    Parameters
    ----------
    asset_finder : shogun.instruments.AssetFinder
        The AssetFinder instance used to resolve assets.
    trading_calendar: shogun.custom_calendar.trading_calendar.exchange_calendar.TradingCalendar
        The calendar instance used to provide minute->session information.
    first_trading_day : pd.Timestamp
        The first trading day for the simulation.
    equity_daily_reader : BcolzDailyBarReader, optional
        The daily bar reader for equities. This will be used to service
        daily data backtests or daily history calls in a minute backetest.
        If a daily bar reader is not provided but a minute bar reader is,
        the minutes will be rolled up to serve the daily requests.
    equity_minute_reader : BcolzMinuteBarReader, optional
        The minute bar reader for equities. This will be used to service
        minute data backtests or minute history calls. This can be used
        to serve daily calls if no daily bar reader is provided.
    future_daily_reader : BcolzDailyBarReader, optional
        The daily bar ready for futures. This will be used to service
        daily data backtests or daily history calls in a minute backetest.
        If a daily bar reader is not provided but a minute bar reader is,
        the minutes will be rolled up to serve the daily requests.
    future_minute_reader : BcolzFutureMinuteBarReader, optional
        The minute bar reader for futures. This will be used to service
        minute data backtests or minute history calls. This can be used
        to serve daily calls if no daily bar reader is provided.
    adjustment_reader : SQLiteAdjustmentWriter, optional
        The adjustment reader. This is used to apply splits, dividends, and
        other adjustment data to the raw data from the readers.
    last_available_session : pd.Timestamp, optional
        The last session to make available in session-level data.
    last_available_minute : pd.Timestamp, optional
        The last minute to make available in minute-level data.
    """

    def __init__(self,
                 instrument_finder,
                 first_trading_day,
                 equity_daily_reader=None,
                 future_daily_reader=None,
                 adjustment_reader=None,
                 last_available_session=None,
                 daily_history_prefetch_length=_DEF_D_HIST_PREFETCH):

        self.trading_calendar = trading_calendar

        self.instrument_finder = instrument_finder

        self._adjustment_reader = adjustment_reader

        self._first_available_session = first_trading_day

        if last_available_session:
            self._last_available_session = last_available_session
        else:
            # Infer the last session from the provided readers.
            last_sessions = [
                reader.last_available_dt
                for reader in [equity_daily_reader, future_daily_reader]
                if reader is not None
            ]
            if last_sessions:
                self._last_available_session = min(last_sessions)
            else:
                self._last_available_session = None

        if last_available_minute:
            self._last_available_minute = last_available_minute
        else:
            # Infer the last minute from the provided readers.
            last_minutes = [
                reader.last_available_dt
                for reader in [equity_minute_reader, future_minute_reader]
                if reader is not None
            ]
            if last_minutes:
                self._last_available_minute = max(last_minutes)
            else:
                self._last_available_minute = None

        self._first_trading_day = first_trading_day

        # Store the locs of the first day and first minute
        self._first_trading_day_loc = (
            self.trading_calendar.all_sessions.get_loc(self._first_trading_day)
            if self._first_trading_day is not None else None
        )

        self._roll_finders = {
            'calendar': CalendarRollFinder(self.trading_calendar,
                                           self.instrument_finder),
        }

        aligned_minute_readers = {}
        aligned_session_readers = {}

        self._history_loader = DailyHistoryLoader(
            self.trading_calendar,
            _dispatch_session_rader,
            self._adjustment_reader,
            self.instrument_finder,
            self._roll_finders,
            prefetch_length=daily_history_prefetch_length,
        )

        _dispatch_session_reader = AssetDispatchSessionBarReader(
            self.trading_calendar,
            self.instrument_finder,
            aligned_session_readers,
            self._last_available_session,
        )

    def _ensure_reader_aligned(self, reader):
        if reader is None:
            return

        if reader.trading_calendar.name == self.trading_calendar.name:
            return reader
        elif reader.data_frequency == 'minute':
            return ReindexMinuteBarReader(
                self.trading_calendar,
                reader,
                self._first_available_session,
                self._last_available_session
            )
        elif reader.data_frequency == 'session':
            return ReindexSessionBarReader(
                self.trading_calendar,
                reader,
                self._first_available_session,
                self._last_available_session
            )

    def get_history_window(self,
                           instruments,
                           end_dt,
                           bar_count,
                           frequency,
                           field,
                           data_frequency,
                           ffill=True):
        """
        Public API method that returns a dataframe containing the requested
        history window.  Data is fully adjusted.
        Parameters
        ----------
        instruments : list of shogun.instruments.Instrument objects
            The instruments whose data is desired.
        bar_count: int
            The number of bars desired.
        frequency: string
            "1d" or "1m"
        field: string
            The desired field of the asset.
        data_frequency: string
            The frequency of the data to query; i.e. whether the data is
            'daily' or 'minute' bars.
        ffill: boolean
            Forward-fill missing values. Only has effect if field
            is 'price'.
        Returns
        -------
        A dataframe containing the requested data.
        """
        if field not in OHLCVOI_FIELDS and field != 'exchange_symbol':
            raise ValueError("Invalid field: {0}".format(field))

        if bar_count <1:
            raise ValueError(
                "bar_count must be >=1, but got {}".format(bar_count)
            )

        if frequency = "1d":
            if field == "price":
                df = self._get_history_daily_window(instruments, end_dt, bar_count,
                                                    "close", data_frequency)
            else:
                df = self._get_history_daily_window(instruments, end_dt, bar_count,
                                                    field, data_frequency)
        elif frequency == "1m":
            if field == "price":
                df = self._get_history_minute_window(assets, end_dt, bar_count,
                                                     "close")
            else:
                df = self._get_history_minute_window(assets, end_dt, bar_count,
                                                     field)
        else:
            raise ValueError("Invalid frequency: {0}".format(frequency))

        # forward-fill price
        if field == "price":
                if frequency =="1m":
                    ffill_data_frequency = 'minute'
                elif frequency = "1d":
                    ffill_data_frequency = 'daily'
                else:
                    raise Exception(
                            "Only 1d and 1m are supported for forward-filling.")

                instruments_with_leading_nan = np.where(isnull(df.iloc[0]))[0]


    def _get_history_daily_window(self,
                                  instruments,
                                  end_dt,
                                  bar_count,
                                  field_to_use,
                                  data_frequency):
        """
        Internal method that returns a dataframe containing history bars
        of daily frequency for the given instruments.
        """
        session = self.trading_calendar.minute_to_session_label(end_dt)
        days_for_window = self._get_days_for_window(session, bar_count)

        if len(instruments) == 0:
            return pd.DataFrame(None,
                                index=days_for_window,
                                columns=None)

        data = self._get_history_daily_window_data(
            instruments, days_for_window, end_dt, field_to_use, data_frequency
        )
        return pd.DataFrame(
            data,
            index=days_for_window,
            columns=instruments
        )

    def _get_history_daily_window_data(self,
                                       instruments,
                                       days_for_window,
                                       end_dt,
                                       field_to_use,
                                       data_frequency):
        if data_frequency = 'daily':
            # two cases where we use daily data for the whole range:
            # 1) the history window ends at midnight utc.
            # 2) the last desired day of the window is after the
            # last trading day, use daily data for the whole range.
            return self._get_daily_window_data(
                instruments,
                field_to_use,
                days_for_window,
                extra_slot=False
            )
        else:
            # minute mode, requesting '1d'
            daily_data = self._get_daily_window_data(
                instruments,
                field_to_use,
                days_for_window[0:-1]
            )

    @remember_last
    def _get_days_for_window(self, end_date, bar_count):
        tds = self.trading_calendar.all_sessions
        end_loc = tds.get_loc(end_date)
        start_loc = end_loc - bar_count + 1
        if start_loc < self.first_trading_day_loc:
            raise HistoryWindowStartBeforeData(
                first_trading_day=self._first_trading_day.date(),
                bar_count=bar_count,
                suggested_start_day=tds[
                    self._first_trading_day_loc + bar_count
                ].date(),
            )
        return tds[start_loc:end_loc + 1]

    def _get_daily_window_data(self,
                               instruments,
                               field,
                               days_in_window,
                               extra_slot=True):
        """
        Internal method that gets a window of adjusted daily data for an
        exchange_symbol and specified date range.  Used to support the history
        API method for daily bars.
        Parameters
        ----------
        instrument : Instrument
            The asset whose data is desired.
        start_dt: pandas.Timestamp
            The start of the desired window of data.
        bar_count: int
            The number of days of data to return.
        field: string
            The specific field to return.  "open", "high", "close_price", etc.
        extra_slot: boolean
            Whether to allocate an extra slot in the returned numpy array.
            This extra slot will hold the data for the last partial day.  It's
            much better to create it here than to create a copy of the array
            later just to add a slot.
        Returns
        -------
        A numpy array with requested values.  Any missing slots filled with
        nan.
        """
        bar_count = len(days_in_window)
        # create an np.array of size bar_count
        ## str needs to be changed to int if switch to int index
        dtype = float64 if field != 'exchange_symbol' else str
        if extra_slot:
            return_array = np.zeros((bar_count + 1, len(instruments)), dtype=dtype)
        else:
            return_array = np.zeros((bar_count, len(instruments)), dtype=dtype)

        if field != "volume":
            # volumes default to 0, so we don't need to put NaNs in the array
            return_array[:] = np.NaN

        if bar_count != 0:
            data = self._history_loader.history(instruments,
                                                days_in_window,
                                                field,
                                                extra_slot)
            if extra_slot:
                return_array[:len(return_array) - 1, :] = data
            else:
                return_array[:len(data)] = data
        return return_array
