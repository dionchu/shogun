import numpy as np
from .errors import HistoryWindowStartsBeforeData

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

        self._first_trading_day = first_trading_day

        # Store the locs of the first day and first minute
        self._first_trading_day_loc = (
            self.trading_calendar.all_sessions.get_loc(self._first_trading_day)
            if self._first_trading_day is not None else None
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
                ].date()
            )
