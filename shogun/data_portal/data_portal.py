import pandas as pd
import numpy as np
from numpy import float64, int64, nan
from shogun.errors import HistoryWindowStartsBeforeData
from shogun.utils.memoize import remember_last, weak_lru_cache

from shogun.instruments.instrument import (
    Instrument,
    Equity,
    Future,
)
from shogun.instruments.continuous_futures import ContinuousFuture
from shogun.instruments.instrument_finder import (
    InstrumentFinder,
    InstrumentConvertible,
    PricingDataAssociable,
)
from shogun.data_portal.history_loader import (
    DailyHistoryLoader,
)
from shogun.data_portal.continuous_future_reader import (
    ContinuousFutureSessionBarReader,
)
from shogun.instruments.roll_finder import (
    CalendarRollFinder,
    VolumeRollFinder
)
from shogun.data_portal.resample import (
    ReindexSessionBarReader,
)
from shogun.data_portal.dispatch_bar_reader import (
    InstrumentDispatchSessionBarReader,
)

BASE_FIELDS = frozenset([
    "open",
    "high",
    "low",
    "close",
    "volume",
    "price",
    "contract",
    "exchange_symbol",
    "last_traded",
])

OHLCVOI_FIELDS = frozenset([
    "open", "high", "low", "close", "volume", "open_interest"
])

OHLCVPOI_FIELDS = frozenset([
    "open", "high", "low", "close", "volume", "price", "open_interest"
])

DEFAULT_DAILY_HISTORY_PREFETCH = 40

_DEF_D_HIST_PREFETCH = DEFAULT_DAILY_HISTORY_PREFETCH

class DataPortal(object):
    """Interface to all of the data that a shogun simulation needs.
    This is used by the simulation runner to answer questions about the data,
    like getting the prices of instruments on a given day or to service history
    calls.
    Parameters
    ----------
    instrument_finder : shogun.instruments.InstrumentFinder
        The InstrumentFinder instance used to resolve instruments.
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
                 trading_calendar,
                 first_trading_day,
                 equity_daily_reader=None,
                 future_daily_reader=None,
                 adjustment_reader=None,
                 last_available_session=None,
                 daily_history_prefetch_length=_DEF_D_HIST_PREFETCH):

        self.trading_calendar = trading_calendar

        self.instrument_finder = instrument_finder

        self._adjustment_reader = adjustment_reader

        # This will be empty placeholder for now
        self._augmented_sources_map = {}

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

        aligned_session_readers = {}

        aligned_equity_session_reader = self._ensure_reader_aligned(
            equity_daily_reader)

        aligned_future_session_reader = self._ensure_reader_aligned(
            future_daily_reader)

        self._roll_finders = {
            'calendar': CalendarRollFinder(self.trading_calendar,
                                           self.instrument_finder),
        }

        if aligned_equity_session_reader is not None:
            aligned_session_readers[Equity] = aligned_equity_session_reader

        if aligned_future_session_reader is not None:
            aligned_session_readers[Future] = aligned_future_session_reader
            self._roll_finders['volume'] = VolumeRollFinder(
                self.trading_calendar,
                self.instrument_finder,
                aligned_future_session_reader,
            )
            aligned_session_readers[ContinuousFuture] = \
                ContinuousFutureSessionBarReader(
                    aligned_future_session_reader,
                    self._roll_finders,
                )

        _dispatch_session_reader = InstrumentDispatchSessionBarReader(
            self.trading_calendar,
            self.instrument_finder,
            aligned_session_readers,
            self._last_available_session,
        )

        self._pricing_readers = {
            'daily': _dispatch_session_reader,
        }

        self._history_loader = DailyHistoryLoader(
            self.trading_calendar,
            _dispatch_session_reader,
            self._adjustment_reader,
            self.instrument_finder,
            self._roll_finders,
            prefetch_length=daily_history_prefetch_length,
        )

        self._first_trading_day = first_trading_day

        # Store the locs of the first day and first minute
        self._first_trading_day_loc = (
            self.trading_calendar.all_sessions.get_loc(self._first_trading_day)
            if self._first_trading_day is not None else None
        )

    def _ensure_reader_aligned(self, reader):
        if reader is None:
            return

        if reader.trading_calendar.name == self.trading_calendar.name:
            return reader
        elif reader.data_frequency == 'session':
            return ReindexSessionBarReader(
                self.trading_calendar,
                reader,
                self._first_available_session,
                self._last_available_session
            )

    def get_last_traded_dt(self, instrument, dt, data_frequency):
        """
        Given an instrument and dt, returns the last traded dt from the viewpoint
        of the given dt.
        If there is a trade on the dt, the answer is dt provided.
        """
        return self._get_pricing_reader(data_frequency).get_last_traded_dt(
            instrument, dt)

    @staticmethod
    def _is_extra_source(instrument, field, map):
        """
        Internal method that determines if this instrument/field combination
        represents a fetcher value or a regular OHLCVP lookup.
        """
        # If we have an extra source with a column called "price", only look
        # at it if it's on something like palladium and not AAPL (since our
        # own price data always wins when dealing with instruments).

        return not (field in BASE_FIELDS and
                    (isinstance(instrument, (Instrument, ContinuousFuture))))

    def _get_fetcher_value(self, instrument, field, dt):
        day = normalize_date(dt)

        try:
            return \
                self._augmented_sources_map[field][instrument].loc[day, field]
        except KeyError:
            return np.NaN

    def _get_single_instrument_value(self,
                                session_label,
                                instrument,
                                field,
                                dt,
                                data_frequency):
        if self._is_extra_source(
                instrument, field, self._augmented_sources_map):
            return self._get_fetcher_value(instrument, field, dt)

        if field not in BASE_FIELDS:
            raise KeyError("Invalid column: " + str(field))

        if dt < instrument.start_date or \
                (data_frequency == "daily" and
                    session_label > instrument.end_date) or \
                (data_frequency == "minute" and
                 session_label > instrument.end_date):
            if field == "volume":
                return 0
            elif field == "contract":
                return None
            elif field != "last_traded":
                return np.NaN

        if data_frequency == "daily":
            if field == "contract":
                return self._get_current_contract(instrument, session_label)
            else:
                return self._get_daily_spot_value(
                    instrument, field, session_label,
                )
        else:
            if field == "last_traded":
                return self.get_last_traded_dt(instrument, dt, 'minute')
            elif field == "price":
                return self._get_minute_spot_value(
                    instrument, "close", dt, ffill=True,
                )
            elif field == "contract":
                return self._get_current_contract(instrument, dt)
            else:
                return self._get_minute_spot_value(instrument, field, dt)

    def get_spot_value(self, instruments, field, dt, data_frequency):
        """
        Public API method that returns a scalar value representing the value
        of the desired instrument's field at either the given dt.
        Parameters
        ----------
        instruments : Instrument, ContinuousFuture, or iterable of same.
            The instrument or instruments whose data is desired.
        field : {'open', 'high', 'low', 'close', 'volume',
                 'price', 'last_traded'}
            The desired field of the instrument.
        dt : pd.Timestamp
            The timestamp for the desired value.
        data_frequency : str
            The frequency of the data to query; i.e. whether the data is
            'daily' or 'minute' bars
        Returns
        -------
        value : float, int, or pd.Timestamp
            The spot value of ``field`` for ``instrument`` The return type is based
            on the ``field`` requested. If the field is one of 'open', 'high',
            'low', 'close', or 'price', the value will be a float. If the
            ``field`` is 'volume' the value will be a int. If the ``field`` is
            'last_traded' the value will be a Timestamp.
        """
        instruments_is_scalar = False
        if isinstance(instruments, (InstrumentConvertible, PricingDataAssociable)):
            instruments_is_scalar = True
        else:
            # If 'instruments' was not one of the expected types then it should be
            # an iterable.
            try:
                iter(instruments)
            except TypeError:
                raise TypeError(
                    "Unexpected 'instruments' value of type {}."
                    .format(type(instruments))
                )

        session_label = self.trading_calendar.minute_to_session_label(dt)

        if instruments_is_scalar:
            return self._get_single_instrument_value(
                session_label,
                instruments,
                field,
                dt,
                data_frequency,
            )
        else:
            get_single_instrument_value = self._get_single_instrument_value
            return [
                get_single_instrument_value(
                    session_label,
                    instrument,
                    field,
                    dt,
                    data_frequency,
                )
                for instrument in instruments
            ]

    def _get_minute_spot_value(self, instrument, column, dt, ffill=False):
        reader = self._get_pricing_reader('minute')

        if not ffill:
            try:
                return reader.get_value(instrument.exchange_symbol, dt, column)
            except NoDataOnDate:
                if column != 'volume':
                    return np.nan
                else:
                    return 0

        # At this point the pairing of column='close' and ffill=True is
        # assumed.
        try:
            # Optimize the best case scenario of a liquid instrument
            # returning a valid price.
            result = reader.get_value(instrument.exchange_symbol, dt, column)
            if not pd.isnull(result):
                return result
        except NoDataOnDate:
            # Handling of no data for the desired date is done by the
            # forward filling logic.
            # The last trade may occur on a previous day.
            pass
        # If forward filling, we want the last minute with values (up to
        # and including dt).
        query_dt = reader.get_last_traded_dt(instrument, dt)

        if pd.isnull(query_dt):
            # no last traded dt, bail
            return np.nan

        result = reader.get_value(instrument.exchange_symbol, query_dt, column)

        if (dt == query_dt) or (dt.date() == query_dt.date()):
            return result

        # the value we found came from a different day, so we have to
        # adjust the data if there are any adjustments on that day barrier
        return self.get_adjusted_value(
            instrument, column, query_dt,
            dt, "minute", spot_value=result
        )

    def _get_daily_spot_value(self, instrument, column, dt):
        reader = self._get_pricing_reader('daily')
        if column == "last_traded":
            last_traded_dt = reader.get_last_traded_dt(instrument, dt)

            if isnull(last_traded_dt):
                return pd.NaT
            else:
                return last_traded_dt
        elif column in OHLCVOI_FIELDS:
            # don't forward fill
            try:
                return reader.get_value(instrument, dt, column)
            except NoDataOnDate:
                return np.nan
        elif column == "price":
            found_dt = dt
            while True:
                try:
                    value = reader.get_value(
                        instrument, found_dt, "close"
                    )
                    if not isnull(value):
                        if dt == found_dt:
                            return value
                        else:
                            # adjust if needed
                            return self.get_adjusted_value(
                                instrument, column, found_dt, dt, "minute",
                                spot_value=value
                            )
                    else:
                        found_dt -= self.trading_calendar.day
                except NoDataOnDate:
                    return np.nan

    def get_adjustments(self, instruments, field, dt, perspective_dt):
        """
        Returns a list of adjustments between the dt and perspective_dt for the
        given field and list of instruments
        Parameters
        ----------
        instruments : list of type Instrument, or Instrument
            The instrument, or instruments whose adjustments are desired.
        field : {'open', 'high', 'low', 'close', 'volume', \
                 'price', 'last_traded'}
            The desired field of the instrument.
        dt : pd.Timestamp
            The timestamp for the desired value.
        perspective_dt : pd.Timestamp
            The timestamp from which the data is being viewed back from.
        Returns
        -------
        adjustments : list[Adjustment]
            The adjustments to that field.
        """
        if isinstance(instruments, Instrument):
            instruments = [instruments]

        adjustment_ratios_per_instrument = []

        def split_adj_factor(x):
            return x if field != 'volume' else 1.0 / x

        for instrument in instruments:
            adjustments_for_instrument = []
            split_adjustments = self._get_adjustment_list(
                instrument, self._splits_dict, "SPLITS"
            )
            for adj_dt, adj in split_adjustments:
                if dt < adj_dt <= perspective_dt:
                    adjustments_for_instrument.append(split_adj_factor(adj))
                elif adj_dt > perspective_dt:
                    break

            if field != 'volume':
                merger_adjustments = self._get_adjustment_list(
                    instrument, self._mergers_dict, "MERGERS"
                )
                for adj_dt, adj in merger_adjustments:
                    if dt < adj_dt <= perspective_dt:
                        adjustments_for_instrument.append(adj)
                    elif adj_dt > perspective_dt:
                        break

                dividend_adjustments = self._get_adjustment_list(
                    instrument, self._dividends_dict, "DIVIDENDS",
                )
                for adj_dt, adj in dividend_adjustments:
                    if dt < adj_dt <= perspective_dt:
                        adjustments_for_instrument.append(adj)
                    elif adj_dt > perspective_dt:
                        break

            ratio = reduce(mul, adjustments_for_instrument, 1.0)
            adjustment_ratios_per_instrument.append(ratio)

        return adjustment_ratios_per_instrument

    def get_adjusted_value(self, instrument, field, dt,
                           perspective_dt,
                           data_frequency,
                           spot_value=None):
        """
        Returns a scalar value representing the value
        of the desired instrument's field at the given dt with adjustments applied.
        Parameters
        ----------
        instrument : Instrument
            The instrument whose data is desired.
        field : {'open', 'high', 'low', 'close', 'volume', \
                 'price', 'last_traded'}
            The desired field of the instrument.
        dt : pd.Timestamp
            The timestamp for the desired value.
        perspective_dt : pd.Timestamp
            The timestamp from which the data is being viewed back from.
        data_frequency : str
            The frequency of the data to query; i.e. whether the data is
            'daily' or 'minute' bars
        Returns
        -------
        value : float, int, or pd.Timestamp
            The value of the given ``field`` for ``instrument`` at ``dt`` with any
            adjustments known by ``perspective_dt`` applied. The return type is
            based on the ``field`` requested. If the field is one of 'open',
            'high', 'low', 'close', or 'price', the value will be a float. If
            the ``field`` is 'volume' the value will be a int. If the ``field``
            is 'last_traded' the value will be a Timestamp.
        """
        if spot_value is None:
            # if this a fetcher field, we want to use perspective_dt (not dt)
            # because we want the new value as of midnight (fetcher only works
            # on a daily basis, all timestamps are on midnight)
            if self._is_extra_source(instrument, field,
                                     self._augmented_sources_map):
                spot_value = self.get_spot_value(instrument, field, perspective_dt,
                                                 data_frequency)
            else:
                spot_value = self.get_spot_value(instrument, field, dt,
                                                 data_frequency)

        if isinstance(instrument, Equity):
            ratio = self.get_adjustments(instrument, field, dt, perspective_dt)[0]
            spot_value *= ratio

        return spot_value

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
            The desired field of the instrument.
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
        if field not in OHLCVPOI_FIELDS and field != 'exchange_symbol':
            raise ValueError("Invalid field: {0}".format(field))

        if bar_count <1:
            raise ValueError(
                "bar_count must be >=1, but got {}".format(bar_count)
            )

        if frequency == "1d":
            if field == "price":
                df = self._get_history_daily_window(instruments, end_dt, bar_count,
                                                    "close", data_frequency)
            else:
                df = self._get_history_daily_window(instruments, end_dt, bar_count,
                                                    field, data_frequency)
        elif frequency == "1m":
            if field == "price":
                df = self._get_history_minute_window(instruments, end_dt, bar_count,
                                                     "close")
            else:
                df = self._get_history_minute_window(instruments, end_dt, bar_count,
                                                     field)
        else:
            raise ValueError("Invalid frequency: {0}".format(frequency))

        # forward-fill price
        if field == "price":
            if frequency =="1m":
                ffill_data_frequency = 'minute'
            elif frequency == "1d":
                ffill_data_frequency = 'daily'
            else:
                raise Exception(
                        "Only 1d and 1m are supported for forward-filling.")

            instruments_with_leading_nan = np.where(isnull(df.iloc[0]))[0]

            history_start, history_end = df.index[[0, -1]]
            if ffill_data_frequency == 'daily' and data_frequency == 'minute':
                # When we're looking for a daily value, but we haven't seen any
                # volume in today's minute bars yet, we need to use the
                # previous day's ffilled daily price. Using today's daily price
                # could yield a value from later today.
                history_start -= self.trading_calendar.day

            initial_values = []
            for instrument in df.columns[instruments_with_leading_nan]:
                last_traded = self.get_last_traded_dt(
                    instrument,
                    history_start,
                    ffill_data_frequency,
                )
                if isnull(last_traded):
                    initial_values.append(nan)
                else:
                    initial_values.append(
                        self.get_adjusted_value(
                            instrument,
                            field,
                            dt=last_traded,
                            perspective_dt=history_end,
                            data_frequency=ffill_data_frequency,
                        )
                    )
            # Set leading values for instruments that were missing data, then ffill.
            df.ix[0, instruments_with_leading_nan] = np.array(
                initial_values,
                dtype=np.float64
            )
            df.fillna(method='ffill', inplace=True)

            # forward-filling will incorrectly produce values after the end of
            # an instrument's lifetime, so write NaNs back over the instrument's
            # end_date.
            normed_index = df.index.normalize()
            for instrument in df.columns:
                if history_end >= instrument.end_date:
                    # if the window extends past the instrument's end date, set
                    # all post-end-date values to NaN in that instrument's series
                    df.loc[normed_index > instrument.end_date, instrument] = nan
        return df

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
        if data_frequency == 'daily':
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
            # not supported yet
            daily_data = self._get_daily_window_data(
                instruments,
                field_to_use,
                days_for_window
#                days_for_window[0:-1]
            )
            # bunch of stuff missing here
            return daily_data

    @remember_last
    def _get_days_for_window(self, end_date, bar_count):
        tds = self.trading_calendar.all_sessions
        end_loc = tds.get_loc(end_date)
        start_loc = end_loc - bar_count + 1
        if start_loc < self._first_trading_day_loc:
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
            The instrument whose data is desired.
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

        if field != "volume" or field != "open_interest":
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

    def _get_current_contract(self, continuous_future, dt):
        rf = self._roll_finders[continuous_future.roll_style]
        contract_exchange_symbol = rf.get_contract_center(continuous_future.root_symbol,
                                              dt,
                                              continuous_future.offset)
        if contract_exchange_symbol is None:
            return None
        return self.instrument_finder.retrieve_instrument(contract_exchange_symbol)

    @property
    def adjustment_reader(self):
        return self._adjustment_reader
