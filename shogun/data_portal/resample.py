from abc import ABCMeta, abstractmethod
from shogun.data_portal.bar_reader import NoDataOnDate
from shogun.data_portal.session_bars import SessionBarReader
from shogun.utils.memoize import lazyval
from six import with_metaclass

class ReindexBarReader(with_metaclass(ABCMeta)):
    """
    A base class for readers which reindexes results, filling in the additional
    indices with empty data.
    Used to align the reading assets which trade on different calendars.
    Currently only supports a ``trading_calendar`` which is a superset of the
    ``reader``'s calendar.
    Parameters
    ----------
    - trading_calendar : zipline.utils.trading_calendar.TradingCalendar
       The calendar to use when indexing results from the reader.
    - reader : MinuteBarReader|SessionBarReader
       The reader which has a calendar that is a subset of the desired
       ``trading_calendar``.
    - first_trading_session : pd.Timestamp
       The first trading session the reader should provide. Must be specified,
       since the ``reader``'s first session may not exactly align with the
       desired calendar. Specifically, in the case where the first session
       on the target calendar is a holiday on the ``reader``'s calendar.
    - last_trading_session : pd.Timestamp
       The last trading session the reader should provide. Must be specified,
       since the ``reader``'s last session may not exactly align with the
       desired calendar. Specifically, in the case where the last session
       on the target calendar is a holiday on the ``reader``'s calendar.
    """

    def __init__(self,
                 trading_calendar,
                 reader,
                 first_trading_session,
                 last_trading_session):
        self._trading_calendar = trading_calendar
        self._reader = reader
        self._first_trading_session = first_trading_session
        self._last_trading_session = last_trading_session

    @property
    def last_available_dt(self):
        return self._reader.last_available_dt

    def get_last_traded_dt(self, sid, dt):
        return self._reader.get_last_traded_dt(sid, dt)

    @property
    def first_trading_day(self):
        return self._reader.first_trading_day

    def get_value(self, exchange_symbol, dt, field):
        # Give an empty result if no data is present.
        try:
            return self._reader.get_value(exchange_symbol, dt, field)
        except NoDataOnDate:
            if field == 'volume' or 'open_interest':
                return 0
            else:
                return np.nan

    @abstractmethod
    def _outer_dts(self, start_dt, end_dt):
        raise NotImplementedError

    @abstractmethod
    def _inner_dts(self, start_dt, end_dt):
        raise NotImplementedError

    @property
    def trading_calendar(self):
        return self._trading_calendar

    @lazyval
    def sessions(self):
        return self.trading_calendar.sessions_in_range(
            self._first_trading_session,
            self._last_trading_session
        )

    def load_raw_arrays(self, fields, start_dt, end_dt, sids):
        outer_dts = self._outer_dts(start_dt, end_dt)
        inner_dts = self._inner_dts(start_dt, end_dt)

        indices = outer_dts.searchsorted(inner_dts)

        shape = len(outer_dts), len(sids)

        outer_results = []

        if len(inner_dts) > 0:
            inner_results = self._reader.load_raw_arrays(
                fields, inner_dts[0], inner_dts[-1], sids)
        else:
            inner_results = None

        for i, field in enumerate(fields):
            if field != 'volume' and field != 'open_interest':
                out = np.full(shape, np.nan)
            else:
                out = np.zeros(shape, dtype=np.uint32)

            if inner_results is not None:
                out[indices] = inner_results[i]

            outer_results.append(out)

        return outer_results


class ReindexSessionBarReader(ReindexBarReader, SessionBarReader):
    """
    See: ``ReindexBarReader``
    """

    def _outer_dts(self, start_dt, end_dt):
        return self.trading_calendar.sessions_in_range(start_dt, end_dt)

    def _inner_dts(self, start_dt, end_dt):
        return self._reader.trading_calendar.sessions_in_range(
            start_dt, end_dt)
