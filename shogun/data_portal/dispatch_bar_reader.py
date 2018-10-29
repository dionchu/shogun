#
# Copyright 2016 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from abc import ABCMeta, abstractmethod

from numpy import (
    full,
    nan,
    int64,
    zeros
)
from six import iteritems, with_metaclass
from itertools import chain

from shogun.utils.memoize import lazyval


class InstrumentDispatchBarReader(with_metaclass(ABCMeta)):
    """
    Parameters
    ----------
    - trading_calendar : zipline.utils.trading_calendar.TradingCalendar
    - instrument_finder : zipline.instrument.InstrumentFinder
    - readers : dict
        A dict mapping Instrument type to the corresponding
        [Minute|Session]BarReader
    - last_available_dt : pd.Timestamp or None, optional
        If not provided, infers it by using the min of the
        last_available_dt values of the underlying readers.
    """
    def __init__(
        self,
        trading_calendar,
        instrument_finder,
        readers,
        last_available_dt=None,
    ):
        self._trading_calendar = trading_calendar
        self._instrument_finder = instrument_finder
        self._readers = readers
        self._last_available_dt = last_available_dt

        for t, r in iteritems(self._readers):
            assert trading_calendar == r.trading_calendar, \
                "All readers must share target trading_calendar. " \
                "Reader={0} for type={1} uses calendar={2} which does not " \
                "match the desired shared calendar={3} ".format(
                    r, t, r.trading_calendar, trading_calendar)

    @abstractmethod
    def _dt_window_size(self, start_dt, end_dt):
        pass

    @property
    def _instrument_types(self):
        return self._readers.keys()

    def _make_raw_array_shape(self, start_dt, end_dt, num_exchange_symbols):
        return self._dt_window_size(start_dt, end_dt), num_exchange_symbols

    def _make_raw_array_out(self, field, shape):
        if field != 'volume' and field != 'exchange_symbol':
            out = full(shape, nan)
        else:
            out = zeros(shape, dtype=int64)
        return out

    @property
    def trading_calendar(self):
        return self._trading_calendar

    @lazyval
    def last_available_dt(self):
        if self._last_available_dt is not None:
            return self._last_available_dt
        else:
            return max(r.last_available_dt for r in self._readers.values())

    @lazyval
    def first_trading_day(self):
        return min(r.first_trading_day for r in self._readers.values())

    def get_value(self, exchange_symbol, dt, field):
        #changed 10/29
        instrument = self._instrument_finder.retrieve_instrument(exchange_symbol)
        r = self._readers[type(instrument)]
        return r.get_value(instrument.exchange_symbol, dt, field)

    def get_last_traded_dt(self, instrument, dt):
        r = self._readers[type(instrument)]
        return r.get_last_traded_dt(instrument, dt)

    def load_raw_arrays(self, fields, start_dt, end_dt, exchange_symbols):
        instrument_types = self._instrument_types
        exchange_symbol_groups = {t: [] for t in instrument_types}
        out_pos = {t: [] for t in instrument_types}

        instruments = [t for t in exchange_symbols if not isinstance(t, str)]
        if len([t for t in exchange_symbols if isinstance(t, str)]):
            instruments = list(chain(instruments, self._instrument_finder.retrieve_all([t for t in exchange_symbols if isinstance(t, str)])))

        for i, instrument in enumerate(instruments):
            t = type(instrument)
            exchange_symbol_groups[t].append(instrument)
            out_pos[t].append(i)

        batched_arrays = {
            t: self._readers[t].load_raw_arrays(fields,
                                                start_dt,
                                                end_dt,
                                                exchange_symbol_groups[t])
            for t in instrument_types if exchange_symbol_groups[t]}

        results = []
        shape = self._make_raw_array_shape(start_dt, end_dt, len(exchange_symbols))

        for i, field in enumerate(fields):
            out = self._make_raw_array_out(field, shape)
            for t, arrays in iteritems(batched_arrays):
                out[:, out_pos[t]] = arrays[i]
            results.append(out)

        return results


class InstrumentDispatchSessionBarReader(InstrumentDispatchBarReader):

    def _dt_window_size(self, start_dt, end_dt):
        return len(self.trading_calendar.sessions_in_range(start_dt, end_dt))

    @lazyval
    def sessions(self):
        return self.trading_calendar.sessions_in_range(
            self.first_trading_day,
            self.last_available_dt)
