import numpy as np
import pandas as pd
from shogun.data_portal.session_bars import SessionBarReader



class ContinuousFutureSessionBarReader(SessionBarReader):

    def __init__(self, bar_reader, roll_finders):
        self._bar_reader = bar_reader
        self._roll_finders = roll_finders

    def load_raw_arrays(self, columns, start_date, end_date, instruments):
        """
        Parameters
        ----------
        fields : list of str
            'sid'
        start_dt: Timestamp
           Beginning of the window range.
        end_dt: Timestamp
           End of the window range.
        sids : list of int
           The instrument identifiers in the window.
        Returns
        -------
        list of np.ndarray
            A list with an entry per field of ndarrays with shape
            (minutes in range, sids) with a dtype of float64, containing the
            values for the respective field over start and end dt range.
        """
        rolls_by_instrument = {}
        for instrument in instruments:
            rf = self._roll_finders[instrument.roll_style]
            rolls_by_instrument[instrument] = rf.get_rolls(
                instrument.root_symbol,
                start_date,
                end_date,
                instrument.offset
            )

        num_sessions = len(
            self.trading_calendar.sessions_in_range(start_date, end_date)
        )
        shape = num_sessions, len(instruments)

        results = []

        tc = self._bar_reader.trading_calendar
        sessions = tc.sessions_in_range(start_date, end_date)

        # Get partitions
        partitions_by_instrument = {}
        for instrument in instruments:
            partitions = []
            partitions_by_instrument[instrument] = partitions

            rolls = rolls_by_instrument[instrument]
            start = start_date

            for roll in rolls:
                exchange_symbol, roll_date = roll
                start_loc = sessions.get_loc(start)

                if roll_date is not None:
                    end = roll_date - sessions.freq
                    end_loc = sessions.get_loc(end)
                else:
                    end = end_date
                    end_loc = len(sessions) - 1

                partitions.append((exchange_symbol, start, end, start_loc, end_loc))

                if roll_date is not None:
                    start = sessions[end_loc + 1]

        for column in columns:
            if column != 'volume' and column != 'exchange_symbol':
                out = np.full(shape, np.nan)
            elif column == 'volume' or column == 'open_interest':
                out = np.zeros(shape, dtype=np.int64)
            elif column == 'exchange_symbol':
                out = np.full(shape, np.nan)
            for i, instrument in enumerate(instruments):
                partitions = partitions_by_instrument[instrument]

                for exchange_symbol, start, end, start_loc, end_loc in partitions:
                    if column != 'exchange_symbol':
                        result = self._bar_reader.load_raw_arrays(
                            [column], start, end, [exchange_symbol])[0][:, 0]
                    else:
                        result = str(exchange_symbol)
                    out[start_loc:end_loc + 1, i] = result

            results.append(out)

        return results

    @property
    def last_available_dt(self):
        """
        Returns
        -------
        dt : pd.Timestamp
            The last session for which the reader can provide data.
        """
        return self._bar_reader.last_available_dt

    @property
    def trading_calendar(self):
        """
        Returns the zipline.utils.calendar.trading_calendar used to read
        the data.  Can be None (if the writer didn't specify it).
        """
        return self._bar_reader.trading_calendar

    @property
    def first_trading_day(self):
        """
        Returns
        -------
        dt : pd.Timestamp
            The first trading day (session) for which the reader can provide
            data.
        """
        return self._bar_reader.first_trading_day

    def get_value(self, continuous_future, dt, field):
        """
        Retrieve the value at the given coordinates.
        Parameters
        ----------
        sid : int
            The instrument identifier.
        dt : pd.Timestamp
            The timestamp for the desired data point.
        field : string
            The OHLVC name for the desired data point.
        Returns
        -------
        value : float|int
            The value at the given coordinates, ``float`` for OHLC, ``int``
            for 'volume'.
        Raises
        ------
        NoDataOnDate
            If the given dt is not a valid market minute (in minute mode) or
            session (in daily mode) according to this reader's tradingcalendar.
        """
        rf = self._roll_finders[continuous_future.roll_style]
        exchange_symbol = (rf.get_contract_center(continuous_future.root_symbol,
                                      dt,
                                      continuous_future.offset))
        return self._bar_reader.get_value(exchange_symbol, dt, field)

    def get_last_traded_dt(self, instrument, dt):
        """
        Get the latest minute on or before ``dt`` in which ``instrument`` traded.
        If there are no trades on or before ``dt``, returns ``pd.NaT``.
        Parameters
        ----------
        instrument : shogun.instruments.Instrument
            The instrument for which to get the last traded minute.
        dt : pd.Timestamp
            The minute at which to start searching for the last traded minute.
        Returns
        -------
        last_traded : pd.Timestamp
            The dt of the last trade for the given instrument, using the input
            dt as a vantage point.
        """
        rf = self._roll_finders[instrument.roll_style]
        sid = (rf.get_contract_center(instrument.root_symbol,
                                      dt,
                                      instrument.offset))
        if sid is None:
            return pd.NaT
        contract = rf.instrument_finder.retrieve_instrument(exchange_symbol)
        return self._bar_reader.get_last_traded_dt(contract, dt)

    @property
    def sessions(self):
        """
        Returns
        -------
        sessions : DatetimeIndex
           All session labels (unioning the range for all instruments) which the
           reader can provide.
        """
        return self._bar_reader.sessions
