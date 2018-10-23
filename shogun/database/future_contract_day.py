import pandas as pd
import warnings
from pandas.errors import PerformanceWarning

class FutureContractDay(object):
    """
    Class that defines a future contract day
    """

    def __init__(self, root_symbol=None, name=None, day=None, offset=None,
                 observance=None, reference_dates=None):
        """
        Parameters
        ----------
        day : int
            day of delivery month, if applicable
        name : str
            Name of the contract day, defaults to class name
        day : int
            day of delivery month, if applicable
        offset : array of pandas.tseries.offsets or
                class from pandas.tseries.offsets
            computes offset from date
        observance: function
            computes when holiday is given a pandas Timestamp
        delivery_month:
            provide a tuple of delivery months e.g  (H,M,U,Z)
        """

        self.root_symbol = root_symbol
        self.name = name
        self.day = day if not None else 1
        self.offset = offset
        self.observance = observance
        self.reference_dates = reference_dates

    def __repr__(self):
        info = ''

        info += 'day={day}, '.format(day=self.day)

        if self.offset is not None:
            info += 'offset={offset}'.format(offset=self.offset)

        if self.observance is not None:
            info += 'observance={obs}'.format(obs=self.observance)

        repr = '{root_symbol}: {name} ({info})'.format(root_symbol=self.root_symbol, name=self.name, info=info)
        return repr

    @property
    def dates(self):
        """
        Calculate contract dates for datetimeindex dates
        Parameters
        ----------
        dates: datetimeindex
        """

        contract_dates = pd.to_datetime(self.reference_dates.to_series().apply(lambda dt: dt.replace(day=self.day)).values)

        if self.offset is not None:
            if not isinstance(self.offset, list):
                offsets = [self.offset]
            else:
                offsets = self.offset
            for offset in offsets:

                # if we are adding a non-vectorized value
                # ignore the PerformanceWarnings:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", PerformanceWarning)
                    contract_dates += offset

        if self.observance is not None:
            contract_dates = contract_dates.map(lambda d: self.observance(d))

        return contract_dates
