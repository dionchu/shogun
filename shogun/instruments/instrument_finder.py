import pandas as pd
from collections import deque
from functools import partial
from pandas import read_hdf
from six import viewkeys
from toolz import ( curry, )
from trading_calendars.utils.memoize import lazyval
from .financial_center_info import FinancialCenterInfo
from .exchange_info import ExchangeInfo
from .country_info import CountryInfo
from .query_utils import group_into_chunks
from .functional_utils import invert
from .errors import (
    EquitiesNotFound,
    FutureContractsNotFound,
    SymbolsNotFound,
)
from .instrument import (
    Instrument, Equity, Future,
)
from .continuous_futures import(
    ADJUSTMENT_STYLES,
    OrderedContracts,
    ContinuousFuture
)

import os
dirname = os.path.dirname(__file__)

FUT_CODE_TO_MONTH = dict(zip('FGHJKMNQUVXZ', range(1, 13)))
MONTH_TO_FUT_CODE = dict(zip(range(1, 13), 'FGHJKMNQUVXZ'))

## do I need to make auto_close_date a date as well
# A set of fields that need to be converted to timestamps in UTC
_instrument_timestamp_fields = frozenset({
    'start_date',
    'end_date',
    'first_trade',
    'last_trade',
    'first_position',
    'last_position',
    'first_notice',
    'last_notice',
    'first_delivery',
    'last_delivery',
    'settlement_date',
    'volume_switch_date',
    'open_interest_switch_date',
})

def _convert_instrument_timestamp_fields(dict_):
    """
    Takes in a dict of Instrument init args and converts dates to pd.Timestamps
    """
    for key in _instrument_timestamp_fields & viewkeys(dict_):
        value = pd.Timestamp(dict_[key], tz='UTC')
        dict_[key] = None if isnull(value) else value
    return dict_

@curry
def _filter_kwargs(names, dict_):
    """Filter out kwargs from a dictionary.
    Parameters
    ----------
    names : set[str]
        The names to select from ``dict_``.
    dict_ : dict[str, any]
        The dictionary to select from.
    Returns
    -------
    kwargs : dict[str, any]
        ``dict_`` where the keys intersect with ``names`` and the values are
        not None.
    """
    return {k: v for k, v in dict_.items() if k in names and v is not None}

_filter_future_kwargs = _filter_kwargs(Future._kwargnames)
_filter_equity_kwargs = _filter_kwargs(Equity._kwargnames)

def _generate_continuous_future_symbol(root_symbol,
                                                offset,
                                                roll_style,
                                                adjustment_style=None):

    if not adjustment_style:
        return '_'.join([root_symbol+str(offset),roll_style])
    else:
        return '_'.join([root_symbol+str(offset),roll_style,adjustment_style])


class InstrumentFinder(object):
    """
    An InstrumentFinder is an interface to a database of Asset metadata written by
    an ``AssetDBWriter``.
    This class provides methods for looking up assets by unique integer id or
    by symbol.  For historical reasons, we refer to these unique ids as 'sids'.
    Parameters
    ----------
    engine : str or SQLAlchemy.engine
        An engine with a connection to the asset database to use, or a string
        that can be parsed by SQLAlchemy as a URI.
    future_chain_predicates : dict
        A dict mapping future root symbol to a predicate function which accepts
    a contract as a parameter and returns whether or not the contract should be
    included in the chain.
    See Also
    --------
    :class:`zipline.assets.AssetDBWriter`
    """

    def __init__(self):
        self._country_code = pd.read_csv(dirname + "\..\shogun_database\_CountryCode.csv", keep_default_na=False)
        self._asset_class = pd.read_csv(dirname + "\..\shogun_database\_AssetClass.csv")
        self._currency_code = pd.read_csv(dirname + "\..\shogun_database\_CurrencyCode.csv")
        self._exchange_code = pd.read_csv(dirname + "\..\shogun_database\_ExchangeCode.csv")
        self._financial_center = pd.read_csv(dirname + "\..\shogun_database\_FinancialCenter.csv")
        self._future_contract_listing = pd.read_csv(dirname + "\..\shogun_database\_FutureRootContractListingTable.csv")
        self._future_root = pd.read_csv(dirname + "\..\shogun_database\_FutureRootTable.csv")
        self._future_instrument = read_hdf(dirname + "\..\shogun_database\_FutureInstrument.h5")
        self._equity_instrument = pd.DataFrame()
        self._instrument_router = read_hdf(dirname +'\..\shogun_database\_InstrumentRouter.h5')
        self._instrument_cache = {}
        self._instrument_type_cache = {}
        self._caches = (self._instrument_cache, self._instrument_type_cache)
        self._ordered_contracts = {}

    @lazyval
    def country_info(self):
        out = {}
        for index, row in self._country_code.iterrows():
            out[row['country_name']] = CountryInfo(row['country_name'], row['country_code'], row['country_code3'], row['region'])
        return out

    @lazyval
    def financial_center_info(self):
        out = {}
        for index, row in self._financial_center.iterrows():
            out[row['financial_center']] = FinancialCenterInfo(row['financial_center'], self.country_info[row['country_id']], row['timezone'])
        return out

    @lazyval
    def exchange_info(self):
        out= {}
        for index, row in self._exchange_code.iterrows():
            out[row['mic']] = ExchangeInfo(row['exchange_full'], row['mic'], self.financial_center_info[row['financial_center_id']])
        return out

    def create_continuous_future(self,
                                 root_symbol,
                                 offset,
                                 roll_style,
                                 adjustment):

        if adjustment not in ADJUSTMENT_STYLES:
            raise ValueError(
                'Invalid adjustment style {!r}. Allowed adjustment styles are '
                '{}.'.format(adjustment, list(ADJUSTMENT_STYLES))
            )

        oc = self.get_ordered_contracts(root_symbol)
        exchange = self._get_root_symbol_exchange(root_symbol)

        exchange_symbol = _generate_continuous_future_symbol(root_symbol, offset,
                                                             roll_style, None)

        mul_exchange_symbol = _generate_continuous_future_symbol(root_symbol, offset,
                                                             roll_style, 'div')

        add_exchange_symbol = _generate_continuous_future_symbol(root_symbol, offset,
                                                             roll_style, 'add')


        cf_template = partial(
            ContinuousFuture,
            root_symbol=root_symbol,
            offset=offset,
            roll_style=roll_style,
            start_date=oc.start_date,
            end_date=oc.end_date,
            exchange_info=self.exchange_info[exchange],
        )

        cf = cf_template(exchange_symbol=exchange_symbol)
        mul_cf = cf_template(mul_exchange_symbol, adjustment = 'mul')
        add_cf = cf_template(add_exchange_symbol, adjustment = 'add')

        self._instrument_cache[cf.exchange_symbol] = cf
        self._instrument_cache[mul_cf.exchange_symbol] = mul_cf
        self._instrument_cache[add_cf.exchange_symbol] = add_cf

        return {None: cf, 'mul': mul_cf, 'add': add_cf}[adjustment]

    def get_ordered_contracts(self, root_symbol, active=1):
        try:
            return self._ordered_contracts[root_symbol]
        except KeyError:
            contract_exchange_symbols = self._get_contract_exchange_symbols(root_symbol)
            contracts = deque(self.retrieve_all(contract_exchange_symbols))
            oc = OrderedContracts(root_symbol, contracts, active)
            self._ordered_contracts[root_symbol] = oc
            return oc

    # May need to implement some sorting
    def _get_contract_exchange_symbols(self, root_symbol):
        return list(self._future_instrument[
                self._future_instrument['root_symbol'] == root_symbol
                ].index)

    def _get_root_symbol_exchange(self, root_symbol):
        # assumes there are no dupes in _future_root
        return list(self._future_root[self._future_root['root_symbol'] == root_symbol].exchange_id)[0]

    def retrieve_instrument(self, exchange_symbol, default_none=False):
        """
        Retrieve the Instrument for a given exchange_symbol.
        """
        try:
            instrument = self._instrument_cache[exchange_symbol]
            if instrument is None and not default_none:
                raise SymbolsNotFound(exchange_symbols=[exchange_symbol])
            return instrument
        except KeyError:
            return self.retrieve_all((exchange_symbol,), default_none=default_none)[0]

    def retrieve_all(self, exchange_symbols, default_none=False):
        """
        Retrieve all instruments in `exchange_symbols`.
        Parameters
        ----------
        exchange_symbols : list of strings
            Instruments to retrieve.
        default_none : bool
            If True, return None for failed lookups.
            If False, raise `SymbolsNotFound`.
        Returns
        -------
        instruments : list[Instrument or None]
            A list of the same length as `exchange_symbols` containing Instruments (or Nones)
            corresponding to the requested exchange symbols.
        Raises
        ------
        SymbolsNotFound
            When a requested exchange_symbol is not found and default_none=False.
        """
        hits, missing, failures = {}, set(), []
        for exchange_symbol in exchange_symbols:
            try:
                instrument = self._instrument_cache[exchange_symbol]
                if not default_none and instrument is None:
                    # Bail early if we've already cached that we don't know
                    # about an asset.
                    raise SymbolsNotFound(exchange_symbols=[exchange_symbol])
                hits[exchange_symbol] = instrument
            except KeyError:
                missing.add(exchange_symbol)

        # All requests were cache hits. Return requested exchange_symbols in order.
        if not missing:
            return [hits[exchange_symbol] for exchange_symbol in exchange_symbols]

        update_hits = hits.update

        # Look up cache misses by type.
        type_to_instruments = self.group_by_type(missing)

        # Handle failures
        failures = {failure: None for failure in type_to_instruments.pop(None, ())}
        update_hits(failures)
        self._instrument_cache.update(failures)

        if failures and not default_none:
            raise SymbolsNotFound(exchange_symbols=list(failures))

        # We don't update the instrument cache here because it should already be
        # updated by `self.retrieve_equities` and `self.retrieve_futures`.
        update_hits(self.retrieve_equities(type_to_instruments.pop('Equity', ())))
        update_hits(
            self.retrieve_futures_contracts(type_to_instruments.pop('Future', ()))
        )

        # We shouldn't know about any other asset types.
        if type_to_instruments:
            raise AssertionError(
                "Found instrument types: %s" % list(type_to_instruments.keys())
            )

        return [hits[exchange_symbol] for exchange_symbol in exchange_symbols]

    def group_by_type(self, exchange_symbols):
        """
        Group a list of exchange_symbols by instrument type.
        Parameters
        ----------
        exchange_symbols : list[str]
        Returns
        -------
        types : dict[str or None -> list[str]]
            A dict mapping unique instrument types to lists of exchange_symbols
            drawn from exchange_symbols. If we fail to look up an asset, we
            assign it a key of None.
        """
        return invert(self.lookup_instrument_types(exchange_symbols))

    def lookup_instrument_types(self, exchange_symbols):
        """
        Retrieve instrument types for a list of exchange_symbols.
        Parameters
        ----------
        exchange_symbols : list[str]
        Returns
        -------
        types : dict[exchange_symbols -> str or None]
            Instrument types for the provided exchange_symbols.
        """
        found = {}
        missing = set()

        for exchange_symbol in exchange_symbols:
            try:
                found[exchange_symbol] = self._instrument_type_cache[exchange_symbol]
            except KeyError:
                missing.add(exchange_symbol)

        if not missing:
            return found

        for instruments in group_into_chunks(missing):
            query = self._instrument_router[
                self._instrument_router.index.isin(instruments)
                ].to_dict()['instrument_type'].items()
            for exchange_symbol, instrument_type in query:
                missing.remove(exchange_symbol)
                found[exchange_symbol] = self._instrument_type_cache[exchange_symbol] = instrument_type

            for exchange_symbol in missing:
                found[exchange_symbol] = self._instrument_type_cache[exchange_symbol] = None

        return found

    def retrieve_equities(self, exchange_symbols):
        """
        Retrieve Equity objects for a list of exchange_symbols.
        Users generally shouldn't need to this method (instead, they should
        prefer the more general/friendly `retrieve_instruments`), but it has a
        documented interface and tests because it's used upstream.
        Parameters
        ----------
        exchange_symbols : list[str]
        Returns
        -------
        equities : dict[int -> Equity]
        Raises
        ------
        EquitiesNotFound
            When any requested instrument isn't found.
        """
        return self._retrieve_instruments(exchange_symbols, self._equity_instrument, Equity)

    def retrieve_futures_contracts(self, exchange_symbols):
        """
        Retrieve Future objects for a list of exchange_symbols.
        Users generally shouldn't need to this method (instead, they should
        prefer the more general/friendly `retrieve_instrument`), but it has a
        documented interface and tests because it's used upstream.
        Parameters
        ----------
        sids : list[str]
        Returns
        -------
        futures : dict[int -> Future]
        Raises
        ------
        FuturesContractsNotFound
            When any requested instrument isn't found.
        """
        return self._retrieve_instruments(exchange_symbols, self._future_instrument, Future)

    def _retrieve_instruments(self, exchange_symbols, instrument_hdf, instrument_type):
        """
        Internal function for loading instruments from a table.
        This should be the only method of `InstrumentFinder` that writes instruments
        into self._instrument_cache.
        Parameters
        ---------
        exchange_symbols : list of str
            Instrument ids to look up.
        instrument_hdf : Pandas hdf
            Table from which to query instruments.
        asset_type : type
            Type of instrument to be constructed.
        Returns
        -------
        assets : dict[int -> Instrument]
            Dict mapping requested exchange_symbols to the retrieved instruments.
        """
        # Fastpath for empty request.
        if not exchange_symbols:
            return {}

        cache = self._instrument_cache
        hits = {}

        querying_equities = issubclass(instrument_type, Equity)
        filter_kwargs = (
            _filter_equity_kwargs
            if querying_equities else
            _filter_future_kwargs
        )

        rows = self._retrieve_instrument_dicts(exchange_symbols, instrument_hdf)
        for row in rows:
            exchange_symbol = row['exchange_symbol']
            instrument = instrument_type(**filter_kwargs(row))
            hits[exchange_symbol] = cache[exchange_symbol] = instrument

        # If we get here, it means something in our code thought that a
        # particular sid was an equity/future and called this function with a
        # concrete type, but we couldn't actually resolve the asset.  This is
        # an error in our code, not a user-input error.
        misses = tuple(set(exchange_symbols) - viewkeys(hits))
        if misses:
            if querying_equities:
                raise EquitiesNotFound(exchange_symbols=misses)
            else:
                raise FutureContractsNotFound(exchange_symbols=misses)
        return hits

    def _retrieve_instrument_dicts(self, exchange_symbols, instrument_hdf):
        if not exchange_symbols:
            return

        def mkdict(row, exchanges=self.exchange_info):
            d = dict(row)
            d['exchange_info'] = exchanges[d.pop('exchange_id')]
            return d

        for instruments in group_into_chunks(exchange_symbols):
            # Load misses from the db
            query = self._select_instruments_by_exchange_symbol(instrument_hdf, instruments)
            query.reset_index(level=[0], inplace=True)
            for index, row in query.iterrows():
                #yield _convert_instrument_timestamp_fields(mkdict(row)) # We don't need this, hdf stores pandas object
                yield mkdict(row)

    @staticmethod
    def _select_instruments_by_exchange_symbol(instrument_hdf, exchange_symbols):
        return instrument_hdf[
            instrument_hdf.index.isin(exchange_symbols)
            ]
