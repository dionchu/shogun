from textwrap import dedent

from trading_calendars.utils.memoize import lazyval

class ShogunError(Exception):
    msg = None

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    @lazyval
    def message(self):
        return str(self)

    def __str__(self):
        msg = self.msg.format(**self.kwargs)
        return msg

    __unicode__ = __str__
    __repr__ = __str__


class SymbolsNotFound(ShogunError):
    """
    Raised when a retrieve_instruments() or retrieve_all() call contains a
    non-existent sid.
    """
    @lazyval
    def plural(self):
        return len(self.sids) > 1

    @lazyval
    def sids(self):
        return self.kwargs['exchange_symbols']

    @lazyval
    def msg(self):
        if self.plural:
            return "No instruments found for exchange symbols: {exchange_symbols}."
        return "No instrument found for exchange_symbol: {exchange_symbols[0]}."

class ExchangeSymbolsNotFound(ShogunError):
    """
    Raised when a retrieve_instrument() or retrieve_all() call contains a
    non-existent exchange_symbol.
    """
    @lazyval
    def plural(self):
        return len(self.exchange_symbols) > 1

    @lazyval
    def exchange_symbols(self):
        return self.kwargs['exchange_symbols']

    @lazyval
    def msg(self):
        if self.plural:
            return "No assets found for exchange_symbols: {exchange_symbols}."
        return "No asset found for exchange_symbol: {exchange_symbols[0]}."

class EquitiesNotFound(ExchangeSymbolsNotFound):
    """
    Raised when a call to `retrieve_equities` fails to find an instrument.
    """
    @lazyval
    def msg(self):
        if self.plural:
            return "No equities found for exchange_symbols: {exchange_symbols}."
        return "No equity found for exchange_symbol: {exchange_symbols[0]}."


class FutureContractsNotFound(ExchangeSymbolsNotFound):
    """
    Raised when a call to `retrieve_futures_contracts` fails to find an instrument.
    """
    @lazyval
    def msg(self):
        if self.plural:
            return "No future contracts found for exchange_symbols: {exchange_symbols}."
        return "No future contract found for exchange_symbol: {exchange_symbols[0]}."

class HistoryWindowStartsBeforeData(ShogunError):
    msg = (
        "History window extends before {first_trading_day}. To use this "
        "history window, start the backtest on or after {suggested_start_day}."
        )
