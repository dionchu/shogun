from trading_calendars import get_calendar


class ExchangeInfo(object):
    """An exchange where assets are traded.
    Parameters
    ----------
    name : str or None
        The full name of the exchange, for example 'NEW YORK STOCK EXCHANGE' or
        'NASDAQ GLOBAL MARKET'.
    canonical_name : str
        The canonical name of the exchange, for example 'XNYS' or 'XASX'. If
        None this will be the same as the name.
    financial_center : FinancialCenter object
        The financial center where exchange is located
    """

    def __init__(self, name, canonical_name, financial_center_info):
        self.name = name

        if canonical_name is None:
            canonical_name = name

        self.canonical_name = canonical_name
        self.financial_center_info = financial_center_info

    def __repr__(self):
        return '%s(%r, %r, %r)' % (
            type(self).__name__,
            self.name,
            self.canonical_name,
            self.financial_center_info.name,
        )

    @property
    def exchange_financial_center(self):
        return self.financial_center_info.name

    @property
    def exchange_country_id(self):
        return self.financial_center_info.country_id

    @property
    def exchange_timezone(self):
        return self.financial_center_info.timezone

    @property
    def calendar(self):
        return get_calendar(self.canonical_name)

    def __eq__(self, other):
        if not isinstance(other, ExchangeInfo):
            return NotImplemented

        return all(
            getattr(self, attr) == getattr(other, attr)
            for attr in ('name', 'canonical_name', 'exchange_financial_center','exchange_country_id','exchange_timezone')
        )

    def __ne__(self, other):
        eq = self == other
        if eq is NotImplemented:
            return NotImplemented
        return not eq
