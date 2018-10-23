

class FutureRootInfo(object):
    """A future root is a class of future contracts with the same contract
    specifications.

    Parameters
    ----------
    root_id : str or None
        The future root id, for example 'ZN' or
        'ES'.
    canonical_name : str
        The canonical name of the exchange, for example 'XNYS' or 'XASX'. If
        None this will be the same as the name.
    financial_center : str
        The financial center where exchange is located
    """

    _kwargnames = frozenset({
        'symbol_id',
        'root_symbol',
        'instrument_name',
        'instrument_country_id',
        'asset_class_id',
        'settle_start',
        'settle_end',
        'settle_method',
        'settle_timezone',
        'final_settle_start',
        'final_settle_end',
        'final_settle_method',
        'final_settle_timezone',
        'last_trade_time'
#        'first_traded',
        'quote_currency_id',
        'multiplier',
        'tick_size',
        'start_date',
        'end_date',
        'first_trade',
        'last_trade',
        'first_position',
        'last_position',
        'first_notice_date',
        'last_notice_date',
        'first_delivery_date',
        'last_delivery_date',
        'settlement_date',
        'volume_switch_date',
        'open_interest_switch_date',
        'auto_close_date',
        'delivery_month',
        'delivery_year',
    })
