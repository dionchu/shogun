from trading_calendars import get_calendar

class Instrument(object):
    """
    An Instrument represents the metadata of a symbol
    """
    _kwargnames = frozenset({
        'exchange_symbol',
        'instrument_name',
        'instrument_country_id',
        'underlying_name',
        'underlying_asset_class_id',
        'settle_start',
        'settle_end',
        'settle_method',
        'settle_timezone',
        'quote_currency_id',
        'multiplier',
        'tick_size',
        'start_date',
        'end_date',
        'exchange_info',
        'parent_calendar_id',
        'child_calendar_id'
    })

    def __init__(self,
                exchange_symbol="",
                instrument_name="",
                instrument_country_id="",
                underlying_name="",
                underlying_asset_class_id="",
                settle_start=None,
                settle_end=None,
                settle_method=None,
                settle_timezone=None,
                quote_currency_id="",
                multiplier=1,
                tick_size=0.01,
                start_date=None,
                end_date=None,
                exchange_info=None,
                parent_calendar_id=None,
                child_calendar_id=None):

        self.exchange_symbol = exchange_symbol
        self.instrument_name = instrument_name
        self.instrument_country_id = instrument_country_id
        self.underlying_name = underlying_name
        self.underlying_asset_class_id = underlying_asset_class_id
        self.settle_start = settle_start
        self.settle_end = settle_end
        self.settle_method = settle_method
        self.settle_timezone = settle_timezone
        self.quote_currency_id = quote_currency_id
        self.multiplier = multiplier
        self.tick_size = tick_size
        self.start_date = start_date
        self.end_date = end_date
        self.exchange_info = exchange_info
        self.parent_calendar_id = parent_calendar_id
        self.child_calendar_id = child_calendar_id

    @property
    def exchange(self):
        return self.exchange_info.canonical_name

    @property
    def exchange_full(self):
        return self.exchange_info.name

    @property
    def exchange_country_id(self):
        return self.exchange_info.exchange_country_id

    @property
    def exchange_financial_center(self):
        return self.exchange_info.exchange_financial_center

    @property
    def exchange_timezone(self):
        return self.exchange_info.exchange_timezone

    ## Quantopian has some equality checkers not implements here

    def __repr__(self):
        if self.symbol:
            return '%s(%s [%s])' % (type(self).__name__, self.symbol_id, self.instrument_name)
        else:
            return '%s(%s)' % (type(self).__name__, self.symbol_id
            )

    ## Quantopian has some function used by pickle to determine how to serialize/deserialized this class

    def to_dict(self):
        """
        Convert to a python dict.
        """
        return {
            'exchange_symbol': self.exchange_symbol,
            'instrument_name': self.instrument_name,
            'instrument_country_id': self.instrument_country_id,
            'underlying_name': self.underlying_name,
            'underlying_asset_class_id': self.underlying_asset_class_id,
            'settle_start': self.settle_start,
            'settle_end': self.settle_end,
            'settle_method': self.settle_method,
            'settle_timezone': self.settle_timezone,
            'quote_currency_id': self.quote_currency_id,
            'multiplier': self.multiplier,
            'tick_size': self.tick_size,
            'start_date': self.start_date,
            'end_date': self.end_date,
            'exchange_info': self.exchange_info,
            'parent_calendar_id': self.parent_calendar_id,
            'child_calendar_id': self.child_calendar_id,
            'exchange_info': self.exchange_info,
            'exchange': self.exchange,
            'exchange_full': self.exchange_full,
            'exchange_financial_center': self.exchange_financial_center,
            'exchange_country_id': self.exchange_country_id,
            'exchange_timezone': self.exchange_financial_center,
        }

    @classmethod
    def from_dict(cls, dict_):
        """
        Build an Asset instance from a dict.
        """
        return cls(**{k: v for k, v in dict_.items() if k in cls._kwargnames})

    def is_alive_for_session(self, session_label):
        """
        Returns whether the asset is alive at the given dt.
        Parameters
        ----------
        session_label: pd.Timestamp
            The desired session label to check. (midnight UTC)
        Returns
        -------
        boolean: whether the asset is alive at the given dt.
        """

        ref_start = self.start_date.value
        ref_end = self.end_date.value

        return ref_start <= session_label.value <= ref_end

    def is_exchange_open(self, dt_minute):
        """
        Parameters
        ----------
        dt_minute: pd.Timestamp (UTC, tz-aware)
            The minute to check.
        Returns
        -------
        boolean: whether the asset's exchange is open at the given minute.
        """
        calendar = get_calendar(self.exchange)
        return calendar.is_open_on_minute(dt_minute)

class Future(Instrument):

    _kwargnames = frozenset({
        'exchange_symbol',
        'root_symbol',
        'instrument_name',
        'instrument_country_id',
        'underlying_name',
        'underlying_asset_class_id',
        'settle_start',
        'settle_end',
        'settle_method',
        'settle_timezone',
        'final_settle_start',
        'final_settle_end',
        'final_settle_method',
        'final_settle_timezone',
        'last_trade_time'
        'quote_currency_id',
        'multiplier',
        'tick_size',
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
        'auto_close_date',
        'exchange_info',
        'parent_calendar_id',
        'child_calendar_id'
        'average_pricing',
        'deliverable',
        'delivery_month',
        'delivery_year',
    })

    def __init__(self,
                exchange_symbol="",
                root_symbol="",
                instrument_name="",
                instrument_country_id="",
                underlying_name="",
                underlying_asset_class_id="",
                settle_start=None,
                settle_end=None,
                settle_method=None,
                settle_timezone=None,
                final_settle_start=None,
                final_settle_end=None,
                final_settle_method=None,
                final_settle_timezone=None,
                last_trade_time=None,
                quote_currency_id="",
                multiplier=1,
                tick_size=0.01,
                start_date=None,
                end_date=None,
                first_trade=None,
                last_trade=None,
                first_position=None,
                last_position=None,
                first_notice=None,
                last_notice=None,
                first_delivery=None,
                last_delivery=None,
                settlement_date=None,
                volume_switch_date=None,
                open_interest_switch_date=None,
                auto_close_date=None,
                exchange_info=None,
                parent_calendar_id=None,
                child_calendar_id=None,
                average_pricing=0,
                deliverable="",
                delivery_month="",
                delivery_year=""):

        super().__init__(
            exchange_symbol=exchange_symbol,
            instrument_name=instrument_name,
            instrument_country_id=instrument_country_id,
            underlying_name=underlying_name,
            underlying_asset_class_id=underlying_asset_class_id,
            settle_start=settle_start,
            settle_end=settle_end,
            settle_method=settle_method,
            settle_timezone=settle_timezone,
            quote_currency_id=quote_currency_id,
            multiplier=multiplier,
            tick_size=tick_size,
            start_date=start_date,
            end_date=end_date,
            exchange_info = exchange_info,
            parent_calendar_id=parent_calendar_id,
            child_calendar_id=child_calendar_id
        )
        self.root_symbol = root_symbol
        self.final_settle_start = final_settle_start
        self.final_settle_end = final_settle_end
        self.final_settle_method = final_settle_method
        self.final_settle_timezone = final_settle_timezone
        self.last_trade_time = last_trade_time
        self.first_trade = first_trade
        self.last_trade = last_trade
        self.first_position = first_position
        self.last_position = last_position
        self.first_notice = first_notice
        self.last_notice = last_notice
        self.first_delivery = first_delivery
        self.last_delivery = last_delivery
        self.settlement_date = settlement_date
        self.volume_switch_date = volume_switch_date
        self.open_interest_switch_date = open_interest_switch_date
        self.average_pricing = average_pricing
        self.deliverable = deliverable
        self.delivery_month = delivery_month
        self.delivery_year = delivery_year

        if auto_close_date is None:
            if first_notice is None:
                self.auto_close_date = last_trade
            else:
                self.auto_close_date = first_notice
        else:
            self.auto_close_date = eval(auto_close_date)

    def to_dict(self):
        """
        Convert to a python dict.
        """
        super_dict = super(Future, self).to_dict()
        super_dict['root_symbol'] = self.root_symbol
        super_dict['final_settle_start'] = self.final_settle_start
        super_dict['final_settle_end'] = self.final_settle_end
        super_dict['final_settle_method'] = self.final_settle_method
        super_dict['final_settle_timezone'] = self.final_settle_timezone
        super_dict['last_trade_time'] = self.last_trade_time
        super_dict['first_trade'] = self.first_trade
        super_dict['last_trade'] = self.last_trade
        super_dict['first_position'] = self.first_position
        super_dict['last_position'] = self.last_position
        super_dict['first_notice'] = self.first_notice
        super_dict['last_notice'] = self.last_notice
        super_dict['first_delivery'] = self.first_delivery
        super_dict['last_delivery'] = self.last_delivery
        super_dict['settlement_date'] = self.settlement_date
        super_dict['volume_switch_date'] = self.volume_switch_date
        super_dict['open_interest_switch_date'] = self.open_interest_switch_date
        super_dict['average_pricing'] = self.average_pricing
        super_dict['deliverable'] = self.deliverable
        super_dict['delivery_month'] = self.delivery_month
        super_dict['delivery_year'] = self.delivery_year


class Equity(Instrument):

    _kwargnames = frozenset({
        'exchange_symbol',
        'root_symbol',
        'instrument_name',
        'instrument_country_id',
        'underlying_name',
        'underlying_asset_class_id',
        'settle_start',
        'settle_end',
        'settle_method',
        'settle_timezone',
        'final_settle_start',
        'final_settle_end',
        'final_settle_method',
        'final_settle_timezone',
        'last_trade_time'
        'quote_currency_id',
        'multiplier',
        'tick_size',
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
        'auto_close_date',
        'exchange_info',
        'parent_calendar_id',
        'child_calendar_id'
        'average_pricing',
        'deliverable',
        'delivery_month',
        'delivery_year',
    })
