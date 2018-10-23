from functools import partial
import pandas as pd
from numpy import array, empty, iinfo
from pandas import Timestamp
from trading_calendars import get_calendar
import warnings

import os
dirname = os.path.dirname(__file__)

def delivery_predicate(codes, contract):
    # This relies on symbols that are construct following a pattern of
    # root symbol + delivery code + year, e.g. PLF16
    # This check would be more robust if the future contract class had
    # a 'delivery_month' member.
    delivery_code = contract.exchange_symbol[-3]
    return delivery_code in codes

ADJUSTMENT_STYLES = {'add', 'mul', None}

class ContinuousFuture(object):
    """
    Represents a specifier for a chain of future contracts, where the
    coordinates for the chain are:
    root_symbol : str
        The root symbol of the contracts.
    offset : int
        The distance from the primary chain.
        e.g. 0 specifies the primary chain, 1 the secondary, etc.
    roll_style : str
        How rolls from contract to contract should be calculated.
        Currently supports 'calendar'.
    Instances of this class are exposed to the algorithm.
    """

    _kwargnames = frozenset({
        'exchange_symbol',
        'root_symbol',
        'offset',
        'start_date',
        'end_date',
        'exchange',
    })

    def __init__(self,
                exchange_symbol,
                root_symbol,
                offset,
                roll_style,
                start_date,
                end_date,
                exchange_info,
                adjustment=None):

        self.exchange_symbol = exchange_symbol
        self.exchange_symbol_hash = hash(exchange_symbol)
        self.root_symbol = root_symbol
        self.roll_style = roll_style
        self.offset = offset
        self.exchange_info = exchange_info
        self.start_date = start_date
        self.end_date = end_date
        self.adjustment = adjustment

    @property
    def exchange(self):
        return self.exchange_info.canonical_name

    @property
    def exchange_full(self):
        return self.exchange_info.name

    # Need to change %d to %s if switch to index number
    def __str__(self):
        return '%s(%s [%s, %s, %s, %s])' % (
            type(self).__name__,
            self.exchange_symbol,
            self.root_symbol,
            self.offset,
            self.roll_style,
            self.adjustment,
        )

    def __repr__(self):
        attrs = ('root_symbol', 'offset', 'roll_style', 'adjustment')
        tuples = ((attr, repr(getattr(self, attr, None)))
                  for attr in attrs)
        strings = ('%s=%s' % (t[0], t[1]) for t in tuples)
        params = ', '.join(strings)
        #will have to change this line if switch to numerica id
        return 'ContinuousFuture(%s, %s)' % (self.exchange_symbol, params)

    def to_dict(self):
        """
        Convert to a python dict.
        """
        return {
            'exchange_symbol': self.exchange_symbol,
            'root_symbol': self.root_symbol,
            'start_date': self.start_date,
            'end_date': self.end_date,
            'offset': self.offset,
            'roll_style': self.roll_style,
            'exchange': self.exchange,
        }

    @classmethod
    def from_dict(cls, dict_):
        """
        Build an ContinuousFuture instance from a dict.
        """
        return cls(**dict_)

    def is_alive_for_session(self, session_label):
        """
        Returns whether the continuous future is alive at the given dt.
        Parameters
        ----------
        session_label: pd.Timestamp
            The desired session label to check. (midnight UTC)
        Returns
        -------
        boolean: whether the continuous is alive at the given dt.
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
        boolean: whether the continuous futures's exchange is open at the
        given minute.
        """
        calendar = get_calendar(self.exchange)
        return calendar.is_open_on_minute(dt_minute)

class ContractNode(object):
    def __init__(self, contract):
        self.contract = contract
        self.prev = None
        self.next = None

        def __rshift__(self, offset):
            i = 0
            curr = self
            while i < offset and curr is not None:
                curr = curr.next
                i += 1
            return curr

        def __lshift__(self, offset):
            i = 0
            curr = self
            while i < offset and curr is not None:
                curr = curr.prev
                i += 1
            return curr

class OrderedContracts(object):
    """
    A container for aligned values of a future contract chain, in sorted order
    of their occurrence.
    Used to get answers about contracts in relation to their auto close
    dates and start dates.
    Members
    -------
    root_symbol : str
        The root symbol of the future contract chain.
    contracts : deque
        The contracts in the chain in order of occurrence.
    start_dates : long[:]
        The start dates of the contracts in the chain.
        Corresponds by index with contract_sids.
    auto_close_date : long[:]
        The auto close dates of the contracts in the chain.
        Corresponds by index with contract_sids.
    chain_predicates : dict
        A dict mapping root symbol to a predicate function which accepts a contract
    as a parameter and returns whether or not the contract should be included in the
    chain.
    """

    def __init__(self, root_symbol, contracts, active=True):
        self._future_contract_listing = pd.read_csv(dirname + "\..\shogun_database\_FutureRootContractListingTable.csv")

        self.root_symbol = root_symbol

        self.exchange_symbol_to_contract = {}

        self._start_date = iinfo('int64').max
        self._end_date = 0

        # assumes that root_symbol is in table, need a check here
        if active:
            chain_predicate = partial(delivery_predicate,
                set(self._future_contract_listing[
                    (self._future_contract_listing['root_symbol'] == self.root_symbol) &
                    (self._future_contract_listing['active'] == 1)
                    ]['delivery_month']))
        else:
            chain_predicate = partial(delivery_predicate,
                set(self._future_contract_listing[
                    (self._future_contract_listing['root_symbol'] == self.root_symbol)
                    ]['delivery_month']))

        self._head_contract = None
        prev = None
        while contracts:
            contract = contracts.popleft()

            # It is possible that the first contract in our list has a start
            # date on or after its auto close date. In that case the contract
            # is not tradable, so do not include it in the chain.
            if prev is None and contract.start_date >= contract.auto_close_date:
                continue

            if not chain_predicate(contract):
                continue

            self._start_date = min(contract.start_date.value, self._start_date)
            self._end_date = max(contract.end_date.value, self._end_date)

            curr = ContractNode(contract)
            self.exchange_symbol_to_contract[contract.exchange_symbol] = curr
            if self._head_contract is None:
                self._head_contract = curr
                prev = curr
                continue
            curr.prev = prev
            prev.next = curr
            prev = curr

    def contract_before_auto_close(self, dt_value):
        """
        Get the contract with next upcoming auto close date.
        """
        curr = self._head_contract
        while curr.next is not None:
            if curr.contract.auto_close_date > dt_value:
                break
            curr = curr.next
        return curr.contract.exchange_symbol

    def contract_at_offset(self, exchange_symbol, offset, start_cap):
        """
        Get the exchange_symbol which is the given exchange_symbol plus the offset distance.
        An offset of 0 should be reflexive.
        """
        curr = self.exchange_symbol_to_contract[exchange_symbol]
        i = 0
        while i < offset:
            if curr.next is None:
                return None
            curr = curr.next
            i += 1
        if curr.contract.start_date.value <= start_cap:
            return curr.contract.exchange_symbol
        else:
            return None

    def active_chain(self, starting_exchange_symbol, dt_value):
        curr = self.exchange_symbol_to_contract[starting_exchange_symbol]
        contracts = []

        while curr is not None:
            if curr.contract.start_date.value <= dt_value:
                contracts.append(curr.contract.exchange_symbol)
            curr = curr.next

        return array(contracts, dtype='str')

    @property
    def start_date(self):
        return Timestamp(self._start_date, tz='UTC')

    @property
    def end_date(self):
        return Timestamp(self._end_date, tz='UTC')
