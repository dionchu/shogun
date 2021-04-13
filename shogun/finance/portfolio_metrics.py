import pandas as pd
import numpy as np
import uuid

from collections import namedtuple, OrderedDict
from six import iteritems

class Transaction(object):
    __slots__ = ["id", "dt", "price", "instrument", "amount", "price",
                 "commission", "multiplier", "broker_order_id"]

    def __init__(self, instrument, amount, dt, price, commission, multiplier, broker_order_id=None, id=None):
        self.id = self.make_id() if id is None else id
        self.instrument = instrument
        self.amount = amount
        self.dt = dt
        self.price = price
        self.commission = commission
#        self.multiplier = multiplier
        self.multiplier = self.instrument.multiplier
        self.broker_order_id = broker_order_id

    def __getitem__(self, name):
        return self.__dict__[name]

    def __repr__(self):
        template = (
            "{cls}(instrument={instrument}, dt={dt},"
            " amount={amount}, price={price}, commission={commission}, multiplier={multiplier})"
        )

        return template.format(
            cls=type(self).__name__,
            instrument=self.instrument,
            dt=self.dt,
            amount=self.amount,
            price=self.price,
            commission=self.commission,
            multiplier=self.multiplier
        )

    @staticmethod
    def make_id():
        return uuid.uuid4().hex

    def to_dict(self):
        dct = {name: getattr(self, name)
               for name in self.__slots__}

        if self.broker_order_id is None:
            del dct['broker_order_id']

        return dct

class InnerPosition(object):

    def __init__(self,
                 instrument,
                 multiplier,
                 amount=0,
                 cost_basis=0.0,
                 realized_pnl=0.0,
                 unrealized_pnl=0.0,
                 commission=0.0,
                 last_sale_price=0.0,
                 last_sale_date=None):
        self.instrument = instrument
        self.multiplier = multiplier
        self.amount = amount
        self.cost_basis = cost_basis  # per share
        self.realized_pnl = realized_pnl
        self.unrealized_pnl = unrealized_pnl
        self.commission = commission
        self.last_sale_price = last_sale_price
        self.last_sale_date = last_sale_date

    def __repr__(self):
        return (
            '%s(instrument=%r, multiplier=%r,'
            ' amount=%r, cost_basis=%r,'
            ' realized_pnl=%r, unrealized_pnl=%r,'
            ' commission=%r, last_sale_price=%r, last_sale_date=%r)' % (
                type(self).__name__,
                self.instrument,
                self.multiplier,
                self.amount,
                self.cost_basis,
                self.realized_pnl,
                self.unrealized_pnl,
                self.commission,
                self.last_sale_price,
                self.last_sale_date,
            )
        )

from math import copysign

class Position(object):

    def __init__(self,
                 instrument,
                 multiplier=0,
                 amount=0,
                 cost_basis=0.0,
                 realized_pnl=0.0,
                 unrealized_pnl=0.0,
                 dividend=0.0,
                 commission=0.0,
                 last_sale_price=0.0,
                 last_sale_date=None):
        self.instrument = instrument
        self.multiplier = multiplier
        self.amount = amount
        self.cost_basis = cost_basis  # per share
        self.realized_pnl = realized_pnl
        self.unrealized_pnl = unrealized_pnl
        self.dividend = dividend
        self.commission = commission
        self.last_sale_price = last_sale_price
        self.last_sale_date = last_sale_date

    def earn_dividend(self, dividend):
        """
        Register the number of shares we held at this dividend's ex date so
        that we can pay out the correct amount on the dividend's pay date.
        """
        self.dividend += self.amount * dividend.dividend

        return {
            'instrument': self.instrument,
            'holding': self.amount,
            'dividend': dividend.dividend,
            'amount': self.amount * dividend.dividend
        }

    def earn_coupon(self, coupon):
        """
        Register the face value we held at this coupons's pay date so
        that we can pay out the correct amount on the coupon's pay date.
        """
        self.dividend += self.amount * coupon

        return {
            'instrument': self.instrument,
            'holding': self.amount,
            'dividend': coupon,
            'amount': self.amount * coupon
        }

    def update(self, txn):
        if self.instrument != txn.instrument:
            raise Exception('updating position with txn for a '
                            'different instrument')

        if self.multiplier ==0:
            self.multiplier = txn.multiplier

        total_shares = self.amount + txn.amount

        if total_shares ==0:
            self.realized_pnl += (txn.price - self.cost_basis) * self.multiplier * self.amount
            self.cost_basis = 0.0
        else:
            prev_direction = copysign(1, self.amount)
            txn_direction = copysign(1, txn.amount)

            if prev_direction != txn_direction:
                # we're covering a short or closing a position
                if abs(txn.amount) > abs(self.amount):
                    # we've closed the position and gone short
                    # or covered the short position and gone long
                    self.realized_pnl += (txn.price - self.cost_basis) * self.multiplier * self.amount
                    self.cost_basis = txn.price
                else:
                    # we've reduced the position
                    self.realized_pnl += (txn.price - self.cost_basis) * self.multiplier * abs(txn.amount) * prev_direction
            else:
                prev_cost = self.cost_basis * self.amount
                txn_cost = txn.amount * txn.price
                total_cost = prev_cost + txn_cost
                self.cost_basis = total_cost / total_shares

            # Update the last sale price if txn is
            # best data we have so far
            if self.last_sale_date is None or txn.dt > self.last_sale_date:
                self.last_sale_price = txn.price
                self.last_sale_date = txn.dt

        self.amount = total_shares
        self.commission += txn.commission
        self.unrealized_pnl = (self.last_sale_price - self.cost_basis) * self.multiplier * self.amount

    def update_market(self, session, bar_reader):
        dt = session.strftime('%Y-%m-%d')
        if self.amount !=0:
            try:
                self.last_sale_price = bar_reader.get_value(self.instrument.exchange_symbol, session, 'close')
                self.last_sale_date = session
                self.unrealized_pnl = (self.last_sale_price - self.cost_basis) * self.amount * self.multiplier
            except:
                print(self.instrument.exchange_symbol + ' ' + session.strftime("%Y-%m-%d") + ' missing')
                pass

    def to_dict(self):
        """
        Creates a dictionary representing the state of this position.
        Returns a dict object of the form:
        """
        return {
            'instrument': self.instrument,
            'amount': self.amount,
            'multiplier': self.multiplier,
            'cost_basis': self.cost_basis,
            'realized_pnl': self.realized_pnl,
            'unrealized_pnl': self.unrealized_pnl,
            'dividend': self.dividend,
            'commission': self.commission,
            'last_sale_price': self.last_sale_price
        }

class Strategy(object):

    def __init__(self):

        self.positions = OrderedDict()
        self.realized_pnl = 0
        self.unrealized_pnl = 0
        self.commission = 0
        self.dividend = 0
        self._processed_transactions = {}
        self._processed_dividends = {}

    def execute_transaction(self, txn):

        instrument = txn.instrument

        if instrument not in self.positions:
            position = Position(instrument)
            self.positions[instrument] = position
        else:
            position = self.positions[instrument]

        position.update(txn)

        transaction_dict = txn.to_dict()
        try:
            self._processed_transactions[txn.dt].append(
                transaction_dict,
            )
        except KeyError:
            self._processed_transactions[txn.dt] = [transaction_dict]

    def process_dividend(self, instrument, dividend):

        if instrument not in self.positions:
            pass
        else:
            div_owed = self.positions[instrument].earn_dividend(
                dividend
            )

            self._processed_dividends[dividend.ex_date] = div_owed

    def process_coupon(self, session):
        dt = session.strftime('%Y-%m-%d')

        current_positions = [
            pos.to_dict()
            for instrument, pos in iteritems(self.positions)
        ]

        fi_positions = [pos for pos in current_positions if pos['instrument'].__class.__name__ == 'FixedIncome' and pos['instrument'].coupon !=0]
        if len(fi_positions) > 0:
            for fi_position in fi_positions:
                if fi_position['instrument'].get_coupon(dt) == False:
                    pass
                else:
                    div_owed = self.positions[fi_position['instrument']].earn_coupon(
                        fi_position['instrument'].get_coupon(dt)/100
                    )
                    self._processed_dividends[session] = div_owed

    def refresh(self,session, bar_reader):
        self.realized_pnl = 0
        self.unrealized_pnl = 0
        self.commission = 0
        self.dividend = 0
        for instrument, pos in iteritems(self.positions):
            pos.update_market(session, bar_reader)
            self.realized_pnl += pos.realized_pnl
            self.unrealized_pnl += pos.unrealized_pnl
            self.commission -= pos.commission
            self.dividend += pos.dividend

    def get_position_list(self):
        return [
            pos.to_dict()
            for instrument, pos in iteritems(self.positions)
        ]
