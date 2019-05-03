from math import copysign
import numpy as np

from shogun.instruments.instrument import Instrument
import shogun.finance.protocol as sh

class Position(object):
    __slots__ = 'inner_position', 'protocol_position'

    def __init__(self,
                 instrument,
                 amount=0,
                 cost_basis=0.0,
                 last_sale_price=0.0,
                 last_sale_date=None):
        inner = sh.InnerPosition(
            instrument=instrument,
            amount=amount,
            cost_basis=cost_basis,
            last_sale_price=last_sale_price,
            last_sale_date=last_sale_date,
        )
        object.__setattr__(self, 'inner_position', inner)
        object.__setattr__(self, 'protocol_position', sh.Position(inner))

    def __getattr__(self, attr):
        return getattr(self.inner_position, attr)

    def __setattr__(self, attr, value):
        setattr(self.inner_position, attr, value)

    def earn_dividend(self, dividend):
        """
        Register the number of shares we held at this dividend's ex date so
        that we can pay out the correct amount on the dividend's pay date.
        """
        return {
            'amount': self.amount * dividend.dividend
        }

    def earn_stock_dividend(self, stock_dividend):
        """
        Register the number of shares we held at this dividend's ex date so
        that we can pay out the correct amount on the dividend's pay date.
        """
        return {
            'payment_instrument': stock_dividend.payment_instrument,
            'share_count': np.floor(
                self.amount * float(stock_dividend.ratio)
            )
        }

    def handle_split(self, instrument, ratio):
        """
        Update the position by the split ratio, and return the resulting
        fractional share that will be converted into cash.
        Returns the unused cash.
        """
        if self.instrument != instrument:
            raise Exception("updating split with the wrong instrument!")

        # adjust the # of shares by the ratio
        # (if we had 100 shares, and the ratio is 3,
        #  we now have 33 shares)
        # (old_share_count / ratio = new_share_count)
        # (old_price * ratio = new_price)

        # e.g., 33.333
        raw_share_count = self.amount / float(ratio)

        # e.g., 33
        full_share_count = np.floor(raw_share_count)

        # e.g., 0.333
        fractional_share_count = raw_share_count - full_share_count

        # adjust the cost basis to the nearest cent, e.g., 60.0
        new_cost_basis = round(self.cost_basis * ratio, 2)

        self.cost_basis = new_cost_basis
        self.amount = full_share_count

        return_cash = round(float(fractional_share_count * new_cost_basis), 2)

        log.info("after split: " + str(self))
        log.info("returning cash: " + str(return_cash))

        # return the leftover cash, which will be converted into cash
        # (rounded to the nearest cent)
        return return_cash

    def update(self, txn):
        if self.instrument != txn.instrument:
            raise Exception('updating position with txn for a '
                            'different instrument')

        total_shares = self.amount + txn.amount

        if total_shares == 0:
            self.cost_basis = 0.0
        else:
            prev_direction = copysign(1, self.amount)
            txn_direction = copysign(1, txn.amount)

            if prev_direction != txn_direction:
                # we're covering a short or closing a position
                if abs(txn.amount) > abs(self.amount):
                    # we've closed the position and gone short
                    # or covered the short position and gone long
                    self.cost_basis = txn.price
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

    def adjust_commission_cost_basis(self, instrument, cost):
        """
        A note about cost-basis in zipline: all positions are considered
        to share a cost basis, even if they were executed in different
        transactions with different commission costs, different prices, etc.
        Due to limitations about how zipline handles positions, zipline will
        currently spread an externally-delivered commission charge across
        all shares in a position.
        """

        if instrument != self.instrument:
            raise Exception('Updating a commission for a different instrument?')
        if cost == 0.0:
            return

        # If we no longer hold this position, there is no cost basis to
        # adjust.
        if self.amount == 0:
            return

        # We treat cost basis as the share price where we have broken even.
        # For longs, commissions cause a relatively straight forward increase
        # in the cost basis.
        #
        # For shorts, you actually want to decrease the cost basis because you
        # break even and earn a profit when the share price decreases.
        #
        # Shorts are represented as having a negative `amount`.
        #
        # The multiplication and division by `amount` cancel out leaving the
        # cost_basis positive, while subtracting the commission.

        prev_cost = self.cost_basis * self.amount
        if isinstance(instrument, Future):
            cost_to_use = cost / instrument.price_multiplier
        else:
            cost_to_use = cost
        new_cost = prev_cost + cost_to_use
        self.cost_basis = new_cost / self.amount

    def __repr__(self):
        template = "instrument: {instrument}, amount: {amount}, cost_basis: {cost_basis}, \
last_sale_price: {last_sale_price}"
        return template.format(
            instrument=self.instrument,
            amount=self.amount,
            cost_basis=self.cost_basis,
            last_sale_price=self.last_sale_price
        )

    def to_dict(self):
        """
        Creates a dictionary representing the state of this position.
        Returns a dict object of the form:
        """
        return {
            'exchange_symbol': self.instrument,
            'amount': self.amount,
            'cost_basis': self.cost_basis,
            'last_sale_price': self.last_sale_price
        }
