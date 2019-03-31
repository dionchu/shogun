from collections import namedtuple, OrderedDict
from functools import partial
from math import isnan

import numpy as np
import pandas as pd
from six import iteritems, itervalues, PY2

from shogun.instruments.instrument import Future
from shogun.finance.position import Position
from shogun.finance.transaction import Transaction
import shogun.finance.protocol as sh
from shogun.utils.sentinel import sentinel

from shogun.finance._finance_ext import (
    PositionStats,
    calculate_position_tracker_stats,
    update_position_last_sale_prices,
)


class PositionTracker(object):
    """The current state of the positions held.
    Parameters
    ----------
    data_frequency : {'daily', 'minute'}
        The data frequency of the simulation.
    """
    def __init__(self, data_frequency):
        self.positions = OrderedDict()

        self._unpaid_dividends = {}
        self._unpaid_stock_dividends = {}
        self._positions_store = sh.Positions()

        self.data_frequency = data_frequency

        # cache the stats until something alters our positions
        self._dirty_stats = True
        self._stats = PositionStats.new()

    def update_position(self,
                        instrument,
                        amount=None,
                        last_sale_price=None,
                        last_sale_date=None,
                        cost_basis=None):
        self._dirty_stats = True

        if instrument not in self.positions:
            position = Position(instrument)
            self.positions[instrument] = position
        else:
            position = self.positions[instrument]

        if amount is not None:
            position.amount = amount
        if last_sale_price is not None:
            position.last_sale_price = last_sale_price
        if last_sale_date is not None:
            position.last_sale_date = last_sale_date
        if cost_basis is not None:
            position.cost_basis = cost_basis

    def execute_transaction(self, txn):
        self._dirty_stats = True

        instrument = txn.instrument

        if instrument not in self.positions:
            position = Position(instrument)
            self.positions[instrument] = position
        else:
            position = self.positions[instrument]

        position.update(txn)

        if position.amount == 0:
            del self.positions[instrument]

            try:
                # if this position exists in our user-facing dictionary,
                # remove it as well.
                del self._positions_store[instrument]
            except KeyError:
                pass

    def handle_commission(self, instrument, cost):
        # Adjust the cost basis of the stock if we own it
        if instrument in self.positions:
            self._dirty_stats = True
            self.positions[instruments].adjust_commission_cost_basis(instrument, cost)

    def handle_splits(self, splits):
        """Processes a list of splits by modifying any positions as needed.
        Parameters
        ----------
        splits: list
            A list of splits.  Each split is a tuple of (instrument, ratio).
        Returns
        -------
        int: The leftover cash from fractional shares after modifying each
            position.
        """
        total_leftover_cash = 0

        for instrument, ratio in splits:
            if instrument in self.positions:
                self._dirty_stats = True

                # Make the position object handle the split. It returns the
                # leftover cash from a fractional share, if there is any.
                position = self.positions[instrument]
                leftover_cash = position.handle_split(instrument, ratio)
                total_leftover_cash += leftover_cash

        return total_leftover_cash

    def earn_dividends(self, cash_dividends, stock_dividends):
        """Given a list of dividends whose ex_dates are all the next trading
        day, calculate and store the cash and/or stock payments to be paid on
        each dividend's pay date.
        Parameters
        ----------
        cash_dividends : iterable of (instrument, amount, pay_date) namedtuples
        stock_dividends: iterable of (instrument, payment_instrument, ratio, pay_date)
            namedtuples.
        """
        for cash_dividend in cash_dividends:
            self._dirty_stats = True  # only mark dirty if we pay a dividend

            # Store the earned dividends so that they can be paid on the
            # dividends' pay_dates.
            div_owed = self.positions[cash_dividend.instrument].earn_dividend(
                cash_dividend,
            )
            try:
                self._unpaid_dividends[cash_dividend.pay_date].append(div_owed)
            except KeyError:
                self._unpaid_dividends[cash_dividend.pay_date] = [div_owed]

        for stock_dividend in stock_dividends:
            self._dirty_stats = True  # only mark dirty if we pay a dividend

            div_owed = self.positions[
                stock_dividend.instrument
            ].earn_stock_dividend(stock_dividend)
            try:
                self._unpaid_stock_dividends[stock_dividend.pay_date].append(
                    div_owed,
                )
            except KeyError:
                self._unpaid_stock_dividends[stock_dividend.pay_date] = [
                    div_owed,
                ]

    def pay_dividends(self, next_trading_day):
        """
        Returns a cash payment based on the dividends that should be paid out
        according to the accumulated bookkeeping of earned, unpaid, and stock
        dividends.
        """
        net_cash_payment = 0.0

        try:
            payments = self._unpaid_dividends[next_trading_day]
            # Mark these dividends as paid by dropping them from our unpaid
            del self._unpaid_dividends[next_trading_day]
        except KeyError:
            payments = []

        # representing the fact that we're required to reimburse the owner of
        # the stock for any dividends paid while borrowing.
        for payment in payments:
            net_cash_payment += payment['amount']

        # Add stock for any stock dividends paid.  Again, the values here may
        # be negative in the case of short positions.
        try:
            stock_payments = self._unpaid_stock_dividends[next_trading_day]
        except KeyError:
            stock_payments = []

        for stock_payment in stock_payments:
            payment_instrument = stock_payment['payment_instrument']
            share_count = stock_payment['share_count']
            # note we create a Position for stock dividend if we don't
            # already own the instrument
            if payment_instrument in self.positions:
                position = self.positions[payment_instrument]
            else:
                position = self.positions[payment_instrument] = Position(
                    payment_instrument,
                )

            position.amount += share_count

        return net_cash_payment

    def get_positions(self):
        positions = self._positions_store

        for instrument, pos in iteritems(self.positions):
            # Adds the new position if we didn't have one before, or overwrite
            # one we have currently
            positions[instrument] = pos.protocol_position

        return positions

    def get_position_list(self):
        return [
            pos.to_dict()
            for instrument, pos in iteritems(self.positions)
            if pos.amount != 0
        ]

    def sync_last_sale_prices(self,
                              dt,
                              data_portal,
                              handle_non_market_minutes=False):
        self._dirty_stats = True

        if handle_non_market_minutes:
            previous_minute = data_portal.trading_calendar.previous_minute(dt)
            get_price = partial(
                data_portal.get_adjusted_value,
                field='price',
                dt=previous_minute,
                perspective_dt=dt,
                data_frequency=self.data_frequency,
            )

        else:
            get_price = partial(
                data_portal.get_scalar_instrument_spot_value,
                field='price',
                dt=dt,
                data_frequency=self.data_frequency,
            )

        update_position_last_sale_prices(self.positions, get_price, dt)

    @property
    def stats(self):
        """The current status of the positions.
        Returns
        -------
        stats : PositionStats
            The current stats position stats.
        Notes
        -----
        This is cached, repeated access will not recompute the stats until
        the stats may have changed.
        """
        if self._dirty_stats:
            calculate_position_tracker_stats(self.positions, self._stats)
            self._dirty_stats = False

        return self._stats

not_overridden = sentinel(
    'not_overridden',
    'Mark that an account field has not been overridden',
)

class Ledger(object):

    def __init__(self, trading_sessions, capital_base, data_frequency):
        if len(trading_sessions):
            start = trading_sessions[0]
        else:
            start = None

        self.daily_returns_series = pd.Series(
            np.nan,
            index=trading_sessions,
        )

        # Have some fields of the portfolio changed? This should be accessed
        # through ``self._dirty_portfolio``
        self.__dirty_portfolio = False
        self._immutable_portfolio = sh.Portfolio(start, capital_base)
        self._portfolio = sh.MutableView(self._immutable_portfolio)

        self.daily_returns_series = pd.Series(
            np.nan,
            index=trading_sessions,
        )

        self.daily_returns_array = self.daily_returns_series.values

        self._previous_total_returns = 0

        # this is a component of the cache key for the account
        self._position_stats = None

        # Have some fields of the account changed?
        self._dirty_account = True
        self._immutable_account = sh.Account()
        self._account = sh.MutableView(self._immutable_account)

        # The broker blotter can override some fields on the account. This is
        # way to tangled up at the moment but we aren't fixing it today.
        self._account_overrides = {}

        self.position_tracker = PositionTracker(data_frequency)

        self._processed_transactions = {}

        self._transactions_by_modified = {}
        self._transactions_by_id = OrderedDict()

        # Keyed by instrument, the previous last sale price of positions with
        # payouts on price differences, e.g. Futures.
        #
        # This dt is not the previous minute to the minute for which the
        # calculation is done, but the last sale price either before the period
        # start, or when the price at execution.
        self._payout_last_sale_prices = {}

    @property
    def todays_returns(self):
        # compute today's returns in returns space instead of portfolio-value
        # space to work even when we have capital changes
        return (
            (self.portfolio.returns + 1) /
            (self._previous_total_returns + 1) -
            1
        )

    @property
    def _dirty_portfolio(self):
        return self.__dirty_portfolio

    @_dirty_portfolio.setter
    def _dirty_portfolio(self, value):
        if value:
            # marking the portfolio as dirty also marks the account as dirty
            self.__dirty_portfolio = self._dirty_account = value
        else:
            self.__dirty_portfolio = value

    def start_of_session(self, session_label):
        self._processed_transactions.clear()
        self._transactions_by_modified.clear()
        self._transactions_by_id.clear()

        # Save the previous day's total returns so that ``todays_returns``
        # produces returns since yesterday. This does not happen in
        # ``end_of_session`` because we want ``todays_returns`` to produce the
        # correct value in metric ``end_of_session`` handlers.
        self._previous_total_returns = self.portfolio.returns

    def end_of_bar(self, session_ix):
        # make daily_returns hold the partial returns, this saves many
        # metrics from doing a concat and copying all of the previous
        # returns
        self.daily_returns_array[session_ix] = self.todays_returns

    def end_of_session(self, session_ix):
        # save the daily returns time-series
        self.daily_returns_series[session_ix] = self.todays_returns

    def sync_last_sale_prices(self,
                              dt,
                              data_portal,
                              handle_non_market_minutes=False):
        self.position_tracker.sync_last_sale_prices(
            dt,
            data_portal,
            handle_non_market_minutes=handle_non_market_minutes,
        )
        self._dirty_portfolio = True

    @staticmethod
    def _calculate_payout(multiplier, amount, old_price, price):

        return (price - old_price) * multiplier * amount

    def _cash_flow(self, amount):
        self._dirty_portfolio = True
        p = self._portfolio
        p.cash_flow += amount
        p.cash += amount

    def process_transaction(self, transaction):
        """Add a transaction to ledger, updating the current state as needed.
        Parameters
        ----------
        transaction : zp.Transaction
            The transaction to execute.
        """
        instrument = transaction.instrument
        if isinstance(instrument, Future):
            try:
                old_price = self._payout_last_sale_prices[instrument]
            except KeyError:
                self._payout_last_sale_prices[instrument] = transaction.price
            else:
                position = self.position_tracker.positions[instrument]
                amount = position.amount
                price = transaction.price

                self._cash_flow(
                    self._calculate_payout(
                        instrument.multiplier,
                        amount,
                        old_price,
                        price,
                    ),
                )

                if amount + transaction.amount == 0:
                    del self._payout_last_sale_prices[instrument]
                else:
                    self._payout_last_sale_prices[instrument] = price
        else:
            self._cash_flow(-(transaction.price * transaction.amount))

        self.position_tracker.execute_transaction(transaction)

        # we only ever want the dict form from now on
        transaction_dict = transaction.to_dict()
        try:
            self._processed_transactions[transaction.dt].append(
                transaction_dict,
            )
        except KeyError:
            self._processed_transactions[transaction.dt] = [transaction_dict]

    def process_commission(self, commission):
        """Process the commission.
        Parameters
        ----------
        commission : zp.Event
            The commission being paid.
        """
        instrument = commission['instrument']
        cost = commission['cost']

        self.position_tracker.handle_commission(instrument, cost)
        self._cash_flow(-cost)

    def capital_change(self, change_amount):
        self.update_portfolio()
        portfolio = self._portfolio

        # we update the cash and total value so this is not dirty
        portfolio.portfolio_value += change_amount
        portfolio.cash += change_amount

    def transactions(self, dt=None):
        """Retrieve the dict-form of all of the transactions in a given bar or
        for the whole simulation.
        Parameters
        ----------
        dt : pd.Timestamp or None, optional
            The particular datetime to look up transactions for. If not passed,
            or None is explicitly passed, all of the transactions will be
            returned.
        Returns
        -------
        transactions : list[dict]
            The transaction information.
        """
        if dt is None:
            # flatten the by-day transactions
            return [
                txn
                for by_day in itervalues(self._processed_transactions)
                for txn in by_day
            ]

        return self._processed_transactions.get(dt, [])

    @property
    def positions(self):
        return self.position_tracker.get_position_list()

    def _get_payout_total(self, positions):
        calculate_payout = self._calculate_payout
        payout_last_sale_prices = self._payout_last_sale_prices

        total = 0
        for instrument, old_price in iteritems(payout_last_sale_prices):
            position = positions[instrument]
            payout_last_sale_prices[instrument] = price = position.last_sale_price
            amount = position.amount
            total += calculate_payout(
                instrument.price_multiplier,
                amount,
                old_price,
                price,
            )

        return total


    def update_portfolio(self):
        """Force a computation of the current portfolio state.
        """
        if not self._dirty_portfolio:
            return

        portfolio = self._portfolio
        pt = self.position_tracker

        portfolio.positions = pt.get_positions()
        position_stats = pt.stats

        portfolio.positions_value = position_value = (
            position_stats.net_value
        )
        portfolio.positions_exposure = position_stats.net_exposure
        self._cash_flow(self._get_payout_total(pt.positions))

        start_value = portfolio.portfolio_value

        # update the new starting value
        portfolio.portfolio_value = end_value = portfolio.cash + position_value

        pnl = end_value - start_value
        if start_value != 0:
            returns = pnl / start_value
        else:
            returns = 0.0

        portfolio.pnl += pnl
        portfolio.returns = (
            (1 + portfolio.returns) *
            (1 + returns) -
            1
        )

        # the portfolio has been fully synced
        self._dirty_portfolio = False

    @property
    def portfolio(self):
        """Compute the current portfolio.
        Notes
        -----
        This is cached, repeated access will not recompute the portfolio until
        the portfolio may have changed.
        """
        self.update_portfolio()
        return self._immutable_portfolio

    def calculate_period_stats(self):
        position_stats = self.position_tracker.stats
        portfolio_value = self.portfolio.portfolio_value

        if portfolio_value == 0:
            gross_leverage = net_leverage = np.inf
        else:
            gross_leverage = position_stats.gross_exposure / portfolio_value
            net_leverage = position_stats.net_exposure / portfolio_value

        return portfolio_value, gross_leverage, net_leverage

    def override_account_fields(self,
                                settled_cash=not_overridden,
                                accrued_interest=not_overridden,
                                buying_power=not_overridden,
                                equity_with_loan=not_overridden,
                                total_positions_value=not_overridden,
                                total_positions_exposure=not_overridden,
                                regt_equity=not_overridden,
                                regt_margin=not_overridden,
                                initial_margin_requirement=not_overridden,
                                maintenance_margin_requirement=not_overridden,
                                available_funds=not_overridden,
                                excess_liquidity=not_overridden,
                                cushion=not_overridden,
                                day_trades_remaining=not_overridden,
                                leverage=not_overridden,
                                net_leverage=not_overridden,
                                net_liquidation=not_overridden):
        """Override fields on ``self.account``.
        """
        # mark that the portfolio is dirty to override the fields again
        self._dirty_account = True
        self._account_overrides = kwargs = {
            k: v for k, v in locals().items() if v is not not_overridden
        }
        del kwargs['self']

    @property
    def account(self):
        if self._dirty_account:
            portfolio = self.portfolio

            account = self._account

            # If no attribute is found in the ``_account_overrides`` resort to
            # the following default values. If an attribute is found use the
            # existing value. For instance, a broker may provide updates to
            # these attributes. In this case we do not want to over write the
            # broker values with the default values.
            account.settled_cash = portfolio.cash
            account.accrued_interest = 0.0
            account.buying_power = np.inf
            account.equity_with_loan = portfolio.portfolio_value
            account.total_positions_value = (
                portfolio.portfolio_value - portfolio.cash
            )
            account.total_positions_exposure = (
                portfolio.positions_exposure
            )
            account.regt_equity = portfolio.cash
            account.regt_margin = np.inf
            account.initial_margin_requirement = 0.0
            account.maintenance_margin_requirement = 0.0
            account.available_funds = portfolio.cash
            account.excess_liquidity = portfolio.cash
            account.cushion = (
                (portfolio.cash / portfolio.portfolio_value)
                if portfolio.portfolio_value else
                np.nan
            )
            account.day_trades_remaining = np.inf
            (account.net_liquidation,
             account.gross_leverage,
             account.net_leverage) = self.calculate_period_stats()

            account.leverage = account.gross_leverage

            # apply the overrides
            for k, v in iteritems(self._account_overrides):
                setattr(account, k, v)

            # the account has been fully synced
            self._dirty_account = False

        return self._immutable_account
