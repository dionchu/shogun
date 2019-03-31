from copy import copy

from shogun.utils.input_validation import expect_types
from shogun.instruments.instrument import Instrument
from shogun.finance.protocol import DATASOURCE_TYPE
import uuid

class Transaction(object):
    __slots__ = ["id", "dt", "price", "instrument", "amount", "price",
                 "commission", "broker_order_id","type"]

    @expect_types(instrument=Instrument)
    def __init__(self, instrument, amount, dt, price, commission, broker_order_id=None, id=None):
        self.id = self.make_id() if id is None else id
        self.instrument = instrument
        self.amount = amount
        self.dt = dt
        self.price = price
        self.commission = commission
        self.broker_order_id = broker_order_id
        self.type = DATASOURCE_TYPE.TRANSACTION

    def __getitem__(self, name):
        return self.__dict__[name]

    def __repr__(self):
        template = (
            "{cls}(instrument={instrument}, dt={dt},"
            " amount={amount}, price={price}, commission={commission})"
        )

        return template.format(
            cls=type(self).__name__,
            instrument=self.instrument,
            dt=self.dt,
            amount=self.amount,
            price=self.price,
            commission=self.commission
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
