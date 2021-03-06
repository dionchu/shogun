#
# Copyright 2015 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from distutils.version import StrictVersion
import os
import numpy as np
import pandas as pd
from pandas.tseries.offsets import *
from datetime import date
from pandas import read_hdf
from pandas import HDFStore,DataFrame

# This is *not* a place to dump arbitrary classes/modules for convenience,
# it is a place to expose the public interfaces.
from trading_calendars import get_calendar
from .data_portal.data_portal import DataPortal
from .instruments.instrument_finder import InstrumentFinder
from .data_portal.hdf_daily_bars import HdfDailyBarReader
from .data_portal.history_loader import ContinuousFutureAdjustmentReader
from .data_portal.continuous_future_reader import ContinuousFutureSessionBarReader
from .database.future_root_factory import FutureRootFactory
from .database.tbill_factory import TBillFactory
from .database.equity_factory import EquityFactory
from .finance.performance_analysis import PerformanceAnalysis
from .instruments.roll_finder import (
    CalendarRollFinder,
    VolumeRollFinder
)

__all__ = [
    'get_calendar',
    'DataPortal',
    'InstrumentFinder',
    'HdfDailyBarReader',
    'ContinuousFutureAdjustmentReader',
    'ContinuousFutureSessionBarReader',
    'FutureRootFactory',
    'TBillFactory',
    'EquityFactory',
    'CalendarRollFinder',
    'VolumeRollFinder',
    'PerformanceAnalysis'
]
