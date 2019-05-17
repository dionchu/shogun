import os.path
import numpy as np
import pandas as pd
import logging
import trading_calendars
import time as time
import math
import eikon as ek
ek.set_app_key('48f17fdf21184b0ca9c4ea8913a840a92b338918')
ek.set_app_key('4ed8362c27f846d09376992fae22fd34dd1c8950')
from .equity_factory import EquityFactory
from .equity_factory import equity_metadata_df, equity_instrument_df, _instrument_timestamp_fields, metadata_columns, _dividend_timestamp_fields

from pandas import read_hdf
from pandas import HDFStore,DataFrame

from shogun.utils.query_utils import query_df

import os
#dirname = os.path.dirname(__file__)
from shogun.DIRNAME import dirname

import logging

from datetime import date
from pandas.tseries.offsets import *

default_start = pd.Timestamp('2000-01-01')

def update_us_equity(factory,dt,platform='RIC'):
    """
    update equity to table
    """
    dt = pd.Timestamp(dt)
    logging.basicConfig(filename='./python_logs/update_equity'+pd.Timestamp('today').strftime("%Y%m%d.%H.%M")+'.log',level=logging.DEBUG)
    logging.info('Started')

    exchange_id = 'XNYS'

    if not trading_calendars.get_calendar(exchange_id).is_session(dt):
            # check if today is exchange holiday/weekend
            print('{dt} not a valid session: do nothing'.format(dt=dt.strftime("%Y-%m-%d")))
            return

    # compare todays list to existing EquityInstrument table
    if os.path.isfile(dirname + "\_EquityInstrument.h5"):
        traded_equities = factory.get_traded_equities()
        processed_instruments = read_hdf(dirname + '\_EquityInstrument.h5',where="instrument_country_id=" + "\"" + 'US' + "\"").reset_index(level=[0])
        new_equities = traded_equities[~traded_equities.exchange_symbol.isin(processed_instruments.exchange_symbol)]
        new_instruments_metadata = factory.construct_us_equity_metadata(new_equities)
        existing_instruments_metadata = processed_instruments[processed_instruments.exchange_symbol.isin(traded_equities.exchange_symbol)]
    else:
        traded_equities = factory.get_traded_equities()
        new_instruments_metadata = factory.construct_us_equity_metadata(traded_equities)
        existing_instruments_metadata = None

    if existing_instruments_metadata is not None:
        # for those already in the Fixed Income Instrument table, get query start date
        existing_instruments_metadata.insert(0,'platform_symbol', [factory.exchange_symbol_to_ticker(x) for x in existing_instruments_metadata.exchange_symbol])
        existing_instruments_dict = existing_instruments_metadata.set_index('platform_symbol').to_dict()
    else:
        # set to empty default dataframe
        existing_instruments_metadata = equity_metadata_df
        existing_instruments_metadata.insert(0,'platform_symbol', [factory.exchange_symbol_to_ticker(x) for x in existing_instruments_metadata.index])
        existing_instruments_dict = existing_instruments_metadata.set_index('platform_symbol').to_dict()

    # for those that are new, make root chain and calculate query start date
    if new_instruments_metadata.shape[0] != 0:
        new_instruments_metadata.insert(0,'platform_symbol', [factory.exchange_symbol_to_ticker(x) for x in new_instruments_metadata.exchange_symbol])
        new_instruments_dict = new_instruments_metadata.set_index('platform_symbol').to_dict()

        # combine information in dictionary
        platform_query = {
        'exchange_symbol': dict(existing_instruments_dict['exchange_symbol'], **new_instruments_dict['exchange_symbol']),
        'start_date': dict( existing_instruments_dict['end_date'],**new_instruments_dict['end_date']),
        'type': dict( existing_instruments_dict['type'],**new_instruments_dict['type'])
        }
    else:
        platform_query = {
        'exchange_symbol': existing_instruments_dict['exchange_symbol'],
        'start_date': existing_instruments_dict['end_date'],
        'type': existing_instruments_dict['type']
        }

    platform_query_df = pd.DataFrame.from_dict(platform_query)
    platform_query = platform_query_df[platform_query_df['start_date'] <= dt.date()].to_dict()

    # Loop through symbols and pull dividend data
    dividend = get_eikon_dividend_data(platform_query, dt)
    # Loop through symbols and pull raw data into data frame
    data_df = get_eikon_equity_data(platform_query, dt)
    # Check missing symbols from platform_query
    check_missing_symbols(data_df, existing_instruments_metadata, new_instruments_metadata)
    # Check missing days and days not expected
    check_missing_extra_days(factory, data_df.reset_index(level=[1]))
    # Append data to hdf, remove duplicates, and write to both hdf and csv
    write_to_instrument_table(dirname, data_df)
    # Append data to hdf, remove duplicates, and write to both hdf and csv
    write_to_equity_event(dirname, dividend, split = None)
    # Construct and write metadata for missing contracts
    start_end_df = calc_start_end_dates(data_df)

    if existing_instruments_metadata.shape[0] == 0:
        existing_instruments_metadata = None

    if new_instruments_metadata.shape[0] != 0:
        new_instruments_metadata = construct_equity_metadata(new_instruments_metadata, start_end_df)
    else:
        new_instruments_metadata = None

    write_to_equity_instrument(dirname, new_instruments_metadata, existing_instruments_metadata, start_end_df)
    # Write to instrument router
    write_to_instrument_router(dirname, new_instruments_metadata)

    logging.info('Finished')

def construct_equity_metadata(new_instruments_metadata, start_end_df):
    if start_end_df.shape[0] != 0:
        start_end_df.reset_index(level=[0],inplace=True)
        metadata_df = pd.merge(new_instruments_metadata.drop(['start_date','end_date'], axis=1), start_end_df, on='exchange_symbol')
        metadata_df = metadata_df[metadata_columns]
        metadata_df['tick_size'] = metadata_df['tick_size'].astype(str).astype(float)
        metadata_df['multiplier'] = metadata_df['multiplier'].astype(str).astype(float)
        metadata_df['underlying_asset_class_id'] = metadata_df['underlying_asset_class_id'].astype(str).astype(float)

        metadata_df.set_index(['exchange_symbol'], append=True, inplace=True)
        metadata_df.reset_index(level=[0],drop=True,inplace=True)

    else:
        metadata_df = None

    return metadata_df

def write_to_equity_event(dirname, dividend = None, split = None):
    # splits not yet included, to be built out at later date
    # Append data to hdf, remove duplicates, and write to both hdf and csv
    if dividend is None:
        return
    else:
        dividend = _convert_dividend_timestamp_fields(dividend)
        # Append data to hdf, remove duplicates, and write to both hdf and csv
        if os.path.isfile(dirname + "\_EquityDividend.h5"):
            equity_dividend_hdf = read_hdf(dirname +'\_EquityDividend.h5')
            equity_dividend_hdf = equity_dividend_hdf.append(dividend)
            equity_dividend_hdf = equity_dividend_hdf[~equity_dividend_hdf.index.duplicated(keep='last')]
            equity_dividend_hdf.sort_index(level=['exchange_symbol'], ascending=[1], inplace=True)
            equity_dividend_hdf.to_hdf(dirname +'\_EquityDividend.h5', 'EquityDividend', mode = 'w',
               format='table', data_columns=True)
            equity_dividend_hdf.to_csv(dirname + "\_EquityDividend.csv")
        else:
            print("Table does not exist! Writing new. ")
            equity_dividend_hdf = dividend.drop_duplicates()
            equity_dividend_hdf.sort_index(level=['exchange_symbol'], ascending=[1], inplace=True)
            equity_dividend_hdf.to_hdf(dirname +'\_EquityDividend.h5', 'EquityDividend', mode = 'w',
                            format='table', data_columns=True)
            equity_dividend_hdf.to_csv(dirname + "\_EquityDividend.csv")

def write_to_equity_instrument(dirname, new_instrument_metadata=None, existing_instruments_metadata=None, start_end_df=None):
    # Append data to hdf, remove duplicates, and write to both hdf and csv
    if new_instrument_metadata is None and existing_instruments_metadata is None:
        return
    else:
        if os.path.isfile(dirname + "\_EquityInstrument.h5"):

            equity_instrument_hdf = read_hdf(dirname +'\_EquityInstrument.h5')

            # update end dates in equity instrument
            if existing_instruments_metadata is not None:
                for symbol in existing_instruments_metadata.exchange_symbol:
                    equity_instrument_hdf.at[symbol, 'end_date'] = start_end_df.to_dict()['end_date'][symbol]
            # append new metadata to equity instrument
            if new_instrument_metadata is not None:
                new_instrument_metadata = _convert_instrument_timestamp_fields(new_instrument_metadata)
                equity_instrument_hdf = equity_instrument_hdf.append(new_instrument_metadata)

            equity_instrument_hdf = equity_instrument_hdf[~equity_instrument_hdf.index.duplicated(keep='last')]
            equity_instrument_hdf.to_hdf(dirname +'\_EquityInstrument.h5', 'EquityInstrument', mode = 'w',
               format='table', data_columns=True)
            equity_instrument_hdf.to_csv(dirname + "\_EquityInstrument.csv")
        else:
            if new_instrument_metadata is not None:
                new_instrument_metadata = _convert_instrument_timestamp_fields(new_instrument_metadata)
                new_instrument_metadata.to_hdf(dirname +'\_EquityInstrument.h5', 'EquityInstrument', mode = 'w',
                   format='table', data_columns=True)
                new_instrument_metadata.to_csv(dirname + "\_EquityInstrument.csv")

def calc_start_end_dates(data_df):
    # Reset index to columns
    _data_df = data_df.reset_index(level=[0,1])

    # Calculate start and end dates
    start_end_df = pd.DataFrame(
        {'start_date': _data_df.groupby(['exchange_symbol']).first()['date'],
        'end_date': _data_df.groupby(['exchange_symbol']).last()['date']
        })
    return start_end_df

def write_to_instrument_router(dirname, new_instrument_metadata):
    # Assign instrument routing information to table
    if new_instrument_metadata is None:
        return
    else:
        instrument_router_df = pd.DataFrame({'instrument_type': ['Equity']}, index=new_instrument_metadata.index)

        if os.path.isfile(dirname + "\_InstrumentRouter.h5"):
            instrument_router_hdf = read_hdf(dirname +'\_InstrumentRouter.h5')
            if instrument_router_hdf is not None:
                instrument_router_hdf = instrument_router_hdf.append(instrument_router_df)
                instrument_router_hdf = instrument_router_hdf[~instrument_router_hdf.index.duplicated(keep='last')]
                instrument_router_hdf.to_hdf(dirname +'\_InstrumentRouter.h5', 'InstrumentRouter', mode = 'w',
                   format='table', data_columns=True)
                instrument_router_hdf.to_csv(dirname + "\_InstrumentRouter.csv")
            else:
                instrument_router_df.to_hdf(dirname +'\_InstrumentRouter.h5', 'InstrumentRouter', mode = 'w',
                   format='table', data_columns=True)
                instrument_router_df.to_csv(dirname + "\_InstrumentRouter.csv")
        else:
            instrument_router_df.to_hdf(dirname +'\_InstrumentRouter.h5', 'InstrumentRouter', mode = 'w',
               format='table', data_columns=True)
            instrument_router_df.to_csv(dirname + "\_InstrumentRouter.csv")

def write_to_instrument_table(dirname, data_df):
    # Append data to hdf, remove duplicates, and write to both hdf and csv
    if os.path.isfile(dirname + "\_InstrumentData.h5"):
        instrument_data_hdf = read_hdf(dirname +'\_InstrumentData.h5')
        instrument_data_hdf = instrument_data_hdf.append(data_df)
        instrument_data_hdf = instrument_data_hdf[~instrument_data_hdf.index.duplicated(keep='last')]
        instrument_data_hdf.sort_index(level=['date','exchange_symbol'], ascending=[1, 0], inplace=True)
        instrument_data_hdf.to_hdf(dirname +'\_InstrumentData.h5', 'InstrumentData', mode = 'w',
           format='table', data_columns=True)
        instrument_data_hdf.to_csv(dirname + "\_InstrumentData.csv")
    else:
        print("Table does not exist! Writing new. ")
        instrument_data_hdf = data_df.drop_duplicates()
        instrument_data_hdf.sort_index(level=['date','exchange_symbol'], ascending=[1, 0], inplace=True)
        instrument_data_hdf.to_hdf(dirname +'\_InstrumentData.h5', 'InstrumentData', mode = 'w',
                        format='table', data_columns=True)
        instrument_data_hdf.to_csv(dirname + "\_InstrumentData.csv")

def check_missing_extra_days(factory, data_df):

#    data_df.reset_index(level=[0], inplace=True)
    grouped_df = data_df.groupby('exchange_symbol')

    for exchange_symbol in grouped_df.groups:
        exchange_id = 'USBOND'
        cal = trading_calendars.get_calendar(exchange_id)

        expected = cal.sessions_in_range(grouped_df.get_group(exchange_symbol).index.values[0],
                                         grouped_df.get_group(exchange_symbol).index.values[-1]
                                         )
        actual = pd.DatetimeIndex(grouped_df.get_group(exchange_symbol).index.values, tz='UTC')

        extra = set(actual).difference(expected)
        missing = set(expected).difference(actual)

        if len(extra) >0:
            print("{exchange_symbol} did not expect: {extra}".format(exchange_symbol=exchange_symbol, extra=set([d.strftime("%Y-%m-%d") for d in extra])))
            logging.info("{exchange_symbol} did not expect: {extra}".format(exchange_symbol=exchange_symbol, extra=set([d.strftime("%Y-%m-%d") for d in extra])))
        if len(missing) > 0:
            print("{exchange_symbol} expected but did not get: {missing}".format(exchange_symbol=exchange_symbol, missing=set([d.strftime("%Y-%m-%d") for d in missing])))
            logging.warning("{exchange_symbol} expected but did not get: {missing}".format(exchange_symbol=exchange_symbol, missing=set([d.strftime("%Y-%m-%d") for d in missing])))

def check_missing_symbols(data_df,existing_instruments,missing_instruments):
    data_check = [x for x in existing_instruments.append(missing_instruments).exchange_symbol if x not in data_df.head(-10).index.get_level_values(level=1)]
    if(len(data_check) > 0):
        return "Missing data for:" + str(data_check)

def get_eikon_dividend_data(platform_query, dt):
    # Loop through symbols and pull raw data into data frame
    today = pd.Timestamp(date.today())
    data_df = pd.DataFrame()
    dt = pd.Timestamp(dt.strftime("%Y-%m-%d"))
    for platform_symbol in platform_query['exchange_symbol'].keys():
        print(platform_symbol)
        exchange_symbol = platform_query['exchange_symbol'][platform_symbol]
        start = min(platform_query['start_date'][platform_symbol], dt).strftime("%Y-%m-%d")
        end = dt.strftime("%Y-%m-%d")
        if platform_query['type'][platform_symbol] == 'ETF':
            tmp, err = ek.get_data(platform_symbol, ["TR.FundExDate", "TR.FundRecordDate", "TR.FundPayDate", "TR.FundDiv", "TR.FundDivCurr"], {'SDate':str(start),'EDate':str(end)})
        else:
            tmp, err = ek.get_data(platform_symbol, ["TR.DivExDate", "TR.DivRecordDate", "TR.DivPayDate", "TR.DivUnadjustedGross", "TR.DivCurr"], {'SDate':str(start),'EDate':str(end)})
        tmp.columns = ['exchange_symbol', 'ex_date', 'record_date', 'pay_date', 'amount', 'currency']
        tmp['exchange_symbol'] = exchange_symbol
        if len(tmp) > 1 and not math.isnan(tmp.iloc[0]['amount']):
            data_df = data_df.append(tmp)

    # Change default column names to lower case
    if len(data_df) > 0:
        data_df.columns = ['exchange_symbol', 'ex_date', 'record_date', 'pay_date', 'amount', 'currency']
        data_df.set_index(['exchange_symbol'], append=True, inplace=True)
        data_df = data_df.reset_index(drop=True, level = 0)
    else:
        data_df = None
    return data_df

def get_eikon_equity_data(platform_query, dt):
    # Loop through symbols and pull raw data into data frame
    today = pd.Timestamp(date.today())
    data_df = pd.DataFrame()
    dt = pd.Timestamp(dt.strftime("%Y-%m-%d"))
    for platform_symbol in platform_query['exchange_symbol'].keys():
        print(platform_symbol)
        exchange_symbol = platform_query['exchange_symbol'][platform_symbol]
        start = min(platform_query['start_date'][platform_symbol], dt).strftime("%Y-%m-%d")
        end = dt.strftime("%Y-%m-%d")
        tmp = eikon_ohlcvoi_batch_retrieval(platform_symbol,exchange_symbol,start_date=start,end_date=end)
        data_df = data_df.append(tmp)

    # Change default column names to lower case
    data_df.columns = ['exchange_symbol','open','high','low','close','volume','open_interest']
    data_df.index.name = 'date'
    data_df.set_index(['exchange_symbol'], append=True, inplace=True)
    return data_df

def get_eikon_ohlcv_oi(eikon_symbol,exchange_symbol,start_date,end_date):
    """
    Fetch daily open, high, low close, open interest data for "platform_symbol".
    """
    assert type(start_date) is str, "start_date is not a string: %r" % start_date
    assert type(end_date) is str, "start_date is not a string: %r" % end_date
    # OI does not come out until the following day, need to get enough data to lag by 1
    oi_start = (pd.Timestamp(start_date)-pd.Timedelta(days=7)).strftime("%Y-%m-%d")

    try:
        tmp_ohlcv = ek.get_timeseries(eikon_symbol,["open","high","low","close","volume"],start_date=str(start_date), end_date=str(end_date))
    except ek.EikonError:
        return pd.DataFrame()
    tmp_ohlcv.insert(0,'exchange_symbol',exchange_symbol)
    try:
        e = ek.get_data(eikon_symbol, ['TR.OPENINTEREST.Date', 'TR.OPENINTEREST'], {'SDate':str(oi_start),'EDate':str(end_date)})
        tmp_oi = pd.DataFrame({'open_interest': e[0]['Open Interest'].values}, index = pd.to_datetime(e[0]['Date'].values)).shift(1)
        if( tmp_oi.shape == (1, 1) ):
            tmp_ohlcv['open_interest'] = pd.Series('NaN', index=tmp_ohlcv.index)
            tmp = tmp_ohlcv
            tmp['open_interest'] = tmp['open_interest'].astype(str).astype(float)
        else:
            tmp = pd.merge(tmp_ohlcv,tmp_oi,left_index=True,right_index=True,how='left')
    except:
        tmp_ohlcv['open_interest'] = pd.Series('NaN', index=tmp_ohlcv.index)
        tmp = tmp_ohlcv
        tmp['open_interest'] = tmp['open_interest'].astype(str).astype(float)

    return tmp[~np.isnan(tmp['CLOSE'])]

def eikon_ohlcvoi_batch_retrieval(eikon_symbol,exchange_symbol,start_date,end_date):
    """
    Fetch daily data for "platform_symbol". Eikon API limits one-time retrievals,
    therefore we retrieve the data in 5 year batches.
    """
    assert type(start_date) is str, "start_date is not a string: %r" % start_date
    assert type(end_date) is str, "start_date is not a string: %r" % end_date

    data_df = pd.DataFrame()
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    counter = 0

    while int((end_date - start_date).days) > 730:
        temp_end_date = start_date + pd.DateOffset(years=2)
        tmp = get_eikon_ohlcv_oi(eikon_symbol,exchange_symbol,start_date.strftime("%Y-%m-%d"),temp_end_date.strftime("%Y-%m-%d"))
        data_df = data_df.append(tmp)
        start_date = temp_end_date
        time.sleep(0.5)

    tmp = get_eikon_ohlcv_oi(eikon_symbol,exchange_symbol,start_date.strftime("%Y-%m-%d"),end_date.strftime("%Y-%m-%d"))
    return data_df.append(tmp)

def _convert_instrument_timestamp_fields(df):
    """
    Takes in a df of Instrument metadata columns and converts dates to pd.datetime64
    """
    for key in _instrument_timestamp_fields:
        df[key] = pd.to_datetime(df[key])
    return df

def _convert_dividend_timestamp_fields(df):
    """
    Takes in a df of Instrument metadata columns and converts dates to pd.datetime64
    """
    for key in _dividend_timestamp_fields:
        df[key] = pd.to_datetime(df[key])
    return df