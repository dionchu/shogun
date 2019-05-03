import os.path
import numpy as np
import pandas as pd
import logging
import trading_calendars
import time as time
import eikon as ek
ek.set_app_key('48f17fdf21184b0ca9c4ea8913a840a92b338918')
ek.set_app_key('4ed8362c27f846d09376992fae22fd34dd1c8950')
from .tbill_factory import TBillFactory
from .tbill_factory import tbill_metadata_df, _instrument_timestamp_fields, metadata_columns

from pandas import read_hdf
from pandas import HDFStore,DataFrame

from shogun.utils.query_utils import query_df
from shogun.analytics.bondmath import billprice

import os
dirname = os.path.dirname(__file__)

import logging

from datetime import date
from pandas.tseries.offsets import *

def update_tbill(factory,dt,platform='RIC'):
    """
    update fixed income to table
    """
    dt = pd.Timestamp(dt)
    logging.basicConfig(filename='./python_logs/update_tbill'+pd.Timestamp('today').strftime("%Y%m%d.%H.%M")+'.log',level=logging.DEBUG)
    logging.info('Started')

    exchange_id = 'USBOND'

    if not trading_calendars.get_calendar(exchange_id).is_session(dt):
            # check if today is exchange holiday/weekend
            print('{dt} not a valid session: do nothing'.format(dt=dt.strftime("%Y-%m-%d")))
            return

    # compare todays list to existing FixedIncomeInstrument table
    if os.path.isfile(dirname + "\_FixedIncomeInstrument.h5"):
        first_time = False
        bills_outstanding = factory.get_outstanding_bills(dt)
        processed_instruments = read_hdf(dirname + '\_FixedIncomeInstrument.h5',where="type=" + "\"" + 'BILL' + "\"").reset_index(level=[0])
        new_bills_outstanding = bills_outstanding[~bills_outstanding.exchange_symbol.isin(processed_instruments.exchange_symbol)]
        new_instruments_metadata = factory.construct_tbill_metadata(new_bills_outstanding)
        existing_instruments_metadata = processed_instruments[processed_instruments.exchange_symbol.isin(bills_outstanding.exchange_symbol)]
    else:
        first_time = True
        new_bills_outstanding = factory.get_outstanding_bills(dt, first_time)
        new_instruments_metadata = factory.construct_tbill_metadata(new_bills_outstanding)
        existing_instruments_metadata = None

    if existing_instruments_metadata is not None:
        # for those already in the Fixed Income Instrument table, get query start date
        existing_instruments_metadata.insert(0,'platform_symbol', [factory.exchange_symbol_to_ticker(x) for x in existing_instruments_metadata.exchange_symbol])
        existing_instruments_dict = existing_instruments_metadata.set_index('platform_symbol').to_dict()
    else:
        # set to empty default dataframe
        existing_instruments_metadata = tbill_metadata_df
        existing_instruments_metadata.insert(0,'platform_symbol', [factory.exchange_symbol_to_ticker(x) for x in existing_instruments_metadata.index])
        existing_instruments_dict = existing_instruments_metadata.set_index('platform_symbol').to_dict()

    # for those that are new, make root chain and calculate query start date
    if new_instruments_metadata.shape[0] != 0:
        new_instruments_metadata.insert(0,'platform_symbol', [factory.exchange_symbol_to_ticker(x) for x in new_instruments_metadata.exchange_symbol])
        new_instruments_dict = new_instruments_metadata.set_index('platform_symbol').to_dict()

        new_query_start = new_instruments_metadata.set_index('exchange_symbol').to_dict()['first_auction_date']

        # combine information in dictionary
        platform_query = {
        'issue_date': dict(existing_instruments_dict['issue_date'], **new_instruments_dict['issue_date']),
        'maturity_date': dict(existing_instruments_dict['maturity_date'], **new_instruments_dict['maturity_date']),
        'exchange_symbol': dict(existing_instruments_dict['exchange_symbol'], **new_instruments_dict['exchange_symbol']),
        'start_date': dict( existing_instruments_dict['end_date'], **new_instruments_dict['first_auction_date'])
        }
    else:
        platform_query = {
        'issue_date': existing_instruments_dict['issue_date'],
        'maturity_date': existing_instruments_dict['maturity_date'],
        'exchange_symbol': existing_instruments_dict['exchange_symbol'],
        'start_date': existing_instruments_dict['end_date'],
        }

#    platform_query_df = pd.DataFrame.from_dict(platform_query)
#    platform_query = platform_query_df[platform_query_df['start_date'] <= dt.date()].to_dict()

    # Loop through symbols and pull raw data into data frame
    data_df = get_eikon_tbill_data(platform_query, dt, first_time)
    # Check missing symbols from platform_query
    check_missing_symbols(data_df, existing_instruments_metadata, new_instruments_metadata)
    # Check missing days and days not expected
    check_missing_extra_days(factory, data_df.reset_index(level=[1]))
    # Append data to hdf, remove duplicates, and write to both hdf and csv
    write_to_instrument_table(dirname, data_df)
    # Construct and write metadata for missing contracts
    start_end_df = calc_start_end_dates(data_df)

    if existing_instruments_metadata.shape[0] == 0:
        existing_instruments_metadata = None

    if new_instruments_metadata.shape[0] != 0:
        new_instruments_metadata = construct_tbill_metadata(new_instruments_metadata, start_end_df)
    else:
        new_instruments_metadata = None

    write_to_fixed_income_instrument(dirname, new_instruments_metadata, existing_instruments_metadata, start_end_df)
    # Write to instrument router
    write_to_instrument_router(dirname, new_instruments_metadata)

    logging.info('Finished')

def update_missing_tbill(factory,dt,platform='RIC'):
    """
    update fixed income to table
    """
    dt = pd.Timestamp(dt)
    logging.basicConfig(filename='./python_logs/update_missing_tbill'+pd.Timestamp('today').strftime("%Y%m%d.%H.%M")+'.log',level=logging.DEBUG)
    logging.info('Started')

    exchange_id = 'USBOND'

    if not trading_calendars.get_calendar(exchange_id).is_session(dt):
            # check if today is exchange holiday/weekend
            print('{dt} not a valid session: do nothing'.format(dt=dt.strftime("%Y-%m-%d")))
            return

    # compare todays list to existing FixedIncomeInstrument table
    if os.path.isfile(dirname + "\_FixedIncomeInstrument.h5"):
        first_time = True
        bills_outstanding = factory.get_outstanding_bills(dt, first_time)
        processed_instruments = read_hdf(dirname + '\_FixedIncomeInstrument.h5',where="type=" + "\"" + 'BILL' + "\"").reset_index(level=[0])
        missing_bills_outstanding = bills_outstanding[~bills_outstanding.exchange_symbol.isin(processed_instruments.exchange_symbol)]
        missing_instruments_metadata = factory.construct_tbill_metadata(missing_bills_outstanding)
        # set to empty default dataframe, we only want update missing symbols here
        existing_instruments_metadata = tbill_metadata_df
        existing_instruments_metadata.insert(0,'platform_symbol', [factory.exchange_symbol_to_ticker(x) for x in existing_instruments_metadata.exchange_symbol])

    else:
        print("Error, no data processed, nothing to fill")
        return

    # for those that are new, make root chain and calculate query start date
    if missing_instruments_metadata.shape[0] != 0:
        missing_instruments_metadata.insert(0,'platform_symbol', [factory.exchange_symbol_to_ticker(x) for x in missing_instruments_metadata.exchange_symbol])
        missing_instruments_dict = missing_instruments_metadata.set_index('platform_symbol').to_dict()

        new_query_start = missing_instruments_metadata.set_index('exchange_symbol').to_dict()['first_auction_date']

        # combine information in dictionary
        platform_query = {
        'issue_date': dict(missing_instruments_dict['issue_date']),
        'maturity_date': dict(missing_instruments_dict['maturity_date']),
        'exchange_symbol': dict(missing_instruments_dict['exchange_symbol']),
        'start_date': dict(missing_instruments_dict['first_auction_date'])
        }
    else:
        print('no missing bills')
        return

    # Loop through symbols and pull raw data into data frame
    data_df = get_eikon_tbill_data(platform_query, dt, first_time)
    # Check missing symbols from platform_query
    check_missing_symbols(data_df, existing_instruments_metadata, missing_instruments_metadata)
    # Check missing days and days not expected
    check_missing_extra_days(factory, data_df.reset_index(level=[1]))
    # Append data to hdf, remove duplicates, and write to both hdf and csv
    write_to_instrument_table(dirname, data_df)
    # Construct and write metadata for missing contracts
    start_end_df = calc_start_end_dates(data_df)

    if existing_instruments_metadata.shape[0] == 0:
        existing_instruments_metadata = None

    if missing_instruments_metadata.shape[0] != 0:
        missing_instruments_metadata = construct_tbill_metadata(missing_instruments_metadata, start_end_df)
    else:
        missing_instruments_metadata = None

    write_to_fixed_income_instrument(dirname, missing_instruments_metadata, existing_instruments_metadata, start_end_df.set_index('exchange_symbol'))
    # Write to instrument router
    write_to_instrument_router(dirname, missing_instruments_metadata)

    logging.info('Finished')

def construct_tbill_metadata(new_instruments_metadata, start_end_df):
    if start_end_df.shape[0] != 0:
        start_end_df.reset_index(level=[0],inplace=True)
        metadata_df = pd.merge(new_instruments_metadata,start_end_df, on='exchange_symbol')
        metadata_df = metadata_df[metadata_columns]
        metadata_df['coupon'] = metadata_df['coupon'].astype(str).astype(float)
        metadata_df['tick_size'] = metadata_df['tick_size'].astype(str).astype(float)
        metadata_df['multiplier'] = metadata_df['multiplier'].astype(str).astype(float)
        metadata_df['redemption'] = metadata_df['redemption'].astype(str).astype(int)
        metadata_df['face_value'] = metadata_df['face_value'].astype(str).astype(int)
        metadata_df['settlement_days'] = metadata_df['settlement_days'].astype(str).astype(int)
        metadata_df['underlying_asset_class_id'] = metadata_df['underlying_asset_class_id'].astype(str).astype(float)

        metadata_df.set_index(['exchange_symbol'], append=True, inplace=True)
        metadata_df.reset_index(level=[0],drop=True,inplace=True)

    else:
        metadata_df = None

    return metadata_df

def _convert_instrument_timestamp_fields(df):
    """
    Takes in a df of Instrument metadata columns and converts dates to pd.datetime64
    """
    for key in _instrument_timestamp_fields:
        df[key] = pd.to_datetime(df[key])
    return df

def write_to_fixed_income_instrument(dirname, new_instrument_metadata=None, existing_instruments_metadata=None, start_end_df=None):
    # Append data to hdf, remove duplicates, and write to both hdf and csv
    if new_instrument_metadata is None and existing_instruments_metadata is None:
        return
    else:
        if os.path.isfile(dirname + "\_FixedIncomeInstrument.h5"):

            fixed_income_instrument_hdf = read_hdf(dirname +'\_FixedIncomeInstrument.h5')

            # update end dates in fixed_income instrument
            if existing_instruments_metadata is not None:
                for symbol in existing_instruments_metadata.exchange_symbol:
                    if start_end_df.to_dict()['end_date'][symbol] >= fixed_income_instrument_hdf.at[symbol, 'end_date']:
                        fixed_income_instrument_hdf.at[symbol, 'end_date'] = start_end_df.to_dict()['end_date'][symbol]
            # append new metadata to fixed_income instrument
            if new_instrument_metadata is not None:
                new_instrument_metadata = _convert_instrument_timestamp_fields(new_instrument_metadata)
                fixed_income_instrument_hdf = fixed_income_instrument_hdf.append(new_instrument_metadata)

            fixed_income_instrument_hdf = fixed_income_instrument_hdf[~fixed_income_instrument_hdf.index.duplicated(keep='last')]
            fixed_income_instrument_hdf.to_hdf(dirname +'\_FixedIncomeInstrument.h5', 'FixedIncomeInstrument', mode = 'w',
               format='table', data_columns=True)
            fixed_income_instrument_hdf.to_csv(dirname + "\_FixedIncomeInstrument.csv")
        else:
            if new_instrument_metadata is not None:
                new_instrument_metadata = _convert_instrument_timestamp_fields(new_instrument_metadata)
                new_instrument_metadata.to_hdf(dirname +'\_FixedIncomeInstrument.h5', 'FixedIncomeInstrument', mode = 'w',
                   format='table', data_columns=True)
                new_instrument_metadata.to_csv(dirname + "\_FixedIncomeInstrument.csv")


def write_to_instrument_router(dirname, new_instrument_metadata):
    # Assign instrument routing information to table
    if new_instrument_metadata is None:
        return
    else:
        instrument_router_df = pd.DataFrame({'instrument_type': ['FixedIncome']}, index=new_instrument_metadata.index)

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

def calc_start_end_dates(data_df):
    # Reset index to columns
    _data_df = data_df.reset_index(level=[0,1])

    # Calculate start and end dates
    start_end_df = pd.DataFrame(
        {'start_date': _data_df.groupby(['exchange_symbol']).first()['date'],
        'end_date': _data_df.groupby(['exchange_symbol']).last()['date']
        })
    return start_end_df

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
        exchange_id = 'XNYS'
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



def get_eikon_tbill_data(platform_query, dt, first_time):
    # Loop through symbols and pull raw data into data frame
    today = pd.Timestamp(date.today())
    data_df = pd.DataFrame()
    dt = pd.Timestamp(dt.strftime("%Y-%m-%d"))
    for platform_symbol in platform_query['exchange_symbol'].keys():
        print(platform_symbol)
        maturity_date = platform_query['maturity_date'][platform_symbol].strftime("%Y-%m-%d")
        issue_date = platform_query['issue_date'][platform_symbol].strftime("%Y-%m-%d")
        exchange_symbol = platform_query['exchange_symbol'][platform_symbol]
        if first_time:
            start = platform_query['start_date'][platform_symbol].strftime("%Y-%m-%d")
            end = platform_query['maturity_date'][platform_symbol].strftime("%Y-%m-%d")
        else:
            start = min(platform_query['start_date'][platform_symbol], dt).strftime("%Y-%m-%d")
            end = min(platform_query['maturity_date'][platform_symbol], dt).strftime("%Y-%m-%d")
        # RIC switches to expired RIC 4 calendar days after last trade
#        i = 0
#        while (i < 3):
#            try:
#                tmp = eikon_ohlcvoi_batch_retrieval(platform_symbol,exchange_symbol,start_date=start,end_date=end)
#                tmp['OPEN'] = [billprice(row['OPEN']/100, index.strftime("%Y-%m-%d"), maturity_date, issue_date) for index, row in tmp.iterrows()]
#                tmp['CLOSE'] = [billprice(row['CLOSE']/100, index.strftime("%Y-%m-%d"), maturity_date, issue_date) for index, row in tmp.iterrows()]
#                tmp_hi = [billprice(row['LOW']/100, index.strftime("%Y-%m-%d"), maturity_date, issue_date) for index, row in tmp.iterrows()]
#                tmp_lo = [billprice(row['HIGH']/100, index.strftime("%Y-%m-%d"), maturity_date, issue_date) for index, row in tmp.iterrows()]
#                tmp['HIGH'] = tmp_hi
#                tmp['LOW'] = tmp_lo
#                data_df = data_df.append(tmp)
#                i = 3
#            except:
#                i = i + 1
#                print("trying again")
        tmp = eikon_ohlcvoi_batch_retrieval(platform_symbol,exchange_symbol,start_date=start,end_date=end)
        tmp['OPEN'] = [billprice(row['OPEN']/100, index.strftime("%Y-%m-%d"), maturity_date, issue_date) for index, row in tmp.iterrows()]
        tmp['CLOSE'] = [billprice(row['CLOSE']/100, index.strftime("%Y-%m-%d"), maturity_date, issue_date) for index, row in tmp.iterrows()]
        tmp_hi = [billprice(row['LOW']/100, index.strftime("%Y-%m-%d"), maturity_date, issue_date) for index, row in tmp.iterrows()]
        tmp_lo = [billprice(row['HIGH']/100, index.strftime("%Y-%m-%d"), maturity_date, issue_date) for index, row in tmp.iterrows()]
        tmp['HIGH'] = tmp_hi
        tmp['LOW'] = tmp_lo
        data_df = data_df.append(tmp)

    # rearrange columns
    data_df = data_df[['exchange_symbol','OPEN','HIGH','LOW','CLOSE','VOLUME','open_interest']]
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
