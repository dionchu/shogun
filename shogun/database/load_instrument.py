import os.path
import numpy as np
import pandas as pd
import logging
import trading_calendars
import time as time
import eikon as ek
ek.set_app_key('48f17fdf21184b0ca9c4ea8913a840a92b338918')
ek.set_app_key('4ed8362c27f846d09376992fae22fd34dd1c8950')
from .future_root_factory import FutureRootFactory

from pandas import read_hdf
from pandas import HDFStore,DataFrame

from shogun.utils.query_utils import query_df

import os
dirname = os.path.dirname(__file__)

import logging

from datetime import date
from pandas.tseries.offsets import *

columns =[
        'exchange_symbol',
        'root_symbol',
        'instrument_name',
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
        'last_trade_time',
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
        'exchange_id',
        'parent_calendar_id',
        'child_calendar_id',
        'average_pricing',
        'deliverable',
        'delivery_month',
        'delivery_year',
]

_instrument_timestamp_fields = frozenset({
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
})

def _convert_instrument_timestamp_fields(df):
    """
    Takes in a df of Instrument metadata columns and converts dates to pd.datetime64
    """
    for key in _instrument_timestamp_fields:
        df[key] = pd.to_datetime(df[key])
    return df

future_instrument_df = pd.DataFrame(columns = columns)

def rebuild_metadata(factory, root_symbol, start=None, end=None):
        if start:
            start = pd.Timestamp(start, tz='UTC')
        if end is None:
            end = date.today()
        end = pd.Timestamp(end)

        root_chain_df = factory.make_root_chain(root_symbol, start)
        root_info_dict = factory.retrieve_root_info(root_symbol)

        data_df = read_hdf(dirname +'\_InstrumentData.h5')

        start_end_df = calc_start_end_dates(data_df)
        metadata_df = construct_future_metadata(root_chain_df, root_info_dict, start_end_df)
        write_to_future_instrument(dirname, metadata_df)
        # Write to instrument router
        write_to_instrument_router(dirname, metadata_df)
        return metadata_df

def load_exchange_symbol(factory, fetch_symbols, root_symbol):
        """
        write exchange symbol to table.
        """
        end = date.today()
        if not isinstance(fetch_symbols, (list,)):
            fetch_symbols = [fetch_symbols]
        root_chain_df = factory.make_root_chain(root_symbol, start=None)
        root_chain_df = root_chain_df[root_chain_df['exchange_symbol'].isin(fetch_symbols)]
        root_info_dict = factory.retrieve_root_info(root_symbol)
        root_chain_dict = root_chain_df.set_index('platform_symbol').to_dict()
        root_listing_df = query_df(factory._future_contract_listing, {'root_symbol': root_symbol})
        root_listing_dict = root_listing_df.set_index('delivery_month').to_dict()

        if 'first_trade' not in root_chain_df.columns:
            first_trade = {}
            d = root_chain_df.set_index('exchange_symbol').to_dict()
            for exchange_symbol in root_chain_df.exchange_symbol:
                month_code = exchange_symbol[-3:][0]
                first_trade[exchange_symbol] = pd.date_range(end = d['last_trade'][exchange_symbol],
                                periods=root_listing_dict['periods'][month_code],
                                freq=root_listing_dict['frequency'][month_code])[0] + MonthBegin(n=-1)

            root_chain_dict['first_trade'] = {factory.exchange_symbol_to_ticker(key): value for (key, value) in first_trade.items()}

        # combine information in dictionary
        platform_query = {
        'last_trade': root_chain_dict['last_trade'],
        'exchange_symbol': root_chain_dict['exchange_symbol'],
        'start_date': root_chain_dict['first_trade'],
        }
        # Loop through symbols and pull raw data into data frame
        data_df = get_eikon_futures_data(platform_query, end)
        # If no data, exit, otherwise will overwrite database to empty
        if len(data_df) ==0:
            print("empty data set")
            return
        # Check missing days and days not expected
        check_missing_extra_days(factory, data_df.reset_index(level=[1]))
        # Append data to hdf, remove duplicates, and write to both hdf and csv
        write_to_instrument_table(dirname, data_df)
        # Construct and write metadata for missing contracts
        start_end_df = calc_start_end_dates(data_df)
        metadata_df = construct_future_metadata(root_chain_df, root_info_dict, start_end_df)
        write_to_future_instrument(dirname, metadata_df)
        # Write to instrument router
        write_to_instrument_router(dirname, metadata_df)

def load_future(factory, root_symbol, start=None, end=None):
        """
        write new future instruments to table.
        """
        if start:
            start = pd.Timestamp(start, tz='UTC')
        if end is None:
            end = date.today()
        end = pd.Timestamp(end, tz='UTC')
        logging.basicConfig(filename='./python_logs/write_future'+pd.Timestamp('today').strftime("%Y%m%d.%H.%M")+'.log',level=logging.DEBUG)
        logging.info('Started')
        # Construct futures instruments data
        root_chain_df = factory.make_root_chain(root_symbol, start)
        root_info_dict = factory.retrieve_root_info(root_symbol)
        root_chain_dict = root_chain_df.set_index('platform_symbol').to_dict()
        root_listing_df = query_df(factory._future_contract_listing, {'root_symbol': root_symbol})
        root_listing_dict = root_listing_df.set_index('delivery_month').to_dict()

        if 'first_trade' not in root_chain_df.columns:
            first_trade = {}
            d = root_chain_df.set_index('exchange_symbol').to_dict()
            for exchange_symbol in root_chain_df.exchange_symbol:
                month_code = exchange_symbol[-3:][0]
                first_trade[exchange_symbol] = pd.date_range(end = d['last_trade'][exchange_symbol],
                                periods=root_listing_dict['periods'][month_code],
                                freq=root_listing_dict['frequency'][month_code])[0] + MonthBegin(n=-1)

            root_chain_dict['first_trade'] = {factory.exchange_symbol_to_ticker(key): value for (key, value) in first_trade.items()}

        # combine information in dictionary
        platform_query = {
        'last_trade': root_chain_dict['last_trade'],
        'exchange_symbol': root_chain_dict['exchange_symbol'],
        'start_date': root_chain_dict['first_trade'],
        }

        platform_query_df = pd.DataFrame.from_dict(platform_query)
        platform_query = platform_query_df[platform_query_df['start_date'] <= end].to_dict()

        # Loop through symbols and pull raw data into data frame
        data_df = get_eikon_futures_data(platform_query, end)
        # Check missing days and days not expected
        check_missing_extra_days(factory, data_df.reset_index(level=[1]))
        # Append data to hdf, remove duplicates, and write to both hdf and csv
        write_to_instrument_table(dirname, data_df)
        # Construct and write metadata for missing contracts
        start_end_df = calc_start_end_dates(data_df)
        metadata_df = construct_future_metadata(root_chain_df, root_info_dict, start_end_df)
        write_to_future_instrument(dirname, metadata_df)
        # Write to instrument router
        write_to_instrument_router(dirname, metadata_df)

        logging.info('Finished')

def update_future(factory,root_symbol,dt,platform='RIC'):
    """
    update future to table
    """
    dt = pd.Timestamp(dt)
    logging.basicConfig(filename='./python_logs/update_future'+pd.Timestamp('today').strftime("%Y%m%d.%H.%M")+'.log',level=logging.DEBUG)
    logging.info('Started')

    exchange_id = query_df(factory._future_root,
                            {'root_symbol': root_symbol}
                            )['parent_calendar_id'].to_string(index=False)

    product_group_id = str(factory._future_root[
                        factory._future_root['root_symbol'] == root_symbol
                        ].set_index('root_symbol').to_dict()['child_calendar_id'][root_symbol])
    # if missing, set to calendar default
    if product_group_id == 'nan':
        # check if today is exchange holiday/weekend
        if not trading_calendars.get_calendar(exchange_id).is_session(dt):
            print('{dt} not a valid session: do nothing'.format(dt=dt.strftime("%Y-%m-%d")))
            return
    else:
        # check if today is exchange holiday/weekend
        if not trading_calendars.get_calendar(exchange_id, product_group_id).is_session(dt):
            print('{dt} not a valid session: do nothing'.format(dt=dt.strftime("%Y-%m-%d")))
            return

    # compare todays list to existing FutureInstrument table
    database_contracts = read_hdf(dirname + '\_FutureInstrument.h5',where="root_symbol=" + "\"" + root_symbol + "\"")
    current_listing = factory.get_contract_listing(root_symbol,dt)
    missing_contracts = current_listing[~current_listing.exchange_symbol.isin(database_contracts.index)]
    existing_contracts = current_listing[current_listing.exchange_symbol.isin(database_contracts.index)]

    # for those already in the Future Instrument table, get query start date
    existing_root_chain_df = database_contracts[database_contracts.index.isin(existing_contracts.exchange_symbol)].reset_index(level=[0])
    existing_root_chain_df.insert(0,'platform_symbol', [factory.exchange_symbol_to_ticker(x) for x in existing_root_chain_df.exchange_symbol])
    existing_root_chain_dict = existing_root_chain_df.set_index('platform_symbol').to_dict()

    # for those that are new, make root chain and calculate query start date
    if missing_contracts.shape[0] != 0:
        missing_root_info_dict = factory.retrieve_root_info(root_symbol)
        missing_root_chain_df = factory.make_root_chain(root_symbol,
                                                        pd.Timestamp(missing_contracts.index[0], tz='UTC'),
                                                        pd.Timestamp(missing_contracts.index[-1], tz='UTC'),
                                                        filter=False)

        missing_root_chain_dict = missing_root_chain_df.set_index('platform_symbol').to_dict()

        if 'first_trade' not in missing_root_chain_df.columns:
            root_df = query_df(factory._future_contract_listing, {'root_symbol': root_symbol})
            root_dict = root_df.set_index('delivery_month').to_dict()
            missing_dict = missing_root_chain_df.set_index('exchange_symbol').to_dict()

            missing_query_start = {}
            for exchange_symbol in missing_root_chain_df.exchange_symbol:
                month_code = exchange_symbol[-3:][0]
                missing_query_start[exchange_symbol] = pd.date_range(end = missing_dict['last_trade'][exchange_symbol],
                                periods=root_dict['periods'][month_code],
                                freq=root_dict['frequency'][month_code])[0] + MonthBegin(n=-1)
        else:
            missing_query_start = missing_root_chain_df.set_index('exchange_symbol').to_dict()['first_trade']

        # combine information in dictionary
        platform_query = {
        'last_trade': dict(existing_root_chain_dict['last_trade'], **missing_root_chain_dict['last_trade']),
        'exchange_symbol': dict(existing_root_chain_dict['exchange_symbol'], **missing_root_chain_dict['exchange_symbol']),
        'start_date': dict( existing_root_chain_dict['end_date'],
                            **{factory.exchange_symbol_to_ticker(key): value for (key, value) in missing_query_start.items()})
        }

    else:
        # combine information in dictionary
        platform_query = {
        'last_trade': existing_root_chain_dict['last_trade'],
        'exchange_symbol': existing_root_chain_dict['exchange_symbol'],
        'start_date': existing_root_chain_dict['end_date'],
        }

    platform_query_df = pd.DataFrame.from_dict(platform_query)
    platform_query = platform_query_df[platform_query_df['start_date'] <= dt].to_dict()

    # Loop through symbols and pull raw data into data frame
    data_df = get_eikon_futures_data(platform_query, dt)
    # Check missing symbols from platform_query
    check_missing_symbols(data_df,existing_contracts,missing_contracts)
    # Check missing days and days not expected
    check_missing_extra_days(factory, data_df.reset_index(level=[1]))
    # Append data to hdf, remove duplicates, and write to both hdf and csv
    write_to_instrument_table(dirname, data_df)
    # Construct and write metadata for missing contracts
    start_end_df = calc_start_end_dates(data_df)
    if missing_contracts.shape[0] != 0:
        metadata_df = construct_future_metadata(missing_root_chain_df, missing_root_info_dict, start_end_df)
    else:
        metadata_df = None
    write_to_future_instrument(dirname, metadata_df, existing_contracts, start_end_df)
    # Write to instrument router
    write_to_instrument_router(dirname, metadata_df)

    logging.info('Finished')

def get_eikon_futures_data(platform_query, dt):
    # Loop through symbols and pull raw data into data frame
    today = pd.Timestamp(date.today())
    data_df = pd.DataFrame()
    dt = pd.Timestamp(dt.strftime("%Y-%m-%d"))
    for platform_symbol in platform_query['exchange_symbol'].keys():
        print(platform_symbol)
        exchange_symbol = platform_query['exchange_symbol'][platform_symbol]
        start = min(platform_query['start_date'][platform_symbol], dt).strftime("%Y-%m-%d")
        end = min(platform_query['last_trade'][platform_symbol], dt).strftime("%Y-%m-%d")
        # RIC switches to expired RIC 4 calendar days after last trade
        i = 0
        while (i < 3):
            try:
                if(today <= platform_query['last_trade'][platform_symbol]+pd.Timedelta(days=4)):
                    tmp = eikon_ohlcvoi_batch_retrieval(platform_symbol.split('^')[0],exchange_symbol,start_date=start,end_date=end)
                else:
                    tmp = eikon_ohlcvoi_batch_retrieval(platform_symbol,exchange_symbol,start_date=start,end_date=end)
                data_df = data_df.append(tmp)
                i = 3
            except:
                i = i + 1
                print("trying again")

    # Change default column names to lower case
    data_df.columns = ['exchange_symbol','open','high','low','close','volume','open_interest']
    data_df.index.name = 'date'
    data_df.set_index(['exchange_symbol'], append=True, inplace=True)
    return data_df

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

def calc_start_end_dates(data_df):
    # Reset index to columns
    _data_df = data_df.reset_index(level=[0,1])

    # Calculate start and end dates
    start_end_df = pd.DataFrame(
        {'start_date': _data_df.groupby(['exchange_symbol']).first()['date'],
        'end_date': _data_df.groupby(['exchange_symbol']).last()['date']
        })
    return start_end_df

def construct_future_metadata(root_chain_df, root_info_dict, start_end_df):
    # Combine futures instrument information and calculated dates
    root_info_and_chain = pd.concat([pd.DataFrame.from_dict(root_info_dict),root_chain_df],axis=1).fillna(method='ffill')

    # filter for common symbols
    common_symbols = set(root_info_and_chain.exchange_symbol).intersection(start_end_df.index)
    root_info_and_chain[root_info_and_chain['exchange_symbol'].isin(common_symbols)]
    start_end_df = start_end_df.loc[common_symbols]
    start_end_df.reset_index(level=[0],inplace=True)

    if start_end_df.shape[0] != 0:
        # inner join with future_instrument_df to enforce column structure, merge start and end
        metadata_df = pd.concat([future_instrument_df,root_info_and_chain], join = "inner")
        metadata_df = pd.merge(metadata_df,start_end_df, on='exchange_symbol')
        metadata_df = pd.concat([future_instrument_df,metadata_df])
        metadata_df['deliverable'] = metadata_df['deliverable'].astype(str).astype(float)
        metadata_df['multiplier'] = metadata_df['multiplier'].astype(str).astype(float)
        metadata_df['underlying_asset_class_id'] = metadata_df['underlying_asset_class_id'].astype(str).astype(float)
        metadata_df['delivery_month'] = metadata_df['delivery_month'].astype(str).astype(float).astype(int)
        metadata_df['delivery_year'] = metadata_df['delivery_year'].astype(str).astype(float).astype(int)

        metadata_df.set_index(['exchange_symbol'], append=True, inplace=True)
        metadata_df.reset_index(level=[0],drop=True,inplace=True)
    else:
        metadata_df = None

    return metadata_df

def write_to_future_instrument(dirname, metadata_df=None, existing_contracts=None, start_end_df=None):
    # Append data to hdf, remove duplicates, and write to both hdf and csv
    if metadata_df is None and existing_contracts is None:
        return
    else:
        if os.path.isfile(dirname + "\_FutureInstrument.h5"):

            future_instrument_hdf = read_hdf(dirname +'\_FutureInstrument.h5')

            # update end dates in future instrument
            if existing_contracts is not None:
                for symbol in existing_contracts.exchange_symbol:
                    future_instrument_hdf.at[symbol, 'end_date'] = start_end_df.to_dict()['end_date'][symbol]
            # append new metadata to future instrument
            if metadata_df is not None:
                metadata_df = _convert_instrument_timestamp_fields(metadata_df)
                future_instrument_hdf = future_instrument_hdf.append(metadata_df)

            future_instrument_hdf = future_instrument_hdf[~future_instrument_hdf.index.duplicated(keep='last')]
            future_instrument_hdf.to_hdf(dirname +'\_FutureInstrument.h5', 'FutureInstrument', mode = 'w',
               format='table', data_columns=True)
            future_instrument_hdf.to_csv(dirname + "\_FutureInstrument.csv")
        else:
            if metadata_df is not None:
                metadata_df = _convert_instrument_timestamp_fields(metadata_df)
                metadata_df.to_hdf(dirname +'\_FutureInstrument.h5', 'FutureInstrument', mode = 'w',
                   format='table', data_columns=True)
                metadata_df.to_csv(dirname + "\_FutureInstrument.csv")


def write_to_instrument_router(dirname, metadata_df):
    # Assign instrument routing information to table
    if metadata_df is None:
        return
    else:
        instrument_router_df = pd.DataFrame({'instrument_type': ['Future']}, index=metadata_df.index)

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

def check_missing_symbols(data_df,existing_contracts,missing_contracts):
    data_check = [x for x in existing_contracts.append(missing_contracts).exchange_symbol if x not in data_df.head(-10).index.get_level_values(level=1)]
    if(len(data_check) > 0):
        return "Missing data for:" + str(data_check)

def check_missing_extra_days(factory, data_df):

#    data_df.reset_index(level=[0], inplace=True)
    grouped_df = data_df.groupby('exchange_symbol')

    for exchange_symbol in grouped_df.groups:
        root_symbol, suffix = exchange_symbol.split("_")
        exchange_id = query_df(factory._future_root,
                                {'root_symbol': root_symbol}
                                )['parent_calendar_id'].to_string(index=False)

        product_group_id = str(factory._future_root[
                        factory._future_root['root_symbol'] == root_symbol
                        ].set_index('root_symbol').to_dict()['child_calendar_id'][root_symbol])
        # if missing, set to calendar default
        if product_group_id == 'nan':
            cal = trading_calendars.get_calendar(exchange_id)
        else:
            cal = trading_calendars.get_calendar(exchange_id, product_group_id)

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
    e = ek.get_data(eikon_symbol, ['TR.OPENINTEREST.Date', 'TR.OPENINTEREST'], {'SDate':str(oi_start),'EDate':str(end_date)})
    tmp_oi = pd.DataFrame({'open_interest': e[0]['Open Interest'].values}, index = pd.to_datetime(e[0]['Date'].values)).shift(1)
    if( tmp_oi.shape == (1, 1) ):
        tmp_ohlcv['open_interest'] = pd.Series('NaN', index=tmp_ohlcv.index)
        tmp = tmp_ohlcv
        tmp['open_interest'] = tmp['open_interest'].astype(str).astype(float)
    else:
        tmp = pd.merge(tmp_ohlcv,tmp_oi,left_index=True,right_index=True,how='left')

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
