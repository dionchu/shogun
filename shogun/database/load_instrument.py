import os.path
import numpy as np
import pandas as pd
import logging
import eikon as ek
ek.set_app_key('48f17fdf21184b0ca9c4ea8913a840a92b338918')
from .future_root_factory import FutureRootFactory

from pandas import read_hdf
from pandas import HDFStore,DataFrame

from shogun.utils.query_utils import query_df

import os
dirname = os.path.dirname(__file__)

import logging

#run through loop to get data for all, keeping in mind 5 year limit to history
def put_to_hdf(df):
    hdf = HDFStore(dirname + "\_InstrumentData.5")
    hdf.put('InstrumentData', df, format='table', data_columns=True)
    hdf.close() # closes the file

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

future_instrument_df = pd.DataFrame(columns = columns)

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
    tmp = pd.merge(tmp_ohlcv,tmp_oi,left_index=True,right_index=True)
    return tmp

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

    while int((end_date - start_date).days) > 1827:
        temp_end_date = start_date + pd.DateOffset(years=5)
        tmp = get_eikon_ohlcv_oi(eikon_symbol,exchange_symbol,start_date.strftime("%Y-%m-%d"),temp_end_date.strftime("%Y-%m-%d"))
        data_df = data_df.append(tmp)
        counter += 1
        start_date = temp_end_date

    tmp = get_eikon_ohlcv_oi(eikon_symbol,exchange_symbol,start_date.strftime("%Y-%m-%d"),end_date.strftime("%Y-%m-%d"))
    return data_df.append(tmp)


def write_future(factory,root_symbol):
        """
        write new future instruments to table.
        """
        logging.basicConfig(filename='write_future'+pd.Timestamp('today').strftime("%Y%m%d.%H.%M")+'.log',level=logging.DEBUG)
        logging.info('Started')
        # Construct futures instruments data
        root_chain_df = factory.make_root_chain(root_symbol)
        root_info_dict = factory.retrieve_root_info(root_symbol)

        # Convert pandas to dict for ease of extraction and indexing
        root_chain_dict = root_chain_df.set_index('platform_symbol').to_dict()
        platform_symbol_list = list(root_chain_dict['exchange_symbol'].keys())

        # Loop through symbols and save in data frame
        data_df = pd.DataFrame()
        for platform_symbol in platform_symbol_list:
            print(platform_symbol)
            exchange_symbol = root_chain_dict['exchange_symbol'][platform_symbol]
            start = root_chain_dict['first_trade'][platform_symbol].strftime("%Y-%m-%d")
            end = root_chain_dict['last_trade'][platform_symbol].strftime("%Y-%m-%d")
            tmp = eikon_ohlcvoi_batch_retrieval(platform_symbol,exchange_symbol,start_date=start,end_date=end)
            data_df = data_df.append(tmp)

        # Change default column names to lower case
        data_df.columns = ['exchange_symbol','open','high','low','close','volume','open_interest']

        data_df.index.name = 'date'
        data_df.set_index(['exchange_symbol'], append=True, inplace=True)

        # Append data to hdf, remove duplicates, and write to both hdf and csv
        instrument_data_hdf = read_hdf(dirname +'\_InstrumentData.h5')
        instrument_data_hdf = instrument_data_hdf.append(data_df).drop_duplicates()
        instrument_data_hdf.to_hdf(dirname +'\_InstrumentData.h5', 'InstrumentData', mode = 'w',
           format='table', data_columns=True)
        instrument_data_hdf.to_csv(dirname + "\_InstrumentData.csv")

        # Combine futures instrument information and calculated dates
        root_info_and_chain = pd.concat([pd.DataFrame.from_dict(root_info_dict),root_chain_df],axis=1).fillna(method='ffill')

        # Reset index to columns
        data_df.reset_index(level=[0,1], inplace=True)

        # Calculate start and end dates
        start_end_df = pd.DataFrame(
            {'start_date': data_df.groupby(['exchange_symbol']).first()['date'],
            'end_date': data_df.groupby(['exchange_symbol']).last()['date']
            })

        # inner join with future_instrument_df to enforce column structure, merge start and end
        metadata_df = pd.concat([future_instrument_df,root_info_and_chain], join = "inner")
        metadata_df = pd.merge(metadata_df,start_end_df, on='exchange_symbol')
        metadata_df = pd.concat([future_instrument_df,metadata_df])
        metadata_df['delivery_month'] = metadata_df['delivery_month'].astype(str).astype(int)
        metadata_df['delivery_year'] = metadata_df['delivery_year'].astype(str).astype(int)

        metadata_df.set_index(['exchange_symbol'], append=True, inplace=True)

        # Assumes that hdf exists
        # Append data to hdf, remove duplicates, and write to both hdf and csv
        if os.path.isfile(dirname + "\_FutureInstrument.h5"):
            future_instrument_hdf = read_hdf(dirname +'\_FutureInstrument.h5')
            future_instrument_hdf = future_instrument_hdf.append(metadata_df).drop_duplicates(keep = 'last')
            future_instrument_hdf.to_hdf(dirname +'\_FutureInstrument.h5', 'FutureInstrument', mode = 'w',
               format='table', data_columns=True)
            future_instrument_hdf.to_csv(dirname + "\_FutureInstrument.csv")
        else:
            metadata_df.to_hdf(dirname +'\_FutureInstrument.h5', 'FutureInstrument', mode = 'w',
               format='table', data_columns=True)
            metadata_df.to_csv(dirname + "\_FutureInstrument.csv")


        # Assumes that hdf exists
        # Assign instrument routing information to table
        instrument_router_df = pd.DataFrame({'instrument_type': ['Future']}, index=metadata_df.index)

        if os.path.isfile(dirname + "\_InstrumentRouter.h5"):
            instrument_router_hdf = read_hdf(dirname +'\_InstrumentRouter.h5')
            instrument_router_hdf = instrument_router_hdf.append(instrument_router_df).drop_duplicates(keep = 'last')
            instrument_router_hdf.to_hdf(dirname +'\_InstrumentRouter.h5', 'InstrumentRouter', mode = 'w',
               format='table', data_columns=True)
            instrument_router_hdf.to_csv(dirname + "\_InstrumentRouter.csv")
        else:
            instrument_router_df.to_hdf(dirname +'\_InstrumentRouter.h5', 'InstrumentRouter', mode = 'w',
               format='table', data_columns=True)
            instrument_router_df.to_csv(dirname + "\_InstrumentRouter.csv")

        logging.info('Finished')

        return data_df

def update_future(factory,root_symbol,dt,platform='RIC'):
    """
    update future to table
    """
    logging.basicConfig(filename='update_future'+pd.Timestamp('today').strftime("%Y%m%d.%H.%M")+'.log',level=logging.DEBUG)
    logging.info('Started')

    exchange_id = query_df(factory._future_root,
                            {'root_symbol': root_symbol}
                            )['parent_calendar_id'].to_string(index=False)

    # check if today is exchange holiday/weekend
    if not trading_calendars.get_calendar(exchange_id).is_session(dt):
        print('{dt} not a valid session: do nothing'.format(dt=today.strftime("%Y-%m-%d")))
        return

    # compare todays list to existing FutureInstrument table
    database_contracts = read_hdf(dirname + '\_FutureInstrument.h5',where="root_symbol=" + root_symbol)
    current_listing = factory.get_contract_listing(root_symbol,dt)
    missing_contracts = current_listing[~current_listing.exchange_symbol.isin(database_contracts.index)]
    existing_contracts = current_listing[current_listing.exchange_symbol.isin(database_contracts.index)]

    # for those already in the Future Instrument table, get query start date
    existing_root_chain_df = database_contracts[database_contracts.index.isin(existing_contracts.exchange_symbol)].reset_index(level=[0])
    existing_root_chain_df.insert(0,'platform_symbol', [factory.exchange_symbol_to_ticker(x) for x in existing_root_chain_df.exchange_symbol])
    existing_root_chain_dict = existing_root_chain_df.set_index('platform_symbol').to_dict()

    existing_query_start = database_contracts[
                    database_contracts.index.isin(existing_contracts.exchange_symbol)
                    ].to_dict()['end_date']
    existing_query_start = existing_root_chain_dict['end_date']

    # for those that are new, make root chain and calculate query start date
    missing_root_info_dict = factory.retrieve_root_info(root_symbol)
    missing_root_chain_df = factory.make_root_chain(root_symbol,
                                                    pd.Timestamp(missing_contracts.index[0], tz='UTC'),
                                                    pd.Timestamp(missing_contracts.index[-1], tz='UTC'))
    missing_root_chain_dict = missing_root_chain_df.set_index('platform_symbol').to_dict()

    if 'first_trade' not in missing_root_chain_df.columns:
        root_df = query_df(factory._future_contract_listing, {'root_symbol': root_symbol})
        root_dict = root_df.set_index('delivery_month').to_dict()
        missing_dict = missing_root_chain_df.set_index('exchange_symbol').to_dict()

        missing_query_start = {}
        for exchange_symbol in missing_root_chain_df.exchange_symbol:
            month_code = exchange_symbol[-3:][0]
            query_start[exchange_symbol] = pd.date_range(end = missing_dict['last_trade'][exchange_symbol],
                            periods=root_dict['periods'][month_code],
                            freq=root_dict['frequency'][month_code])[0] + MonthBegin(n=-1)
    else:
        missing_query_start = missing_root_chain_df.set_index('exchange_symbol').to_dict()['first_trade']

    # combine information in dictionary
    platform_query = {
    'last_trade': dict(existing_root_chain_dict['last_trade'], **missing_root_chain_dict['last_trade']),
    'exchange_symbol': dict(existing_root_chain_dict['exchange_symbol'], **missing_root_chain_dict['exchange_symbol']),
    'start_date': dict({factory.exchange_symbol_to_ticker(key): value for (key, value) in existing_query_start.items()},
                        **{factory.exchange_symbol_to_ticker(key): value for (key, value) in missing_query_start.items()})
    }

    # Loop through symbols and pull raw data into data frame
    today = pd.Timestamp(date.today())
    data_df = pd.DataFrame()
    for platform_symbol in platform_query['exchange_symbol'].keys():
        print(platform_symbol)
        exchange_symbol = platform_query['exchange_symbol'][platform_symbol]
        start = platform_query['start_date'][platform_symbol].strftime("%Y-%m-%d")
        end = dt.strftime("%Y-%m-%d")
        if(today <= platform_query['last_trade'][platform_symbol]):
            tmp = eikon_ohlcvoi_batch_retrieval(platform_symbol.split('^')[0],exchange_symbol,start_date=start,end_date=end)
        else:
            tmp = eikon_ohlcvoi_batch_retrieval(platform_symbol,exchange_symbol,start_date=start,end_date=end)
        data_df = data_df.append(tmp)

    # Change default column names to lower case
    data_df.columns = ['exchange_symbol','open','high','low','close','volume','open_interest']

    data_df.index.name = 'date'
    data_df.set_index(['exchange_symbol'], append=True, inplace=True)

    check_missing_extra_days(data_df)

    # Append data to hdf, remove duplicates, and write to both hdf and csv
    instrument_data_hdf = read_hdf(dirname +'\_InstrumentData.h5')
    instrument_data_hdf = instrument_data_hdf.append(data_df).drop_duplicates()
    instrument_data_hdf.to_hdf(dirname +'\_InstrumentData.h5', 'InstrumentData', mode = 'w',
       format='table', data_columns=True)
    instrument_data_hdf.to_csv(dirname + "\_InstrumentData.csv")

    # Combine futures instrument information and calculated dates
    root_info_and_chain = pd.concat([pd.DataFrame.from_dict(missing_root_info_dict),missing_root_chain_df],axis=1).fillna(method='ffill')

    # Reset index to columns
    data_df.reset_index(level=[0,1], inplace=True)

    # Calculate start and end dates
    start_end_df = pd.DataFrame(
        {'start_date': data_df.groupby(['exchange_symbol']).first()['date'],
        'end_date': data_df.groupby(['exchange_symbol']).last()['date']
        })

    # filter for common symbols
    common_symbols = set(root_info_and_chain.exchange_symbol).intersection(start_end_df.index)
    root_info_and_chain[root_info_and_chain['exchange_symbol'].isin(common_symbols)]
    missing_start_end_df = start_end_df.loc[common_symbols]
    missing_start_end_df.reset_index(level=[0],inplace=True)

    # inner join with future_instrument_df to enforce column structure, merge start and end
    metadata_df = pd.concat([future_instrument_df,root_info_and_chain], join = "inner")
    metadata_df = pd.merge(metadata_df,missing_start_end_df, on='exchange_symbol')
    metadata_df = pd.concat([future_instrument_df,metadata_df])
    metadata_df['delivery_month'] = metadata_df['delivery_month'].astype(str).astype(int)
    metadata_df['delivery_year'] = metadata_df['delivery_year'].astype(str).astype(int)

    metadata_df.set_index(['exchange_symbol'], inplace=True)

    # Append data to hdf, remove duplicates, and write to both hdf and csv
    if os.path.isfile(dirname + "\_FutureInstrument.h5"):
        future_instrument_hdf = read_hdf(dirname +'\_FutureInstrument.h5')
        # update end dates in metadata
        for symbol in existing_contracts.exchange_symbol:
            future_instrument_hdf.at[symbol, 'end_date'] = start_end_df.to_dict()['end_date'][symbol]
        future_instrument_hdf = future_instrument_hdf.append(metadata_df).drop_duplicates(keep = 'last')
        future_instrument_hdf.to_hdf(dirname +'\_FutureInstrument.h5', 'FutureInstrument', mode = 'w',
           format='table', data_columns=True)
        future_instrument_hdf.to_csv(dirname + "\_FutureInstrument.csv")
    else:
        metadata_df.to_hdf(dirname +'\_FutureInstrument.h5', 'FutureInstrument', mode = 'w',
           format='table', data_columns=True)
        metadata_df.to_csv(dirname + "\_FutureInstrument.csv")

    # Assign instrument routing information to table
    instrument_router_df = pd.DataFrame({'instrument_type': ['Future']}, index=metadata_df.index)

    if os.path.isfile(dirname + "\_InstrumentRouter.h5"):
        instrument_router_hdf = read_hdf(dirname +'\_InstrumentRouter.h5')
        instrument_router_hdf = instrument_router_hdf.append(instrument_router_df).drop_duplicates(keep = 'last')
        instrument_router_hdf.to_hdf(dirname +'\_InstrumentRouter.h5', 'InstrumentRouter', mode = 'w',
           format='table', data_columns=True)
        instrument_router_hdf.to_csv(dirname + "\_InstrumentRouter.csv")
    else:
        instrument_router_df.to_hdf(dirname +'\_InstrumentRouter.h5', 'InstrumentRouter', mode = 'w',
           format='table', data_columns=True)
        instrument_router_df.to_csv(dirname + "\_InstrumentRouter.csv")

    logging.info('Finished')

    return data_df

def check_missing_extra_days(data_df):

    data_df.reset_index(level=[0], inplace=True)
    grouped_df = data_df.groupby('exchange_symbol')

    for exchange_symbol in grouped_df.groups:
        root_symbol, suffix = exchange_symbol.split("_")
        exchange_id = query_df(factory._future_root,
                                {'root_symbol': root_symbol}
                                )['parent_calendar_id'].to_string(index=False)
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

        # create new continuous future for entire history
        #dump this into a csv file, either locally or ftp to blackbox
        #read the data in R and save to RData file in compatible format

        #one for us, one for eu, one for asia, or separated by exchange close times
        #check against thomson

        #setup account for each client
        #have algorithm run incrementally, day by day
        #have algorithm spit out orders with equity and vol info to file or shiny

        #have R trade on real prices rather than continuous prices
        #track roll dates

        #migrate all R code into python and integrate

        #track pnl and have program set up to take actual fills against benchmark

        #clean up code and start testing new strategies
