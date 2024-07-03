import asyncio
import logging
import os

import pandas as pd

from candle_listener import CandleListener

logging.basicConfig(format='%(asctime)s (%(levelname)s) - %(message)s', level=logging.INFO, datefmt='%Y%m%d %H:%M:%S')


def update_candle_data(df_new: pd.DataFrame, symbol, time_interval, trade_type):
    """
    Writes the received K-line data DataFrame to the hard disk in Parquet format.
    """
    output_path = f'{trade_type}_{symbol}_{time_interval}.pqt'

    if not os.path.exists(output_path):
        df_new.to_parquet(output_path, compression='zstd')
        return

    df = pd.read_parquet(output_path)
    df = pd.concat([df, df_new])
    df.sort_index()
    df.drop_duplicates('candle_begin_time')
    df.to_parquet(output_path)


async def dispatcher(main_que: asyncio.Queue):
    """
    Consumer that processes the received K-line data.
    """
    while True:
        # Get data from the main queue
        req = await main_que.get()
        run_time = req['run_time']
        req_type = req['type']

        # Call the appropriate processing function based on the data type
        if req_type == 'candle_data':
            # K-line data update
            symbol = req['symbol']
            time_interval = req['time_interval']
            trade_type = req['trade_type']
            update_candle_data(req['data'], symbol, time_interval, trade_type)
            logging.info('Record %s %s-%s at %s', trade_type, symbol, time_interval, run_time)
        else:
            logging.warning('Unknown request %s %s', req_type, run_time)


async def main():
    logging.info('Start recording candlestick data')
    # Main queue
    main_que = asyncio.Queue()

    # Producers
    listener_usdt_perp_1m = CandleListener('usdt_futures', ['BTCUSDT', 'ETHUSDT'], '1m', main_que)
    listener_coin_perp_3m = CandleListener('coin_futures', ['BTCUSD_PERP', 'ETHUSD_PERP'], '3m', main_que)
    listener_spot_1m = CandleListener('spot', ['BTCUSDT', 'BNBUSDT'], '1m', main_que)

    # Consumer
    dispatcher_task = dispatcher(main_que)

    await asyncio.gather(listener_usdt_perp_1m.start_listen(), listener_coin_perp_3m.start_listen(),
                         listener_spot_1m.start_listen(), dispatcher_task)


if __name__ == '__main__':
    asyncio.run(main())
