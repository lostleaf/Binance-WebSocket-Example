import asyncio
import logging
from datetime import datetime, timedelta

import pandas as pd
import pytz

from binance_market_ws import (get_coin_futures_multi_candlesticks_socket, get_spot_multi_candlesticks_socket,
                               get_usdt_futures_multi_candlesticks_socket)


def convert_to_dataframe(x, interval_delta):
    """
    Parse the dictionary returned by WS and return a DataFrame
    """
    columns = [
        'candle_begin_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trade_num',
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume'
    ]
    candle_data = [
        pd.to_datetime(int(x['t']), unit='ms', utc=True),
        float(x['o']),
        float(x['h']),
        float(x['l']),
        float(x['c']),
        float(x['v']),
        float(x['q']),
        float(x['n']),
        float(x['V']),
        float(x['Q'])
    ]

    # Use the K-line end time as the timestamp
    return pd.DataFrame(data=[candle_data], columns=columns, index=[candle_data[0] + interval_delta])


class CandleListener:

    # Mapping of trade types to ws functions
    TRADE_TYPE_MAP = {
        'usdt_futures': get_usdt_futures_multi_candlesticks_socket,
        'coin_futures': get_coin_futures_multi_candlesticks_socket,
        'spot': get_spot_multi_candlesticks_socket
    }

    def __init__(self, type_, symbols, time_interval, que):
        # Trade type
        self.trade_type = type_
        # Trading symbols
        self.symbols = set(symbols)
        # K-line period
        self.time_interval = time_interval
        self.interval_delta = convert_interval_to_timedelta(time_interval)
        # Message queue
        self.que: asyncio.Queue = que
        # Reconnection flag
        self.req_reconnect = False

    async def start_listen(self):
        """
        Main function for WS listening
        """

        if not self.symbols:
            return
        
        socket_func = self.TRADE_TYPE_MAP[self.trade_type]
        while True:
            # Create WS
            socket = socket_func(self.symbols, self.time_interval)
            async with socket as socket_conn:
                # After the WS connection is successful, receive and parse the data
                while True:
                    if self.req_reconnect:
                        # Reconnect if reconnection is required
                        self.req_reconnect = False
                        break
                    try:
                        res = await socket_conn.recv()
                        self.handle_candle_data(res)
                    except asyncio.TimeoutError: 
                        # Reconnect if no data is received for long (default 60 seconds)
                        # Normally K-line is pushed every 1-2 seconds                        
                        logging.error('Recv candle ws timeout, reconnecting')
                        break

    def handle_candle_data(self, res):
        """
        Handle data returned by WS
        """

        # Defensive programming, discard if Binance returns an error without the data field
        if 'data' not in res:
            return

        # Extract the data field
        data = res['data']

        # Defensive programming, discard if the data does not contain the e field or the e field (data type) is not kline or the data does not contain the k field (K-line data)
        if data.get('e', None) != 'kline' or 'k' not in data:
            return

        # Extract the k field, which is the K-line data
        candle = data['k']

        # Check if the K-line is closed, discard if not closed
        is_closed = candle.get('x', False)
        if not is_closed:
            return

        # Convert the K-line to a DataFrame
        df_candle = convert_to_dataframe(candle, self.interval_delta)

        # Put the K-line DataFrame into the communication queue
        self.que.put_nowait({
            'type': 'candle_data',
            'data': df_candle,
            'closed': is_closed,
            'run_time': df_candle.index[0],
            'symbol': data['s'],
            'time_interval': self.time_interval,
            'trade_type': self.trade_type,
            'recv_time': now_time()
        })

    def add_symbols(self, *symbols):
        for symbol in symbols:
            self.symbols.add(symbol)

    def remove_symbols(self, *symbols):
        for symbol in symbols:
            if symbol in self.symbols:
                self.symbols.remove(symbol)

    def reconnect(self):
        self.req_reconnect = True


def convert_interval_to_timedelta(time_interval: str) -> timedelta:
    if time_interval.endswith('m') or time_interval.endswith('T'):
        return timedelta(minutes=int(time_interval[:-1]))

    if time_interval.endswith('H') or time_interval.endswith('h'):
        return timedelta(hours=int(time_interval[:-1]))

    raise ValueError('time_interval %s format error', time_interval)


DEFAULT_TZ = pytz.timezone('hongkong')


def now_time() -> datetime:
    return datetime.now(DEFAULT_TZ)
