from ws_basics import ReconnectingWebsocket

# Spot WS Base URL
SPOT_STREAM_URL = 'wss://stream.binance.com:9443/'

# USDT Futures WS Base URL
USDT_FUTURES_FSTREAM_URL = 'wss://fstream.binance.com/'

# Coin Futures WS Base URL
COIN_FUTURES_DSTREAM_URL = 'wss://dstream.binance.com/'


def get_coin_futures_multi_candlesticks_socket(symbols, time_inteval):
    """
    Returns a WebSocket connection for multiple symbols' K-line data for coin-margined futures.
    """
    channels = [f'{s.lower()}@kline_{time_inteval}' for s in symbols]
    return ReconnectingWebsocket(
        path='/'.join(channels),
        url=COIN_FUTURES_DSTREAM_URL,
        prefix='stream?streams=',
    )


def get_usdt_futures_multi_candlesticks_socket(symbols, time_inteval):
    """
    Returns a WebSocket connection for multiple symbols' K-line data for USDT-margined futures.
    """
    channels = [f'{s.lower()}@kline_{time_inteval}' for s in symbols]
    return ReconnectingWebsocket(
        path='/'.join(channels),
        url=USDT_FUTURES_FSTREAM_URL,
        prefix='stream?streams=',
    )


def get_spot_multi_candlesticks_socket(symbols, time_inteval):
    """
    Returns a WebSocket connection for multiple symbols' K-line data for spot trading.
    """
    channels = [f'{s.lower()}@kline_{time_inteval}' for s in symbols]
    return ReconnectingWebsocket(
        path='/'.join(channels),
        url=SPOT_STREAM_URL,
        prefix='stream?streams=',
    )
