import json

import pandas as pd


def convert_to_dataframe(x, interval_delta):
    """
    Parse WS returned data dictionary, return as DataFrame
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

    # Use K-line end time as the timestamp
    return pd.DataFrame(data=[candle_data], columns=columns, index=[candle_data[0] + interval_delta])


def handle_candle_data(res, interval_delta):
    """
    Handle WS returned data
    """

    # Defensive programming, discard if Binance returns no data field
    if 'data' not in res:
        return

    # Extract data field
    data = res['data']

    # Defensive programming, discard if data does not contain e field or e field (data type) is not kline or data does not contain k field (K-line data)
    if data.get('e', None) != 'kline' or 'k' not in data:
        return

    # Extract k field, i.e., K-line data
    candle = data['k']

    # Determine if K-line is closed, discard if not closed
    is_closed = candle.get('x', False)
    if not is_closed:
        return

    # Convert K-line to DataFrame
    df_candle = convert_to_dataframe(candle, interval_delta)
    return df_candle


def main():
    # Load JSON data
    data = json.load(open('ex2_ws_candle.json'))

    # K-line interval is 1m
    interval_delta = pd.Timedelta(minutes=1)

    # Try to parse each WS data
    for idx, row in enumerate(data, 1):
        row_parsed = handle_candle_data(row, interval_delta)
        if row_parsed is None:
            print(f'Row{idx} is None')
        else:
            print(f'Row{idx} candlestick\n' + str(row_parsed))


if __name__ == '__main__':
    main()
