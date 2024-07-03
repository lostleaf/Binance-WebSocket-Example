import asyncio
import logging

from binance_market_ws import get_usdt_futures_multi_candlesticks_socket


async def main():
    socket = get_usdt_futures_multi_candlesticks_socket(['BTCUSDT'], '1m')
    async with socket as socket_conn:
        while True:
            try:
                res = await socket_conn.recv()
                print(res)
            except asyncio.TimeoutError:
                logging.error('Recv candle ws timeout')
                break


if __name__ == '__main__':
    asyncio.run(main())
