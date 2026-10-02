import asyncio
from nse_client import get_live_gainers, get_live_losers


async def main():
    print("===== NSE GAINERS =====")
    gainers = await get_live_gainers()
    print(gainers)

    print("\n===== NSE LOSERS =====")
    losers = await get_live_losers()
    print(losers)


asyncio.run(main())