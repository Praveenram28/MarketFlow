import asyncio
from nse_client import get_stock_quote

async def main():
    result = await get_stock_quote("MEESHO")

    print("===== RAW NSE RESPONSE =====")
    print(result)

asyncio.run(main())
