import asyncio
from nse_client import get_stock_quote

async def main():
    result = await get_stock_quote("MEESHO")

    print("===== CONTENT COUNT =====")
    print(len(result.content))

    for i, part in enumerate(result.content):
        print(f"\n===== PART {i} =====")
        print("TYPE:", type(part))
        print("TEXT:")
        print(getattr(part, "text", None))

asyncio.run(main())
