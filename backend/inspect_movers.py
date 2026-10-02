import asyncio
from mcp import Client

async def main():
    async with Client("https://mcp.nseindia.in/cmmkt/mcp") as client:
        print("CONNECTED")

        result = await client.call_tool(
            "nse_get_market_movers",
            {}
        )

        print("RESULT:")
        print(result)

asyncio.run(main())
