import asyncio
from mcp import Client

async def main():
    async with Client("https://mcp.nseindia.in/cmmkt/mcp") as client:
        result = await client.list_tools()

        for tool in result.tools:
            if tool.name in [
                "cm_get_live_gainers",
                "cm_get_live_losers",
                "cm_get_stock_quote",
                "nse_get_market_movers",
                "cm_get_allstocks_status"
            ]:
                print("\n==============================")
                print("TOOL:", tool.name)
                print("DESCRIPTION:", tool.description)
                print("INPUT:", tool.input_schema)

asyncio.run(main())
