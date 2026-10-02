import asyncio
from mcp import Client

async def main():
    async with Client("https://mcp.nseindia.in/cmmkt/mcp") as client:
        result = await client.call_tool("cm_get_live_gainers", {})
        print("LIVE GAINERS:")
        print(result)

asyncio.run(main())
