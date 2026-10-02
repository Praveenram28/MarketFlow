import asyncio
from mcp import Client

async def main():
    async with Client("https://mcp.nseindia.in/cmmkt/mcp") as client:
        result = await client.list_tools()

        print("NSE STOCK TOOLS:")
        for tool in result.tools:
            name = tool.name.lower()
            if "stock" in name or "all" in name or "equity" in name:
                print("-", tool.name)
                print(" ", tool.description)

asyncio.run(main())
