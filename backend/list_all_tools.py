import asyncio
from mcp import Client

async def main():
    async with Client("https://mcp.nseindia.in/cmmkt/mcp") as client:
        result = await client.list_tools()

        print("===== ALL NSE MCP TOOLS =====")
        for tool in result.tools:
            print(tool.name)

asyncio.run(main())
