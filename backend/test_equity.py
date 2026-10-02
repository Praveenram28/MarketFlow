import asyncio
from mcp import Client

NSE_MCP_URL = "https://mcp.nseindia.in/cmmkt/mcp"


async def main():
    async with Client(NSE_MCP_URL) as client:
        result = await client.call_tool(
            "cm_get_allstocks_status",
            {}
        )

        print("NSE ALL-STOCKS STATUS:")
        print(result)


if __name__ == "__main__":
    asyncio.run(main())