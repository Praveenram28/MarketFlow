import asyncio
from mcp import Client


NSE_MCP_URL = "https://mcp.nseindia.in/cmmkt/mcp"


async def get_stock_quote(symbol: str):
    """Get the latest available NSE quote for one stock."""
    async with Client(NSE_MCP_URL) as client:
        result = await client.call_tool(
            "cm_get_stock_quote",
            {
                "symbol": symbol.upper()
            }
        )

        return result


async def get_live_gainers():
    """Get current NSE live gainers."""
    async with Client(NSE_MCP_URL) as client:
        result = await client.call_tool(
            "cm_get_live_gainers",
            {}
        )

        return result


async def get_live_losers():
    """Get current NSE live losers."""
    async with Client(NSE_MCP_URL) as client:
        result = await client.call_tool(
            "cm_get_live_losers",
            {}
        )

        return result


async def get_index_data(index_name: str):
    """Get the latest available NSE index data."""

    import requests

    def fetch():
        url = "https://www.nseindia.com/api/allIndices"

        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "Referer": "https://www.nseindia.com/",
        }

        response = requests.get(
            url,
            headers=headers,
            timeout=15
        )

        response.raise_for_status()

        data = response.json().get("data", [])

        # Convert requested index name to uppercase
        target = index_name.upper().strip()

        # NSE index name aliases
        aliases = {
            "NIFTY": "NIFTY 50",
            "NIFTY50": "NIFTY 50",
            "BANKNIFTY": "NIFTY BANK",
            "NIFTYBANK": "NIFTY BANK",
            "INDIAVIX": "INDIA VIX",
        }

        target = aliases.get(target, target)

        # Find matching index
        for item in data:

            index_symbol = str(
                item.get("indexSymbol", "")
            ).upper()

            index = str(
                item.get("index", "")
            ).upper()

            if target == index_symbol or target == index:
                return item

        return None

    return await asyncio.to_thread(fetch)


if __name__ == "__main__":

    async def test():
        print("Testing NSE MCP...")

        # Test stock
        print("\nMEESHO:")
        stock_result = await get_stock_quote("MEESHO")
        print(stock_result)

        # Test NIFTY 50
        print("\nNIFTY 50:")
        nifty_result = await get_index_data("NIFTY")
        print(nifty_result)

        # Test NIFTY BANK
        print("\nNIFTY BANK:")
        bank_result = await get_index_data("BANKNIFTY")
        print(bank_result)

    asyncio.run(test())