"""Smoke test: connect to the running claude-delegate server as a real MCP client."""

import asyncio
import os
import sys

from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport


URL = os.environ.get("DELEGATE_URL", "http://localhost:4115/mcp")
TOKEN = os.environ["DELEGATE_TOKEN"]


async def main() -> int:
    transport = StreamableHttpTransport(URL, auth=TOKEN)
    async with Client(transport) as client:
        print("ping:", await client.ping())

        tools = await client.list_tools()
        print("tools:", [t.name for t in tools])

        info = await client.call_tool("info", {})
        print("info:", info.data)

        result = await client.call_tool(
            "delegate",
            {"prompt": "Reply with exactly the string PONG and nothing else.", "timeout_seconds": 60},
        )
        print("delegate stdout:")
        print(result.data)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
