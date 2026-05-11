"""Smoke test: exercise the claude-delegate server end-to-end as a real MCP client."""

import asyncio
import os
import sys
import time

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

        print("\n--- delegate (fire-and-await) ---")
        result = await client.call_tool(
            "delegate",
            {"prompt": "Reply with exactly the string PONG and nothing else.",
             "timeout_seconds": 60},
        )
        print("delegate output:", repr(result.data[:80]))

        print("\n--- submit + poll ---")
        submitted = await client.call_tool(
            "submit",
            {"prompt": "Reply with exactly the string ASYNC_PONG and nothing else.",
             "timeout_seconds": 60},
        )
        print("submit:", submitted.data)
        task_id = submitted.data["task_id"]

        next_byte = 0
        for _ in range(20):
            r = await client.call_tool(
                "poll",
                {"task_id": task_id, "since_byte": next_byte, "wait_seconds": 10},
            )
            d = r.data
            if d.get("output"):
                print(f"  chunk @{next_byte}: {d['output'][:80]!r}")
            next_byte = d["next_byte"]
            if d["done"]:
                print(f"  done: status={d['status']} rc={d['return_code']}")
                break

        print("\n--- conversation continuity ---")
        conv = f"smoketest-{int(time.time())}"
        print(f"  using conversation_id={conv!r}")
        first = await client.call_tool(
            "delegate",
            {"prompt": "Please remember the secret number 8675309. "
                       "Reply with exactly 'OK' and nothing else.",
             "conversation_id": conv,
             "timeout_seconds": 120},
        )
        print(f"  turn 1 reply: {first.data[:80]!r}")

        second = await client.call_tool(
            "delegate",
            {"prompt": "What secret number did I ask you to remember? "
                       "Reply with just the digits, nothing else.",
             "conversation_id": conv,
             "timeout_seconds": 120},
        )
        reply2 = second.data.strip()
        print(f"  turn 2 reply: {reply2[:80]!r}")
        ok = "8675309" in reply2
        print(f"  continuity {'PASSED' if ok else 'FAILED'} — number recalled: {ok}")

        convs = await client.call_tool("list_conversations", {"limit": 5})
        for c in convs.data:
            print(f"  conv: {c['conversation_id']} session={c['session_id'][:8]}… "
                  f"turns={c['turns']} cwd={c['cwd']}")

        forgotten = await client.call_tool(
            "forget_conversation", {"conversation_id": conv}
        )
        print(f"  forget_conversation: {forgotten.data}")

        print("\n--- submit + cancel ---")
        slow = await client.call_tool(
            "submit",
            {"prompt": "Count slowly from 1 to 100, one number per line, "
                       "no other text. Take your time.",
             "timeout_seconds": 300},
        )
        slow_id = slow.data["task_id"]
        print("submit slow:", slow_id)
        await asyncio.sleep(3)
        cancelled = await client.call_tool("cancel", {"task_id": slow_id})
        print("cancel:", cancelled.data)
        post = await client.call_tool(
            "poll", {"task_id": slow_id, "wait_seconds": 5}
        )
        print("post-cancel poll status:", post.data["status"],
              "done:", post.data["done"])

        print("\n--- list_tasks ---")
        listing = await client.call_tool("list_tasks", {"limit": 5})
        for row in listing.data:
            print(f"  {row['status']:10s} {row['task_id']} "
                  f"({row['output_bytes']}B) "
                  f"conv={row['conversation_id'] or '-':20s} "
                  f"{row['prompt_preview'][:30]!r}")

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
