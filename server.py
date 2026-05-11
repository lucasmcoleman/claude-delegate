"""Claude-delegate MCP server.

Exposes one MCP tool — `delegate` — that runs a prompt through `claude -p`
on this machine and returns whatever it produced. Intended to let a Claude
Code instance on another machine hand work to the Claude Code on this one.
"""

from __future__ import annotations

import asyncio
import logging
import os
import shlex
from pathlib import Path

from fastmcp import FastMCP
from fastmcp.server.auth.providers.jwt import StaticTokenVerifier

CLAUDE_BIN = os.environ.get("CLAUDE_BIN", "/home/lucas/.local/bin/claude")
DEFAULT_CWD = os.environ.get("DELEGATE_DEFAULT_CWD", str(Path.home()))
MAX_TIMEOUT = int(os.environ.get("DELEGATE_MAX_TIMEOUT", "1800"))
BIND_HOST = os.environ.get("DELEGATE_HOST", "0.0.0.0")
BIND_PORT = int(os.environ.get("DELEGATE_PORT", "4115"))
TOKEN = os.environ["DELEGATE_TOKEN"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("claude-delegate")

verifier = StaticTokenVerifier(
    tokens={
        TOKEN: {
            "client_id": "lan-peer",
            "scopes": ["delegate"],
        }
    },
    required_scopes=["delegate"],
)

mcp = FastMCP(
    name="claude-delegate",
    instructions=(
        "Run a Claude Code prompt on a different machine. "
        "Use when a task is clearly tied to that machine — accessing files, "
        "running services, querying its databases, building containers, etc. "
        "The remote Claude executes headless and returns its final answer."
    ),
    auth=verifier,
)


@mcp.tool
async def delegate(
    prompt: str,
    cwd: str | None = None,
    timeout_seconds: int = 600,
    model: str | None = None,
) -> str:
    """Run a prompt through Claude Code in headless mode on this machine.

    Args:
        prompt: The task for the remote Claude. Be specific; it has no
            conversation context from your session.
        cwd: Working directory the remote Claude should run in (defaults to
            the service user's home). Use this to point Claude at a specific
            project so it picks up the right CLAUDE.md / .git context.
        timeout_seconds: Hard cap on how long the remote Claude may run.
            Capped server-side at DELEGATE_MAX_TIMEOUT.
        model: Optional model alias ('opus', 'sonnet', 'haiku') or full name.
            Omit to use the remote Claude's configured default.

    Returns:
        Whatever the remote Claude printed to stdout (and stderr on failure).
    """
    timeout = min(max(timeout_seconds, 10), MAX_TIMEOUT)

    work_dir = cwd or DEFAULT_CWD
    if not Path(work_dir).is_dir():
        return f"ERROR: cwd does not exist: {work_dir}"

    argv: list[str] = [
        CLAUDE_BIN,
        "-p",
        "--permission-mode", "bypassPermissions",
        "--output-format", "text",
    ]
    if model:
        argv += ["--model", model]
    argv.append(prompt)

    log.info(
        "delegate: cwd=%s timeout=%ss model=%s prompt=%s",
        work_dir, timeout, model or "<default>", shlex.quote(prompt[:120]),
    )

    proc = await asyncio.create_subprocess_exec(
        *argv,
        cwd=work_dir,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(), timeout=timeout
        )
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        return f"ERROR: claude exceeded {timeout}s timeout"

    out = stdout.decode(errors="replace").rstrip()
    err = stderr.decode(errors="replace").rstrip()
    if proc.returncode != 0:
        return f"ERROR: claude exited {proc.returncode}\n--- stdout ---\n{out}\n--- stderr ---\n{err}"
    if err:
        log.info("delegate stderr: %s", err[:500])
    return out


@mcp.tool
def info() -> dict:
    """Return basic info about this delegation endpoint."""
    return {
        "hostname": os.uname().nodename,
        "user": os.environ.get("USER", "?"),
        "claude_bin": CLAUDE_BIN,
        "default_cwd": DEFAULT_CWD,
        "max_timeout_seconds": MAX_TIMEOUT,
    }


if __name__ == "__main__":
    log.info("starting claude-delegate on %s:%s", BIND_HOST, BIND_PORT)
    mcp.run(transport="http", host=BIND_HOST, port=BIND_PORT)
