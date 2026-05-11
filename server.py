"""Claude-delegate MCP server.

Lets a Claude Code instance on one machine hand work off to Claude Code on
this one.

Tools:
- submit(...)        -> {task_id, status}   start a job, returns immediately
- poll(task_id, ...) -> {status, output, next_byte, done, ...}
                                            check on a running/done job; long-polls
- cancel(task_id)    -> {status}            kill a running job
- list_tasks(limit)  -> [task summary, ...] recent jobs
- delegate(...)      -> str                 fire-and-await convenience wrapper
- info()             -> dict                hostname / config snapshot
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import aiosqlite
from fastmcp import FastMCP
from fastmcp.server.auth.providers.jwt import StaticTokenVerifier

CLAUDE_BIN = os.environ.get("CLAUDE_BIN", "/home/lucas/.local/bin/claude")
DEFAULT_CWD = os.environ.get("DELEGATE_DEFAULT_CWD", str(Path.home()))
MAX_TIMEOUT = int(os.environ.get("DELEGATE_MAX_TIMEOUT", "1800"))
BIND_HOST = os.environ.get("DELEGATE_HOST", "0.0.0.0")
BIND_PORT = int(os.environ.get("DELEGATE_PORT", "4115"))
DB_PATH = os.environ.get("DELEGATE_DB", "tasks.db")
TOKEN = os.environ["DELEGATE_TOKEN"]

ACTIVE_STATES = {"queued", "running"}
TERMINAL_STATES = {"completed", "failed", "cancelled", "timeout"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("claude-delegate")


@dataclass
class Job:
    task_id: str
    prompt: str
    cwd: str
    model: str | None
    timeout: int
    submitted_at: float
    started_at: float | None = None
    completed_at: float | None = None
    status: str = "queued"
    return_code: int | None = None
    error: str | None = None
    proc: asyncio.subprocess.Process | None = None
    output: bytearray = field(default_factory=bytearray)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    event: asyncio.Event = field(default_factory=asyncio.Event)

    def summary(self) -> dict:
        return {
            "task_id": self.task_id,
            "status": self.status,
            "submitted_at": self.submitted_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "return_code": self.return_code,
            "prompt_preview": self.prompt[:120],
            "cwd": self.cwd,
            "model": self.model,
            "output_bytes": len(self.output),
            "error": self.error,
        }


JOBS: dict[str, Job] = {}
_db_lock = asyncio.Lock()


SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    prompt TEXT NOT NULL,
    cwd TEXT NOT NULL,
    model TEXT,
    timeout_seconds INTEGER NOT NULL,
    submitted_at REAL NOT NULL,
    started_at REAL,
    completed_at REAL,
    status TEXT NOT NULL,
    return_code INTEGER,
    output BLOB,
    error TEXT
);
CREATE INDEX IF NOT EXISTS idx_tasks_submitted_at ON tasks(submitted_at DESC);
"""


async def _db_init() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA)
        await db.commit()


async def _db_archive(job: Job) -> None:
    async with _db_lock, aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT OR REPLACE INTO tasks
            (task_id, prompt, cwd, model, timeout_seconds,
             submitted_at, started_at, completed_at,
             status, return_code, output, error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job.task_id, job.prompt, job.cwd, job.model, job.timeout,
                job.submitted_at, job.started_at, job.completed_at,
                job.status, job.return_code, bytes(job.output), job.error,
            ),
        )
        await db.commit()


async def _db_load(task_id: str) -> Job | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM tasks WHERE task_id = ?", (task_id,)
        ) as cur:
            row = await cur.fetchone()
    if not row:
        return None
    job = Job(
        task_id=row["task_id"],
        prompt=row["prompt"],
        cwd=row["cwd"],
        model=row["model"],
        timeout=row["timeout_seconds"],
        submitted_at=row["submitted_at"],
        started_at=row["started_at"],
        completed_at=row["completed_at"],
        status=row["status"],
        return_code=row["return_code"],
        error=row["error"],
    )
    if row["output"]:
        job.output.extend(row["output"])
    job.event.set()
    return job


def _build_argv(job: Job) -> list[str]:
    argv = [
        CLAUDE_BIN,
        "-p",
        "--permission-mode", "bypassPermissions",
        "--output-format", "text",
    ]
    if job.model:
        argv += ["--model", job.model]
    argv.append(job.prompt)
    return argv


async def _run_job(job: Job) -> None:
    """Run claude -p for a job, streaming output into the job buffer.

    On any exit path — completion, timeout, cancellation, error — make sure
    the subprocess is reaped and the job is moved to a terminal state.
    """
    notify = job.event
    reader_task: asyncio.Task | None = None
    try:
        if not Path(job.cwd).is_dir():
            job.error = f"cwd does not exist: {job.cwd}"
            job.status = "failed"
            return

        job.status = "running"
        job.started_at = time.time()
        notify.set(); notify.clear()

        argv = _build_argv(job)
        log.info(
            "job %s start: cwd=%s model=%s timeout=%ss",
            job.task_id, job.cwd, job.model or "<default>", job.timeout,
        )

        try:
            job.proc = await asyncio.create_subprocess_exec(
                *argv,
                cwd=job.cwd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
        except FileNotFoundError as e:
            job.error = f"claude binary not found: {e}"
            job.status = "failed"
            return

        async def reader() -> None:
            assert job.proc and job.proc.stdout
            while True:
                chunk = await job.proc.stdout.read(4096)
                if not chunk:
                    return
                async with job.lock:
                    job.output.extend(chunk)
                notify.set(); notify.clear()

        reader_task = asyncio.create_task(reader())
        try:
            await asyncio.wait_for(job.proc.wait(), timeout=job.timeout)
        except asyncio.TimeoutError:
            job.proc.kill()
            await job.proc.wait()
            job.status = "timeout"
            job.error = f"exceeded {job.timeout}s timeout"
            return

        await reader_task
        reader_task = None
        job.return_code = job.proc.returncode
        if job.status not in TERMINAL_STATES:
            # Don't overwrite a status that cancel() already set.
            job.status = "completed" if job.return_code == 0 else "failed"

    except asyncio.CancelledError:
        job.status = "cancelled"
        job.error = "task cancelled"
        raise
    except Exception as e:
        log.exception("job %s crashed", job.task_id)
        job.status = "failed"
        job.error = f"{type(e).__name__}: {e}"
    finally:
        if job.proc and job.proc.returncode is None:
            try:
                job.proc.kill()
                await job.proc.wait()
            except ProcessLookupError:
                pass
        if reader_task and not reader_task.done():
            reader_task.cancel()
            try:
                await reader_task
            except (asyncio.CancelledError, Exception):
                pass
        job.completed_at = time.time()
        notify.set()
        try:
            await _db_archive(job)
        except Exception:
            log.exception("failed to archive job %s", job.task_id)
        log.info(
            "job %s end: status=%s rc=%s bytes=%d",
            job.task_id, job.status, job.return_code, len(job.output),
        )


verifier = StaticTokenVerifier(
    tokens={TOKEN: {"client_id": "lan-peer", "scopes": ["delegate"]}},
    required_scopes=["delegate"],
)

mcp = FastMCP(
    name="claude-delegate",
    instructions=(
        "Hand a task to Claude Code on a different machine. "
        "Prefer `submit` + `poll` for anything that might take >30s — that "
        "way you can check in or move on without blocking. Use `delegate` "
        "for quick one-shots. `cancel` aborts a running job."
    ),
    auth=verifier,
)


@mcp.tool
async def submit(
    prompt: str,
    cwd: str | None = None,
    timeout_seconds: int = 600,
    model: str | None = None,
) -> dict:
    """Start a Claude Code job on this machine. Returns immediately.

    Args:
        prompt: Task for the remote Claude. Be specific; it has no
            conversation context from your session.
        cwd: Working directory for the remote Claude (default: service
            default). Use this to point at a specific project so Claude
            picks up its CLAUDE.md / .git context.
        timeout_seconds: Hard cap on the job's runtime. Capped server-side
            at DELEGATE_MAX_TIMEOUT.
        model: Optional model alias ('opus', 'sonnet', 'haiku') or full name.

    Returns:
        {"task_id": "...", "status": "queued"}. Use `poll(task_id)` to get
        output and check completion.
    """
    timeout = min(max(int(timeout_seconds), 10), MAX_TIMEOUT)
    work_dir = cwd or DEFAULT_CWD
    job = Job(
        task_id=str(uuid.uuid4()),
        prompt=prompt,
        cwd=work_dir,
        model=model,
        timeout=timeout,
        submitted_at=time.time(),
    )
    JOBS[job.task_id] = job
    asyncio.create_task(_run_job(job))
    return {"task_id": job.task_id, "status": job.status}


@mcp.tool
async def poll(
    task_id: str,
    since_byte: int = 0,
    wait_seconds: int = 0,
) -> dict:
    """Check on a job. Returns new output since `since_byte` and current status.

    Use `wait_seconds > 0` to long-poll: the call will block up to that many
    seconds waiting for new output or completion before returning. This is
    much more efficient than tight-polling.

    Args:
        task_id: The id returned by `submit`.
        since_byte: Byte offset of last-seen output. Pass the `next_byte`
            from your previous poll. Defaults to 0 (return everything so far).
        wait_seconds: Max seconds to wait for new output / completion. 0
            returns immediately. Capped at 60.

    Returns:
        {
          "status": "running" | "completed" | "failed" | "cancelled" | "timeout",
          "output": "<text chunk since since_byte>",
          "next_byte": <int — pass to next poll call>,
          "done": true if status is terminal,
          "return_code": int|null,
          "error": str|null,
          "started_at": float|null, "completed_at": float|null
        }
    """
    job = JOBS.get(task_id)
    if not job:
        job = await _db_load(task_id)
        if not job:
            return {"error": f"unknown task_id: {task_id}"}
        JOBS[task_id] = job

    wait = min(max(int(wait_seconds), 0), 60)
    deadline = time.time() + wait

    while True:
        async with job.lock:
            current_len = len(job.output)
            new_chunk = bytes(job.output[since_byte:current_len])
            status = job.status

        is_done = status in TERMINAL_STATES
        has_new = current_len > since_byte
        now = time.time()

        if has_new or is_done or now >= deadline:
            return {
                "status": status,
                "output": new_chunk.decode(errors="replace"),
                "next_byte": current_len,
                "done": is_done,
                "return_code": job.return_code,
                "error": job.error,
                "started_at": job.started_at,
                "completed_at": job.completed_at,
            }

        try:
            await asyncio.wait_for(job.event.wait(), timeout=deadline - now)
        except asyncio.TimeoutError:
            pass


@mcp.tool
async def cancel(task_id: str) -> dict:
    """Kill a running job. No-op if the job is already terminal.

    Returns:
        {"status": "<current status>", "cancelled": <bool>}
    """
    job = JOBS.get(task_id)
    if not job:
        job = await _db_load(task_id)
        if not job:
            return {"error": f"unknown task_id: {task_id}"}
        JOBS[task_id] = job

    if job.status in TERMINAL_STATES:
        return {"status": job.status, "cancelled": False}

    if job.proc and job.proc.returncode is None:
        try:
            job.proc.kill()
        except ProcessLookupError:
            pass

    job.status = "cancelled"
    job.error = "cancelled by request"
    job.completed_at = time.time()
    job.event.set()
    return {"status": "cancelled", "cancelled": True}


@mcp.tool
async def list_tasks(limit: int = 20, include_running: bool = True) -> list[dict]:
    """Recent jobs, newest first. Pulls from the in-memory set and the SQLite archive."""
    limit = max(1, min(limit, 200))
    seen: set[str] = set()
    rows: list[dict] = []

    for j in sorted(JOBS.values(), key=lambda j: j.submitted_at, reverse=True):
        if not include_running and j.status in ACTIVE_STATES:
            continue
        rows.append(j.summary())
        seen.add(j.task_id)
        if len(rows) >= limit:
            return rows[:limit]

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT task_id, prompt, cwd, model, status, "
            "submitted_at, started_at, completed_at, return_code, "
            "LENGTH(output) AS output_bytes, error "
            "FROM tasks ORDER BY submitted_at DESC LIMIT ?",
            (limit,),
        ) as cur:
            async for row in cur:
                if row["task_id"] in seen:
                    continue
                rows.append({
                    "task_id": row["task_id"],
                    "status": row["status"],
                    "submitted_at": row["submitted_at"],
                    "started_at": row["started_at"],
                    "completed_at": row["completed_at"],
                    "return_code": row["return_code"],
                    "prompt_preview": (row["prompt"] or "")[:120],
                    "cwd": row["cwd"],
                    "model": row["model"],
                    "output_bytes": row["output_bytes"] or 0,
                    "error": row["error"],
                })
                if len(rows) >= limit:
                    break

    return rows[:limit]


@mcp.tool
async def delegate(
    prompt: str,
    cwd: str | None = None,
    timeout_seconds: int = 600,
    model: str | None = None,
) -> str:
    """Fire-and-await convenience: submit a job and block until it finishes.

    Equivalent to calling `submit` then `poll(wait_seconds=...)` until done.
    Prefer `submit` + `poll` for long jobs — if your caller hangs up here,
    the underlying claude subprocess is killed (the cancel bug is fixed).

    Returns:
        The job's stdout (or an error string prefixed with 'ERROR:').
    """
    timeout = min(max(int(timeout_seconds), 10), MAX_TIMEOUT)
    work_dir = cwd or DEFAULT_CWD
    job = Job(
        task_id=str(uuid.uuid4()),
        prompt=prompt,
        cwd=work_dir,
        model=model,
        timeout=timeout,
        submitted_at=time.time(),
    )
    JOBS[job.task_id] = job
    runner = asyncio.create_task(_run_job(job))

    try:
        while job.status in ACTIVE_STATES:
            await job.event.wait()
        async with job.lock:
            out = bytes(job.output).decode(errors="replace").rstrip()
        if job.status == "completed":
            return out
        return f"ERROR: status={job.status} rc={job.return_code} err={job.error}\n--- output ---\n{out}"
    except asyncio.CancelledError:
        if not runner.done():
            if job.proc and job.proc.returncode is None:
                try:
                    job.proc.kill()
                except ProcessLookupError:
                    pass
            job.status = "cancelled"
            job.error = "caller hung up"
            job.event.set()
        raise


@mcp.tool
def info() -> dict:
    """Return basic info about this delegation endpoint."""
    return {
        "hostname": os.uname().nodename,
        "user": os.environ.get("USER", "?"),
        "claude_bin": CLAUDE_BIN,
        "default_cwd": DEFAULT_CWD,
        "max_timeout_seconds": MAX_TIMEOUT,
        "db_path": str(Path(DB_PATH).resolve()),
        "active_jobs": sum(1 for j in JOBS.values() if j.status in ACTIVE_STATES),
    }


async def _startup() -> None:
    await _db_init()
    log.info(
        "claude-delegate starting on %s:%s (db=%s, claude=%s)",
        BIND_HOST, BIND_PORT, Path(DB_PATH).resolve(), CLAUDE_BIN,
    )


if __name__ == "__main__":
    asyncio.run(_startup())
    mcp.run(transport="http", host=BIND_HOST, port=BIND_PORT)
