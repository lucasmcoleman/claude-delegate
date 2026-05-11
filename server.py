"""Claude-delegate MCP server.

Lets a Claude Code instance on one machine hand work off to Claude Code on
this one, with named conversations for multi-turn delegation.

Tools:
- submit(...)        -> {task_id, status, conversation_id?, session_id?}
                                            start a job, returns immediately
- poll(task_id, ...) -> {status, output, next_byte, done, ...}
                                            check on a running/done job
- cancel(task_id)    -> {status}            kill a running job
- list_tasks(limit)  -> [task summary, ...] recent jobs
- list_conversations(limit) -> [conv, ...]  known conversations
- forget_conversation(id) -> {ok}           drop a conversation mapping
- delegate(...)      -> str                 fire-and-await convenience
- info()             -> dict                hostname / config snapshot
"""

from __future__ import annotations

import asyncio
import json
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
    conversation_id: str | None = None
    session_id: str | None = None
    resume_existing: bool = False
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
            "conversation_id": self.conversation_id,
            "session_id": self.session_id,
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
    error TEXT,
    conversation_id TEXT,
    session_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_tasks_submitted_at ON tasks(submitted_at DESC);

CREATE TABLE IF NOT EXISTS conversations (
    conversation_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    cwd TEXT NOT NULL,
    created_at REAL NOT NULL,
    last_used_at REAL NOT NULL,
    turns INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_conversations_last_used ON conversations(last_used_at DESC);
"""


async def _db_init() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA)
        # Idempotent migration for older tasks.db that pre-dated conversations.
        async with db.execute("PRAGMA table_info(tasks)") as cur:
            existing_cols = {row[1] async for row in cur}
        if "conversation_id" not in existing_cols:
            await db.execute("ALTER TABLE tasks ADD COLUMN conversation_id TEXT")
        if "session_id" not in existing_cols:
            await db.execute("ALTER TABLE tasks ADD COLUMN session_id TEXT")
        await db.commit()


async def _db_archive(job: Job) -> None:
    async with _db_lock, aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT OR REPLACE INTO tasks
            (task_id, prompt, cwd, model, timeout_seconds,
             submitted_at, started_at, completed_at,
             status, return_code, output, error,
             conversation_id, session_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job.task_id, job.prompt, job.cwd, job.model, job.timeout,
                job.submitted_at, job.started_at, job.completed_at,
                job.status, job.return_code, bytes(job.output), job.error,
                job.conversation_id, job.session_id,
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
        conversation_id=row["conversation_id"],
        session_id=row["session_id"],
    )
    if row["output"]:
        job.output.extend(row["output"])
    job.event.set()
    return job


async def _db_get_conversation(conversation_id: str) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM conversations WHERE conversation_id = ?",
            (conversation_id,),
        ) as cur:
            row = await cur.fetchone()
    return dict(row) if row else None


async def _db_create_conversation(
    conversation_id: str, session_id: str, cwd: str
) -> None:
    now = time.time()
    async with _db_lock, aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO conversations "
            "(conversation_id, session_id, cwd, created_at, last_used_at, turns) "
            "VALUES (?, ?, ?, ?, ?, 0)",
            (conversation_id, session_id, cwd, now, now),
        )
        await db.commit()


async def _db_touch_conversation(conversation_id: str) -> None:
    async with _db_lock, aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE conversations "
            "SET last_used_at = ?, turns = turns + 1 "
            "WHERE conversation_id = ?",
            (time.time(), conversation_id),
        )
        await db.commit()


async def _db_forget_conversation(conversation_id: str) -> bool:
    async with _db_lock, aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "DELETE FROM conversations WHERE conversation_id = ?",
            (conversation_id,),
        )
        await db.commit()
        return cur.rowcount > 0


def _build_argv(job: Job) -> list[str]:
    argv = [
        CLAUDE_BIN,
        "-p",
        "--permission-mode", "bypassPermissions",
        "--output-format", "stream-json",
        "--include-partial-messages",
        "--verbose",
    ]
    if job.model:
        argv += ["--model", job.model]
    if job.session_id:
        if job.resume_existing:
            argv += ["--resume", job.session_id]
        else:
            argv += ["--session-id", job.session_id]
    argv.append(job.prompt)
    return argv


def _format_event(event: dict) -> str:
    """Convert one stream-json event to human-readable text for the output buffer.

    We surface:
    - assistant text deltas (the live stream of what claude is saying)
    - tool_use starts ([tool: Bash], etc.) as signposts
    - turn boundaries (blank line between agent turns)
    - terminal errors

    We drop hook spam, init banners, message envelopes, rate-limit pings, and
    the redundant `result.result` (which duplicates the streamed text).
    """
    etype = event.get("type")

    if etype == "stream_event":
        inner = event.get("event") or {}
        itype = inner.get("type")
        if itype == "content_block_delta":
            delta = inner.get("delta") or {}
            if delta.get("type") == "text_delta":
                return delta.get("text", "")
        elif itype == "content_block_start":
            block = inner.get("content_block") or {}
            if block.get("type") == "tool_use":
                name = block.get("name", "?")
                return f"\n[tool: {name}]\n"
        elif itype == "message_stop":
            return "\n"
        return ""

    if etype == "result" and event.get("is_error"):
        msg = event.get("result") or event.get("error") or "unknown error"
        return f"\n[error: {msg}]\n"

    return ""


def _active_job_for_conversation(conversation_id: str) -> Job | None:
    for j in JOBS.values():
        if j.conversation_id == conversation_id and j.status in ACTIVE_STATES:
            return j
    return None


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
            "job %s start: cwd=%s model=%s timeout=%ss conv=%s session=%s%s",
            job.task_id, job.cwd, job.model or "<default>", job.timeout,
            job.conversation_id or "-", job.session_id or "-",
            " (resume)" if job.resume_existing else "",
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
            stdout = job.proc.stdout
            # claude emits JSON Lines; some can be very large (hook events with
            # full skill text embedded), so raise the line limit well above the
            # asyncio default (~64 KiB).
            stdout._limit = max(getattr(stdout, "_limit", 0), 4 * 1024 * 1024)
            while True:
                try:
                    line = await stdout.readline()
                except asyncio.LimitOverrunError:
                    # Skip the offending oversized line so we don't deadlock.
                    log.warning("job %s: dropped oversized JSON line", job.task_id)
                    continue
                if not line:
                    return
                try:
                    event = json.loads(line)
                    text = _format_event(event)
                except json.JSONDecodeError:
                    # Unexpected non-JSON output (e.g. a crash). Surface it raw.
                    text = line.decode(errors="replace")
                if not text:
                    continue
                async with job.lock:
                    job.output.extend(text.encode())
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
        if job.conversation_id and job.status == "completed":
            try:
                await _db_touch_conversation(job.conversation_id)
            except Exception:
                log.exception("failed to touch conversation %s", job.conversation_id)
        log.info(
            "job %s end: status=%s rc=%s bytes=%d",
            job.task_id, job.status, job.return_code, len(job.output),
        )


async def _prepare_job(
    prompt: str,
    cwd: str | None,
    timeout_seconds: int,
    model: str | None,
    conversation_id: str | None,
) -> Job | dict:
    """Build a Job ready to run, or return {"error": ...} if input is bad.

    Resolves conversation_id to a session_id and pinned cwd, creating a new
    conversation row on first use.
    """
    timeout = min(max(int(timeout_seconds), 10), MAX_TIMEOUT)
    work_dir = cwd or DEFAULT_CWD
    session_id: str | None = None
    resume_existing = False

    if conversation_id:
        active = _active_job_for_conversation(conversation_id)
        if active:
            return {
                "error": (
                    f"conversation {conversation_id!r} already has an active "
                    f"task ({active.task_id}); wait for it to finish, cancel "
                    f"it, or use a different conversation_id"
                )
            }

        conv = await _db_get_conversation(conversation_id)
        if conv:
            session_id = conv["session_id"]
            pinned_cwd = conv["cwd"]
            if cwd and str(Path(cwd).resolve()) != str(Path(pinned_cwd).resolve()):
                return {
                    "error": (
                        f"conversation {conversation_id!r} is pinned to "
                        f"cwd={pinned_cwd!r}; got cwd={cwd!r}. To switch "
                        f"projects, use a new conversation_id."
                    )
                }
            work_dir = pinned_cwd
            resume_existing = True
        else:
            session_id = str(uuid.uuid4())
            await _db_create_conversation(conversation_id, session_id, work_dir)

    return Job(
        task_id=str(uuid.uuid4()),
        prompt=prompt,
        cwd=work_dir,
        model=model,
        timeout=timeout,
        submitted_at=time.time(),
        conversation_id=conversation_id,
        session_id=session_id,
        resume_existing=resume_existing,
    )


verifier = StaticTokenVerifier(
    tokens={TOKEN: {"client_id": "lan-peer", "scopes": ["delegate"]}},
    required_scopes=["delegate"],
)

mcp = FastMCP(
    name="claude-delegate",
    instructions=(
        "Hand a task to Claude Code on a different machine. "
        "Prefer `submit` + `poll` for anything that might take >30s. "
        "Pass a `conversation_id` (any short memorable string) to keep "
        "multi-turn context across calls — the same conversation_id "
        "resumes the remote Claude session each time. `cancel` aborts a "
        "running job; `list_conversations` shows what's been established."
    ),
    auth=verifier,
)


@mcp.tool
async def submit(
    prompt: str,
    cwd: str | None = None,
    timeout_seconds: int = 600,
    model: str | None = None,
    conversation_id: str | None = None,
) -> dict:
    """Start a Claude Code job on this machine. Returns immediately.

    Args:
        prompt: Task for the remote Claude. Be specific; without a
            conversation_id it has no memory of prior calls.
        cwd: Working directory for the remote Claude. Required for the FIRST
            call of a conversation; pinned and reused for later calls.
        timeout_seconds: Hard cap on the job's runtime. Capped server-side
            at DELEGATE_MAX_TIMEOUT.
        model: Optional model alias ('opus', 'sonnet', 'haiku') or full name.
        conversation_id: Any short memorable string (e.g. 'auth-debug').
            First call creates the conversation; later calls with the same
            id resume the remote Claude session so it remembers everything
            from prior turns. cwd is pinned at first use.

    Returns:
        {"task_id": "...", "status": "queued",
         "conversation_id": "...", "session_id": "...", "resumed": <bool>}
        Use `poll(task_id)` to get output and check completion.
    """
    prepared = await _prepare_job(
        prompt, cwd, timeout_seconds, model, conversation_id
    )
    if isinstance(prepared, dict):
        return prepared
    job = prepared
    JOBS[job.task_id] = job
    asyncio.create_task(_run_job(job))
    return {
        "task_id": job.task_id,
        "status": job.status,
        "conversation_id": job.conversation_id,
        "session_id": job.session_id,
        "resumed": job.resume_existing,
    }


@mcp.tool
async def poll(
    task_id: str,
    since_byte: int = 0,
    wait_seconds: int = 0,
) -> dict:
    """Check on a job. Returns new output since `since_byte` and current status.

    Use `wait_seconds > 0` to long-poll: the call blocks server-side up to
    that many seconds (capped at 60) waiting for new output or completion
    before returning. Much more efficient than tight-polling.

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
    """Kill a running job. No-op if the job is already terminal."""
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
    """Recent jobs, newest first. Pulls from the in-memory set and SQLite archive."""
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
            "LENGTH(output) AS output_bytes, error, "
            "conversation_id, session_id "
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
                    "conversation_id": row["conversation_id"],
                    "session_id": row["session_id"],
                    "output_bytes": row["output_bytes"] or 0,
                    "error": row["error"],
                })
                if len(rows) >= limit:
                    break

    return rows[:limit]


@mcp.tool
async def list_conversations(limit: int = 20) -> list[dict]:
    """Known conversations, most recently used first."""
    limit = max(1, min(limit, 200))
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT conversation_id, session_id, cwd, "
            "created_at, last_used_at, turns "
            "FROM conversations ORDER BY last_used_at DESC LIMIT ?",
            (limit,),
        ) as cur:
            return [dict(row) async for row in cur]


@mcp.tool
async def forget_conversation(conversation_id: str) -> dict:
    """Drop a conversation mapping.

    Note: this only removes the (conversation_id -> session_id) mapping from
    the local DB. The underlying Claude session file on disk is left alone;
    subsequent calls with the same conversation_id will create a brand-new
    session.
    """
    if _active_job_for_conversation(conversation_id):
        return {
            "ok": False,
            "error": f"conversation {conversation_id!r} has an active task; "
                     f"cancel it first",
        }
    dropped = await _db_forget_conversation(conversation_id)
    return {"ok": dropped, "forgotten": dropped}


@mcp.tool
async def delegate(
    prompt: str,
    cwd: str | None = None,
    timeout_seconds: int = 600,
    model: str | None = None,
    conversation_id: str | None = None,
) -> str:
    """Fire-and-await convenience: submit a job and block until it finishes.

    Equivalent to `submit` + `poll(wait_seconds=...)` until done. Prefer
    `submit` + `poll` for long jobs — but unlike before, if the caller
    hangs up here, the underlying claude subprocess IS killed.

    Pass `conversation_id` to keep multi-turn context across calls (see
    `submit` for details).

    Returns:
        The job's stdout (or an error string prefixed with 'ERROR:').
    """
    prepared = await _prepare_job(
        prompt, cwd, timeout_seconds, model, conversation_id
    )
    if isinstance(prepared, dict):
        return f"ERROR: {prepared.get('error', 'submit failed')}"
    job = prepared
    JOBS[job.task_id] = job
    runner = asyncio.create_task(_run_job(job))

    try:
        while job.status in ACTIVE_STATES:
            await job.event.wait()
        async with job.lock:
            out = bytes(job.output).decode(errors="replace").rstrip()
        if job.status == "completed":
            return out
        return (
            f"ERROR: status={job.status} rc={job.return_code} "
            f"err={job.error}\n--- output ---\n{out}"
        )
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
