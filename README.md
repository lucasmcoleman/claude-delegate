# claude-delegate

A tiny MCP server that lets one [Claude Code](https://github.com/anthropics/claude-code) instance hand a task off to another Claude Code on a different machine — and get a real answer back, synchronously, in a single tool call.

Run it on machine B. Configure machine A's Claude Code to point at it. Now A's Claude has a `delegate` tool: it sends a prompt, B's Claude runs headless, the result comes back. No polling. No message-passing daemon. No always-on bridge.

## Why this exists

Existing "AI ↔ AI" bridges treat Claude Code as if it could be interrupted by an incoming message. It can't. The receiving instance has to be sitting in a poll loop, burning tokens, just to notice mail arrived.

`claude-delegate` skips the mailbox entirely. The MCP tool itself shells out to `claude -p` on the target machine, captures the output, and returns it. Same interaction pattern Claude already uses for any other tool — just running on a different host.

```
machine A (Claude Code, your dev box)
   └─ MCP tool call: delegate(prompt, cwd)
        └─ HTTP/MCP over LAN/VPN  ────────►  machine B (this server)
                                                  └─ subprocess: claude -p
                                                       └─ stdout returned
```

## What it exposes

Eight MCP tools:

- **`submit(prompt, cwd?, timeout_seconds?, model?, conversation_id?) -> {task_id, status, conversation_id, session_id, resumed}`** — start a job. Returns immediately. **Use this for anything that might take >30s.** Pair with `poll`.
- **`poll(task_id, since_byte?, wait_seconds?) -> {status, output, next_byte, done, ...}`** — check on a job. Returns new output since `since_byte`. Set `wait_seconds > 0` for long-polling (efficient — the call blocks server-side until there's new output or the job ends, up to 60s).
- **`cancel(task_id) -> {status, cancelled}`** — kill a running job.
- **`list_tasks(limit?, include_running?) -> [task summary, ...]`** — recent jobs, newest first. Pulls from both the in-memory live set and the SQLite archive.
- **`list_conversations(limit?) -> [{conversation_id, session_id, cwd, turns, ...}]`** — known conversations, most recently used first.
- **`forget_conversation(conversation_id) -> {ok, forgotten}`** — drop a conversation mapping. The underlying Claude session file on disk is left alone.
- **`delegate(prompt, cwd?, timeout_seconds?, model?, conversation_id?) -> str`** — fire-and-await convenience: equivalent to `submit` then block until done. If the caller hangs up, the underlying `claude` subprocess is killed.
- **`info() -> dict`** — hostname, user, claude binary, default cwd, max timeout, active job count, db path. Useful for confirming the connection.

### Conversations (multi-turn delegation)

Pass `conversation_id` (any short memorable string — `"auth-debug"`, `"frontend-refactor"`, whatever) to keep context across delegated calls. The first call creates a fresh Claude session pinned to that id; subsequent calls with the same id `--resume` the same session, so the remote Claude remembers earlier turns.

```
turn 1: delegate(prompt="please remember 8675309", conversation_id="demo")     -> "OK"
turn 2: delegate(prompt="what number did I tell you?", conversation_id="demo") -> "8675309"
```

Constraints:

- **`cwd` is pinned at first call.** Claude Code sessions live under a cwd-derived project dir on disk, so switching cwd mid-conversation would lose all context. Use a new `conversation_id` to switch projects.
- **One active job per conversation.** Submitting a second job to a conversation with one still running returns an error — sessions can't be written concurrently.
- **`forget_conversation` only drops the mapping**, not the underlying session file. If you call `submit` with the same `conversation_id` afterwards, you get a fresh session.

### When to use which

| You want to... | Tool |
|---|---|
| Quickly ask the other Claude something | `delegate` |
| Kick off a long task and check back later | `submit` → `poll` |
| Stream output as the remote Claude works | `submit` → `poll(wait_seconds=30)` in a loop |
| Carry multi-turn context across calls | pass `conversation_id` to `submit` / `delegate` |
| See what's currently running | `list_tasks` or `info` |
| See what conversations exist | `list_conversations` |
| Stop a hung job | `cancel` |

## Streaming output

Jobs run with `claude -p --output-format stream-json --include-partial-messages`. The server parses each JSONL event and writes a readable transcript into the job's output buffer as Claude works — assistant text streams in as it's generated, with `[tool: Bash]` (etc.) signposts marking each tool call. `poll(wait_seconds=...)` returns new chunks as they arrive instead of waiting until the whole run is done.

Hook events, init banners, message envelopes, rate-limit pings, and the redundant `result.result` payload are dropped from the visible output. Use `journalctl -u claude-delegate` if you need to see raw events for debugging.

## Task state

Live job state is in memory. On terminal status (completed / failed / cancelled / timeout) the job is archived to `tasks.db` (SQLite, configurable via `DELEGATE_DB`). `poll` and `cancel` will load by task id from the archive if the service has been restarted since the job ran. Conversation mappings (id → session uuid + cwd) live in the same DB and survive restarts.

Inspect history directly with sqlite3:

```bash
sqlite3 tasks.db 'SELECT task_id, status, return_code, length(output), conversation_id, substr(prompt,1,60) FROM tasks ORDER BY submitted_at DESC LIMIT 20'
sqlite3 tasks.db 'SELECT conversation_id, session_id, cwd, turns, datetime(last_used_at,"unixepoch") FROM conversations ORDER BY last_used_at DESC'
```

## Requirements

- Python 3.10+ (3.12 tested)
- `claude` CLI installed and authenticated on the host
- `uv` (optional, faster venv setup) or stock `python -m venv`
- Linux with systemd (the bundled service unit assumes that — the Python is portable)

## Quickstart

```bash
git clone https://github.com/<you>/claude-delegate.git /opt/claude-delegate
cd /opt/claude-delegate

# 1. venv + deps
uv venv --python 3.12 .venv
uv pip install --python .venv/bin/python -r requirements.txt

# 2. generate a token and write .env
cp .env.example .env
sed -i "s|REPLACE_ME|$(openssl rand -base64 36 | tr -d '=+/' | head -c 48)|" .env
chmod 600 .env

# 3. install systemd unit (edit the paths if you didn't clone to /opt/claude-delegate)
sudo cp claude-delegate.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now claude-delegate

# 4. allow LAN traffic to the port
sudo ufw allow from 192.168.0.0/24 to any port 4115 proto tcp

# 5. smoke test
DELEGATE_TOKEN=$(grep ^DELEGATE_TOKEN= .env | cut -d= -f2) \
  .venv/bin/python test_client.py
```

You should see `PONG` come back from the remote Claude.

## Wiring up the caller

On the machine that should *call* `delegate`, add an MCP server entry to Claude Code:

```bash
claude mcp add --transport http delegate-server \
  http://<server-lan-ip>:4115/mcp \
  --header "Authorization: Bearer <your-token>"
```

Or edit `~/.claude.json` directly:

```json
{
  "mcpServers": {
    "delegate-server": {
      "type": "http",
      "url": "http://192.168.0.29:4115/mcp",
      "headers": {
        "Authorization": "Bearer YOUR_TOKEN"
      }
    }
  }
}
```

`delegate` and `info` will appear in the tool list.

## Configuration

All via `.env` (loaded by the systemd unit). Defaults shown:

| Variable | Default | Purpose |
|---|---|---|
| `DELEGATE_TOKEN` | *(required)* | Shared-secret bearer token. Generate with `openssl rand -base64 36`. |
| `DELEGATE_HOST` | `0.0.0.0` | Bind address. Set to a specific interface IP if you only want VPN access. |
| `DELEGATE_PORT` | `4115` | TCP port to listen on. |
| `DELEGATE_DEFAULT_CWD` | `$HOME` | Working directory when caller doesn't specify one. |
| `DELEGATE_MAX_TIMEOUT` | `1800` | Hard cap (seconds) on any single `delegate` call. |
| `CLAUDE_BIN` | `/home/lucas/.local/bin/claude` | Path to the `claude` CLI. |

## Security model

Designed for **trusted networks** — LAN, Wireguard, Tailscale. Notes:

- **Auth:** single shared bearer token via `Authorization: Bearer …`. Generate something long and random.
- **Transport:** plain HTTP. Encrypt the wire with a VPN or front it with a reverse proxy (Caddy/SWAG/nginx) for TLS if exposing beyond the trusted LAN.
- **Trust:** the delegated Claude runs with `--permission-mode bypassPermissions`. It has every permission *you* have on that host (filesystem, git, docker, network). Treat anyone with the token as having shell access. Do **not** expose this to the public internet without a TLS-fronted auth layer.
- The `.env` file is `chmod 600` and gitignored; never commit it.

## Bidirectional delegation

The setup is symmetric. To delegate *both* directions, install on both machines (each with its own token) and add an MCP entry on each side pointing at the other.

## Logs / management

```bash
journalctl -u claude-delegate -f       # tail logs
sudo systemctl restart claude-delegate # after editing server.py or .env
sudo systemctl status claude-delegate
```

## License

[MIT](LICENSE)
