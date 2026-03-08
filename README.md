# ai-hist

Sync and search your [Claude Code](https://docs.anthropic.com/en/docs/claude-code) and [Codex CLI](https://github.com/openai/codex) conversation history into a local SQLite database with full-text search.

**Zero dependencies** — Python 3.8+ standard library only. Single file. ~200 lines.

## Install

```bash
curl -o ~/.local/bin/ai-hist https://raw.githubusercontent.com/khaliqgant/ai-hist/main/ai-hist
chmod +x ~/.local/bin/ai-hist
```

Or clone and symlink:

```bash
git clone https://github.com/khaliqgant/ai-hist.git
ln -s "$(pwd)/ai-hist/ai-hist" ~/.local/bin/ai-hist
```

Make sure `~/.local/bin` is in your `PATH`:

```bash
export PATH="$HOME/.local/bin:$PATH"  # add to .zshrc / .bashrc
```

## Usage

```bash
# Import all history (incremental — only reads new bytes on re-run)
ai-hist sync

# Full-text search
ai-hist search "authentication bug"
ai-hist search "refactor" --source claude --limit 10
ai-hist search "deploy" --project relay

# Recent prompts
ai-hist recent                             # last 20
ai-hist recent 50                          # last 50
ai-hist recent --source claude --project my-app

# Drill into a specific entry (shows full prompt + metadata)
ai-hist show 4521

# View all prompts in a session
ai-hist session abc-1234-def
ai-hist session abc-1234-def --full   # no truncation

# Stats overview
ai-hist stats
```

Search results include entry IDs (`#NNN`) you can pass to `show` or use to find the `session_id` for `session`.

Example output from `ai-hist stats`:

```
Total entries: 47,665

By source:
  claude: 37,406
  codex: 10,259

Date range:
  2025-10-05 to 2026-03-08

Top 10 projects:
   8,701  /Users/you/Projects/my-app
   4,586  /Users/you/Projects/api-server
   ...
```

## How it works

Both Claude Code and Codex CLI store conversation history as JSONL files:

| Source | File | Key fields |
|--------|------|------------|
| Claude Code | `~/.claude/history.jsonl` | `display`, `timestamp`, `project`, `sessionId` |
| Codex CLI | `~/.codex/history.jsonl` | `text`, `ts`, `session_id` |

`ai-hist sync` reads these files incrementally (tracking byte offsets in `.sync-state.json`) and inserts rows into a SQLite database with an [FTS5](https://www.sqlite.org/fts5.html) full-text search index.

Deduplication uses `INSERT OR IGNORE` on a `UNIQUE(source, timestamp_ms, prompt)` constraint.

## Database location

Default: `~/.local/share/ai-hist/ai-history.db`

Override with the `AI_HIST_DB` environment variable:

```bash
export AI_HIST_DB="$HOME/Dropbox/ai-history/ai-history.db"
```

## Continuous sync (macOS)

Create a launchd plist to sync every 60 seconds:

```bash
cat > ~/Library/LaunchAgents/com.ai-hist.sync.plist << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.ai-hist.sync</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/python3</string>
        <string>${HOME}/.local/bin/ai-hist</string>
        <string>sync</string>
    </array>
    <key>StartInterval</key>
    <integer>60</integer>
    <key>RunAtLoad</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/tmp/ai-hist-sync.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/ai-hist-sync.err</string>
</dict>
</plist>
EOF

launchctl load ~/Library/LaunchAgents/com.ai-hist.sync.plist
```

> Replace `/usr/bin/python3` with your Python path if needed (e.g., from `which python3`).

### Linux (cron)

```bash
# Sync every minute
echo "* * * * * python3 ~/.local/bin/ai-hist sync >> /tmp/ai-hist-sync.log 2>&1" | crontab -
```

### Alternative: watch mode

```bash
ai-hist watch              # syncs every 60s
ai-hist watch --interval 30  # syncs every 30s
```

## Schema

```sql
CREATE TABLE history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,          -- 'claude' or 'codex'
    session_id TEXT,
    project TEXT,
    prompt TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    UNIQUE(source, timestamp_ms, prompt)
);

-- FTS5 full-text search index
CREATE VIRTUAL TABLE history_fts USING fts5(prompt, project, content='history', content_rowid='id');
```

You can query the database directly with any SQLite client:

```bash
sqlite3 ~/.local/share/ai-hist/ai-history.db "SELECT COUNT(*) FROM history"
```

## License

MIT
