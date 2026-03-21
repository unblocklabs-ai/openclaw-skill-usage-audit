# @unblocklabs/skill-usage-audit

[![npm version](https://img.shields.io/npm/v/@unblocklabs/skill-usage-audit.svg)](https://www.npmjs.com/package/@unblocklabs/skill-usage-audit)
[![license](https://img.shields.io/npm/l/@unblocklabs/skill-usage-audit.svg)](./package.json)

OpenClaw plugin that tracks skill/tool usage telemetry **and** automatically nudges sub-agents + cron runs toward relevant skills.

---

## Features

### Skill Router (new in v1.0.0)

A lightweight routing layer that runs in `before_prompt_build` and **nudges** the model to read the most relevant skill(s).

- **Targets:** sub-agent sessions and cron-triggered runs (configurable)
- **Relevance scoring:** BM25-style scoring against skill *descriptions* (from `SKILL.md` frontmatter) + optional keyword boosts
- **Lightweight injection:** adds ~100 tokens via `prependContext` (a “read this skill” hint) — **not** full skill injection
- **Recency-aware dedup:** won’t re-nudge if the skill was recently suggested or already read (within a configurable message window)
- **Suppresses when skill blocks exist:** if the prompt already contains `<skill ...>` blocks, router does nothing
- **Manual overrides:** deterministic routing for critical workflows via `taskPattern → skills[]`
- **Closed-loop tracking:** every nudge is recorded in SQLite (`skill_nudges`) so you can measure nudge → read → use conversion

### Usage Audit (existing)

A telemetry pipeline that records skill and tool execution behavior to SQLite.

- Tracks:
  - tool call lifecycle (`before_tool_call` / `after_tool_call`)
  - skill file reads (infers skill name + source)
  - session lifecycle (`session_start` / `session_end`)
  - message context around skill execution (intent + follow-ups)
- **Implied outcome detection:** `positive` / `negative` / `unclear` inferred from tool success + follow-up signals
- **Skill version tracking:** content hashing of `SKILL.md` (+ optional `scripts/` folder) into `skill_versions`
- **Privacy-first:** configurable message capture; secret/PII redaction (emails/phones/tokens/URL querystrings)

---

## Installation

### npm (direct)

```bash
# In your OpenClaw extensions directory
cd ~/.openclaw/extensions
npm install @unblocklabs/skill-usage-audit
```

### OpenClaw CLI (equivalent)

```bash
openclaw plugins install @unblocklabs/skill-usage-audit
```

### Clone (dev)

Clone into:

```bash
~/.openclaw/extensions/skill-usage-audit/
```

---

## Requirements

- **Node.js >= 22** (the plugin can use `node:sqlite` as a fallback)
- Optional: `better-sqlite3` (if present, it will be preferred)

---

## Configuration

In your `openclaw.json`:

```json
{
  "plugins": {
    "entries": {
      "skill-usage-audit": {
        "enabled": true,
        "config": {
          "dbPath": "~/.openclaw/audits/skill-usage.db",
          "includeToolParams": false,
          "captureMessageContent": false,
          "redactKeys": [
            "token",
            "apikey",
            "api_key",
            "apiKey",
            "password",
            "passwd",
            "auth",
            "authorization",
            "secret",
            "secretToken",
            "refreshToken",
            "client_secret"
          ],
          "skillBlockDetection": true,
          "contextWindowSize": 5,
          "contextTimeoutMs": 60000,
          "router": {
            "enabled": true,
            "targets": {
              "subagent": true,
              "cron": true
            },
            "minScore": 6,
            "maxSkillsToNudge": 1,
            "recencyWindow": 10,
            "overrides": [
              {
                "taskPattern": "(read|extract) (twitter|x)\\b",
                "skills": ["browser-read-x"]
              },
              {
                "taskPattern": "\\bspawn\\b|sub-agent",
                "skills": ["sub-agents"]
              }
            ],
            "skillKeywords": {
              "video-understanding": ["loom", "tiktok", "vimeo", "youtube"],
              "github": ["gh", "pull request", "ci", "workflow"]
            },
            "blocklist": ["humanizer"]
          }
        }
      }
    }
  }
}
```

Notes:
- The router scores against **skill descriptions**. For best results, add YAML frontmatter to each `SKILL.md`:
  ```md
  ---
  name: browser-read-x
  description: Use when reading ANY X/Twitter URL — tweets, threads, articles, profiles.
  ---
  ```
- Defaults (from `openclaw.plugin.json`): router enabled, `minScore=6`, `maxSkillsToNudge=1`, `recencyWindow=10`, targets `{subagent:true, cron:true}`.

---

## How the Router Works

1. A **sub-agent** or **cron** session reaches `before_prompt_build`
2. The plugin loads known skills (workspace `skills/`, `~/.openclaw/skills`, and bundled OpenClaw skills)
3. It BM25-scores the task text against skill descriptions (plus optional keyword boosts)
4. If the best score exceeds `router.minScore`, it injects a short hint via `prependContext`
5. Nudges are logged to SQLite (`skill_nudges`) so you can evaluate effectiveness

---

## Evaluator scripts

### `evaluate-skill-health.mjs`

Computes per-skill health metrics from `skill_executions`, writes snapshots, updates `skills.status`, and emits a markdown report.

```bash
node evaluate-skill-health.mjs --help
node evaluate-skill-health.mjs --db-path ~/.openclaw/audits/skill-usage.db --window-days 14
```

Useful flags:
- `--db-path` / `--db` (or env `SKILL_USAGE_AUDIT_DB_PATH`)
- `--report-dir <path>`
- `--window-days <n>`
- `--no-update-status`
- `--no-report`

### `evaluate-nudge-health.mjs`

Measures router effectiveness (nudge → skill read → downstream usage).

```bash
node evaluate-nudge-health.mjs --help
node evaluate-nudge-health.mjs --db-path ~/.openclaw/audits/skill-usage.db --days 14
node evaluate-nudge-health.mjs --db-path ~/.openclaw/audits/skill-usage.db --days 14 --json
```

Flags:
- `--days N` (default 14)
- `--db-path <path>`
- `--json`

---

## Database schema (high level)

The plugin writes to these SQLite tables:

- `skill_events` — raw event stream (tool calls, session start/end, skill blocks)
- `skill_executions` — aggregated “skill execution” records (intent context + follow-ups + implied outcome)
- `skill_nudges` — router nudge events (score, match reason, task excerpt)
- `skills` — per-skill rollups (status, last used, total executions, current version)
- `skill_versions` — version hashes (content-based)
- `skill_feedback` — optional feedback labels on executions
- `skill_health_snapshots` — time series snapshots from the evaluator

---

## License

MIT
