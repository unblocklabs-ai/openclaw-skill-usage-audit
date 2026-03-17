# skill-usage-audit

## What it does

`skill-usage-audit` is an OpenClaw plugin that records tool calls and skill execution telemetry to SQLite. It tracks:

- skill reads (which skill files were read)
- tool call lifecycle (`before_tool_call`/`after_tool_call`)
- user/assistant messages around a skill execution
- inferred skill execution outcomes (`positive` / `negative` / `unclear`)
- skill versions and execution snapshots

The goal is to give you machine-readable data for health scoring and automated lifecycle decisions (stable/experimental/degraded/underused).

## Install

```bash
openclaw plugins install @unblocklabs/skill-usage-audit
```

Then enable/configure in OpenClaw config:

```json
{
  "plugins": {
    "entries": {
      "skill-usage-audit": {
        "enabled": true,
        "config": {
          "dbPath": "~/.openclaw/audits/skill-usage.db"
        }
      }
    }
  }
}
```

## Configuration (`configSchema`)

The plugin manifest defines these options:

- `dbPath` (string, default `~/.openclaw/audits/skill-usage.db`)
  - SQLite database file to write telemetry to.
- `includeToolParams` (boolean, default `false`)
  - Include tool parameters in `before_tool_call` / `after_tool_call` event rows.
- `captureMessageContent` (boolean, default `false`)
  - If true, stores sanitized snapshots of message text; if false, stores only metadata for privacy.
- `redactKeys` (array<string>, default `["token", "apikey", "api_key", "apiKey", "password", "passwd", "auth", "authorization", "secret", "secretToken", "refreshToken", "client_secret"]`)
  - Keys that are redacted from object payloads before storage.
- `skillBlockDetection` (boolean, default `true`)
  - Emit `skill_block_detected` events from `before_prompt_build` prompts.
- `contextWindowSize` (integer, default `5`, min `1`)
  - Number of messages retained before/after a skill read for execution context.
- `contextTimeoutMs` (integer, default `60000`, min `0`)
  - Milliseconds to wait for follow-up messages before finalizing a skill execution record.

## What gets stored

Data is written into SQLite tables from the plugin at `dbPath`:

- `skill_events`
- `skill_versions`
- `skills`
- `skill_executions`
- `skill_feedback`
- `skill_health_snapshots`

## Query examples

### Show recent skill executions

```bash
sqlite3 ~/.openclaw/audits/skill-usage.db \
  "SELECT skill_name, ts, mechanical_success, implied_outcome, error FROM skill_executions ORDER BY ts DESC LIMIT 20;"
```

### Count by status recommendation

```bash
sqlite3 ~/.openclaw/audits/skill-usage.db \
  "SELECT status, COUNT(*) FROM skills GROUP BY status;"
```

### Extract events and inspect JSON payloads with `jq`

```bash
sqlite3 -json ~/.openclaw/audits/skill-usage.db \
  "SELECT ts, type, skill_name, params FROM skill_events WHERE type='tool_call_end' LIMIT 50;" \
  | jq '.[].params'
```

### Show latest health snapshot per skill

```bash
sqlite3 ~/.openclaw/audits/skill-usage.db \
  "SELECT skill_name, usage_count, mechanical_failure_rate, implied_negative_rate, status_recommendation, created_at FROM skill_health_snapshots ORDER BY created_at DESC LIMIT 20;"
```

## Evaluator script

A bundled evaluator script can compute deterministic health signals and write recommendations back to the same DB:

```bash
node evaluate-skill-health.mjs --help
node evaluate-skill-health.mjs --db-path ~/.openclaw/audits/skill-usage.db --window-days 14
```

Supported options (highlights):

- `--db-path` / `--db` (or env `SKILL_USAGE_AUDIT_DB_PATH`)
- `--report-dir <path>`
- `--window-days <n>`
- `--stable-min-usage <n>`
- `--experimental-min-usage <n>`
- `--degraded-sample-min <n>`
- `--degraded-mechanical-rate <r>`
- `--degraded-implied-rate <r>`
- `--underused-max <n>`
- `--no-update-status`
- `--no-report`
- `--no-filesystem-scan`
- `--verbose`

The evaluator writes:

- `skills.status` updates (stable/experimental/degraded/unused)
- `skill_health_snapshots` rows for historical tracking
- Markdown reports under `reports/skill-health/`

## Requirements

- **Node.js >= 22** (for `node:sqlite` support)
- No external npm dependencies are required for the plugin at runtime.

## Notes

- This package is distributed as a scoped npm package and installs into OpenClaw as `~/.openclaw/extensions/skill-usage-audit/`.
- `openclaw.plugins install` uses `npm pack` under the hood and expects `index.ts` and `openclaw.plugin.json` to be included in package files.
