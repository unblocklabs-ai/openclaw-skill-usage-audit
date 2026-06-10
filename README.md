![Skill Usage Audit banner](./docs/skill-usage-audit-banner.webp)

# Skill Usage Audit

`skill-usage-audit` is a native OpenClaw plugin that records skill/tool telemetry to SQLite and nudges agent, sub-agent, and cron turns toward relevant skills before prompt build.

The plugin is intentionally practical:

- local SQLite audit trail by default
- privacy-first message capture
- lightweight skill-router nudges instead of full skill injection
- decision traces for debugging why a router nudge did or did not fire
- evaluator scripts for skill health and nudge conversion

Lifecycle note: OpenClaw hook registration is per registry, not process-global. This plugin shares SQLite init/runtime state across the process, but binds hooks once for each `registrationMode=full` API instance.

## How It Works

The runtime flow is:

1. OpenClaw starts a session, tool call, message, or prompt-build lifecycle event.
2. The plugin records canonical telemetry in `~/.openclaw/audits/skill-usage.db`.
3. Skill reads are tracked as executions with surrounding context, version hashes, and implied outcomes.
4. On `before_prompt_build`, the router loads all known skill roots and scores the configured task window against skill descriptions.
5. If a skill clears the target threshold and was not recently suggested or read, the plugin injects a short `prependContext` nudge.
6. Nudges and compact router decision traces are written to SQLite so you can evaluate effectiveness later.

The router discovers skills from workspace roots, `.agents`, personal OpenClaw roots, bundled OpenClaw skills, configured extra directories, plugin skills, and Codex/OpenAI skill roots under `$CODEX_HOME` or `~/.codex`.

## Install

Prerequisites:

- Node.js `22` or newer
- Optional: `better-sqlite3`; when absent, Node's built-in `node:sqlite` backend is used

Remote install:

```bash
openclaw plugins install @unblocklabs/skill-usage-audit
openclaw plugins enable skill-usage-audit
```

Local iteration:

```bash
git clone https://github.com/unblocklabs-ai/openclaw-skill-usage-audit.git
cd openclaw-skill-usage-audit
npm install
cd ..
openclaw plugins install --link ./openclaw-skill-usage-audit
openclaw plugins enable skill-usage-audit
```

Then restart the gateway and verify:

```bash
openclaw plugins list --enabled
openclaw plugins inspect skill-usage-audit
```

## Config

The plugin is useful with defaults. In most cases, enable it and let the router/audit settings use their built-in values:

```json
{
  "plugins": {
    "entries": {
      "skill-usage-audit": {
        "enabled": true
      }
    }
  }
}
```

By default, the plugin writes to `~/.openclaw/audits/skill-usage.db`, does not store message text snapshots, scrubs PII, redacts common secret keys, and nudges regular agent, sub-agent, and cron turns.

Common config examples:

```json
{
  "dbPath": "~/.openclaw/audits/team-skill-usage.db",
  "captureMessageContent": false,
  "scrubPII": true,
  "router": {
    "enabled": true,
    "targets": {
      "agent": true,
      "subagent": true,
      "cron": true
    },
    "agentMinScore": 8,
    "minScore": 6,
    "observability": {
      "enabled": true,
      "retentionDays": 30
    }
  }
}
```

Tune noisy or critical routing:

```json
{
  "router": {
    "agentMinScore": 9,
    "overrides": [
      {
        "taskPattern": "(read|extract) (twitter|x)\\b",
        "skills": ["browser-read-x"]
      }
    ],
    "skillKeywords": {
      "github": ["gh", "pull request", "ci", "workflow"]
    },
    "blocklist": ["humanizer"]
  }
}
```

Advanced options:

| Option | Default | Use when |
| --- | --- | --- |
| `dbPath` | `~/.openclaw/audits/skill-usage.db` | You want audits written somewhere else. |
| `captureMessageContent` | `false` | You want stored execution context text, after redaction. |
| `scrubPII` | `true` | You want email, phone, and URL-query redaction. Secret/token patterns are always scrubbed. |
| `includeToolParams` | `false` | You need tool argument telemetry for local debugging. |
| `redactKeys` | common secret keys | Your tools use additional sensitive parameter names. |
| `skillBlockDetection` | `true` | You want prompts with existing skill blocks recorded as audit events. |
| `contextWindowSize` | `5` | You want more or fewer surrounding messages attached to skill executions. |
| `contextTimeoutMs` | `60000` | You want executions finalized sooner or later after the last related message. |
| `router.enabled` | `true` | You want to disable all skill nudges but keep audit telemetry. |
| `router.targets` | all enabled | You want nudges only for regular agents, sub-agents, or cron jobs. |
| `router.minScore` | `6` | You want stricter or looser sub-agent/cron recommendations. |
| `router.agentMinScore` | `8` | You want stricter or looser regular-agent recommendations. |
| `router.maxSkillsToNudge` | `1` | You want more than one suggested skill per turn. |
| `router.recencyWindow` | `10` | You want a longer or shorter message-based duplicate-suppression window. |
| `router.recencyFallbackMinutes` | `30` | Turn numbers are unavailable and you want a different time fallback. |
| `router.lookbackMessages` | `8` | Regular-agent routing should score more or fewer recent messages. |
| `router.taskWindowMode` | `recentMessages` | You want scoring based on only the latest user message or prompt. |
| `router.discovery` | all sources enabled | You need to disable a skill source or adjust grouping depth. |
| `router.observability.enabled` | `true` | You want compact decision traces explaining why the router did or did not nudge. |
| `router.observability.topCandidates` | `5` | You want more or fewer near-miss candidates stored per decision trace. |
| `router.observability.retentionDays` | `30` | You want decision traces cleaned up after a different number of whole days; `0` disables cleanup. |
| `router.observability.includeTaskExcerpt` | `false` | You want a redacted task excerpt in decision traces for local debugging. |
| `router.overrides` | `[]` | Some workflows should always route to specific skills. |
| `router.skillKeywords` | `{}` | A skill needs extra keyword boosts beyond its description. |
| `router.blocklist` | `[]` | A skill should never be suggested by the router. |

Target-specific overrides inherit global values when unset: `subagentMinScore` and `cronMinScore` inherit `minScore`; `agentMaxSkillsToNudge`, `subagentMaxSkillsToNudge`, and `cronMaxSkillsToNudge` inherit `maxSkillsToNudge`.

For best routing results, add YAML frontmatter to each `SKILL.md`:

```md
---
name: browser-read-x
description: Use when reading ANY X/Twitter URL: tweets, threads, articles, profiles.
---
```

## Runtime Behavior

The plugin includes guards to avoid noisy nudges:

- no router nudge when the prompt already contains skill blocks
- regular agent turns are held to a stricter default score than sub-agent and cron turns
- duplicate suggestions are suppressed by message history, in-memory session scope, and SQLite recency fallback
- disabled skills, agent allowlists, router blocklists, and bundled-skill allowlists are honored by canonical skill key/name
- duplicate display names are not treated as unique identities unless a canonical skill key is available

Decision tracing is compact by default. It stores skip/nudge reason, counts, selected skill keys, and bounded top candidates. It does not store raw task text unless `router.observability.includeTaskExcerpt` is enabled.

## Evaluators

Skill health:

```bash
node evaluate-skill-health.mjs --help
node evaluate-skill-health.mjs --db-path ~/.openclaw/audits/skill-usage.db --window-days 14
```

Nudge health:

```bash
node evaluate-nudge-health.mjs --help
node evaluate-nudge-health.mjs --db-path ~/.openclaw/audits/skill-usage.db --days 14
node evaluate-nudge-health.mjs --db-path ~/.openclaw/audits/skill-usage.db --days 14 --json
node evaluate-nudge-health.mjs --db-path ~/.openclaw/audits/skill-usage.db --days 7 --decisions
```

## Database Tables

- `skill_events` records raw lifecycle events.
- `skill_executions` aggregates skill-read executions with context and implied outcome.
- `skill_nudges` records emitted router nudges.
- `skill_router_decisions` records compact skip/nudge traces.
- `skills` stores per-skill rollups.
- `skill_versions` stores content-based version hashes.
- `skill_feedback` stores optional feedback labels.
- `skill_health_snapshots` stores evaluator snapshots.

## Development

Run focused tests:

```bash
npm test
```

Run the local preflight:

```bash
npm run preflight
```

The preflight runs tests, the OpenClaw entrypoint smoke check, install-shape checks, marketplace sync checks, package dry-run, evaluator help checks, and the optional plugin-inspector check. Set `OPENCLAW_CHECKOUT=/path/to/openclaw` to force compatibility checks against a specific checkout. The inspector step is skipped when `@openclaw/plugin-inspector` is not installed unless `CHECK_INSPECTOR_REQUIRE=1` is set.

## Repository Layout

- `index.ts` is the TypeScript source for the compiled `dist/index.js` runtime entrypoint.
- `skill-roots.mjs` discovers OpenClaw, Codex/OpenAI, workspace, and configured skill roots.
- `skill-router-helpers.mjs` contains shared skill identity, routing config, and text-window helpers.
- `evaluate-skill-health.mjs` computes skill health metrics from SQLite.
- `evaluate-nudge-health.mjs` measures router nudge conversion and decision reasons.
- `scripts/` contains package, marketplace, inspector, and release checks.
- `marketplace/skill-usage-audit/` is the installable marketplace package mirror.

Maintainer release notes live in [`docs/RELEASE.md`](./docs/RELEASE.md).

## License

MIT
