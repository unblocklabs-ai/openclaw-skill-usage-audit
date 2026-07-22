# Changelog

All notable changes to this plugin are documented here.

## Unreleased

- Added forward-looking per-nudge funnel classification for opened, ignored, failed-open, and unknown outcomes.
- Added idempotent nudge event recording, exact/symlink-aware structured and shell path detection, post-open resource tracking, and `agent_end` finalization.
- Added additive schema v2 migration without historical backfill, a discovered-skill catalog, and weekly portfolio reporting queries.
- Documented hook coverage, classification limits, and funnel privacy behavior; new nudge task excerpts now follow the existing opt-in observability setting.
- Added Knip dead-code and unused-export analysis to the repository preflight used by CI and local reviews.

## 1.1.4

- Added regular-agent skill router nudges.
- Expanded skill discovery across OpenClaw, `.agents`, plugin, Codex/OpenAI, workspace, bundled, and configured roots.
- Added canonical skill identity handling and duplicate-name safeguards.
- Added router observability decision traces and nudge evaluator decision reporting.
- Added entrypoint smoke checks, package/install checks, marketplace mirror scaffolding, and release documentation.
