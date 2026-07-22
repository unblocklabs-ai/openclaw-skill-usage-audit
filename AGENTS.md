# AGENTS.md

Work from the repository root. Keep changes small, preserve generated/package mirrors, and do not revert user changes.

## Layout

- Runtime entrypoint is `index.ts`.
- Shared helper modules live at the repo root as `.mjs` files.
- Evaluator CLIs live at the repo root.
- Tests live in `test/`.
- The installable marketplace package lives in `marketplace/skill-usage-audit/` and is synced from the root package.
- Release process docs live in `docs/RELEASE.md`; release notes live in `CHANGELOG.md` and GitHub releases.

## Validation

- Use Node 24 for local validation and CI.
- Run `npm test` for focused behavior checks.
- Run `npm run preflight` before release or broad plugin/package changes.
- `npm run preflight` is expected to run Knip, tests, entrypoint smoke checks, install-shape checks, marketplace sync checks, package dry-run, evaluator help checks, and inspector checks when available.

## Marketplace Mirror

- Run `npm run marketplace:sync` after README, manifest, package metadata, runtime file, evaluator, or helper changes.
- Run `npm run marketplace:check` when reviewing whether the installable subdirectory is in sync.
- Do not edit `marketplace/skill-usage-audit/` manually unless the sync script cannot represent the intended structure.

## Release

- npm package: `@unblocklabs/skill-usage-audit`.
- Current release flow is local: `npm run release -- patch|minor|major`.
- Release requires npm publish access and GitHub release access when publishing is enabled.
- If npm trusted publishing is configured later for `unblocklabs-ai/openclaw-skill-usage-audit`, move npm publishing into GitHub Actions and stop publishing from local machines.
- After release, verify npm, GitHub tag, GitHub release, marketplace mirror, and a clean working tree.
