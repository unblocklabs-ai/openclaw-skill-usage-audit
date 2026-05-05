# Release

This plugin is released from one repo version. The release script keeps the GitHub tag/release, npm package, root plugin manifest, and OpenClaw marketplace install mirror on the same version.

## Marketplace Mirror

For marketplace installs, OpenClaw resolves the plugin from this repo's marketplace manifest. A deployable release should keep these files in sync:

- `package.json`
- `openclaw.plugin.json`
- `.claude-plugin/marketplace.json`
- `marketplace/skill-usage-audit/`

`marketplace/skill-usage-audit/` is the installable marketplace package mirror. It contains only runtime install files so OpenClaw's marketplace security scanner does not scan development and release scripts from the repository root.

Refresh it with:

```bash
npm run marketplace:sync
```

## Release Script

Run releases from the repo root:

```bash
npm run release -- patch
```

You can also use:

```bash
npm run release:patch
npm run release:minor
npm run release:major
```

What it does:

- bumps the version in release metadata files
- runs `npm run preflight`
- refreshes and stages the marketplace package mirror
- stages only release metadata and marketplace files
- commits with `release: vX.Y.Z`
- creates an annotated `vX.Y.Z` git tag
- pushes the current `main` branch and tag to `origin`
- publishes `@unblocklabs/skill-usage-audit@X.Y.Z` to npm when `openclaw.release.publishToNpm` is enabled
- creates a GitHub release for `vX.Y.Z`

Useful flags:

```bash
npm run release -- 1.1.5 --dry-run
npm run release -- patch --message "release: v1.1.5 router observability"
npm run release -- patch --no-npm
npm run release -- patch --no-github-release
```

After pushing, OpenClaw installs can pick up the new repo state and existing installs can update with:

```bash
openclaw plugins update skill-usage-audit
```
