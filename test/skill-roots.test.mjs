import test from "node:test";
import assert from "node:assert/strict";
import { mkdir, mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import { buildSkillRootSpecs, collectCodexPluginSkillRoots, resolveBundledSkillsRoot, resolveOpenClawConfigPath } from "../skill-roots.mjs";

test("buildSkillRootSpecs includes configured extra dirs and Codex plugin skill roots", async () => {
  const root = await mkdtemp(join(tmpdir(), "skill-roots-"));
  const home = resolve(root, "home");
  const workspace = resolve(root, "workspace");
  const codexHome = resolve(root, "codex");
  const extra = resolve(root, "extra-skills");

  await mkdir(resolve(home, ".openclaw"), { recursive: true });
  await mkdir(resolve(codexHome, "plugins", "cache", "openai-curated", "github", "v1", "skills"), { recursive: true });
  await writeFile(
    resolve(home, ".openclaw", "openclaw.json"),
    JSON.stringify({ skills: { load: { extraDirs: [extra] } } }),
  );

  const previousHome = process.env.HOME;
  const previousCodexHome = process.env.CODEX_HOME;
  const previousConfig = process.env.OPENCLAW_CONFIG_PATH;
  const previousState = process.env.OPENCLAW_STATE_DIR;
  process.env.HOME = home;
  process.env.CODEX_HOME = codexHome;
  delete process.env.OPENCLAW_CONFIG_PATH;
  delete process.env.OPENCLAW_STATE_DIR;

  try {
    assert.equal(resolveOpenClawConfigPath(), resolve(home, ".openclaw", "openclaw.json"));

    const specs = await buildSkillRootSpecs({ workspaceDir: workspace });
    assert.deepEqual(
      specs.map((spec) => [spec.source, spec.root]),
      [
        ["openclaw-workspace", resolve(workspace, "skills")],
        ["agents-skills-project", resolve(workspace, ".agents", "skills")],
        ["agents-skills-personal", resolve(home, ".agents", "skills")],
        ["openclaw-managed", resolve(home, ".openclaw", "skills")],
        ["openclaw-extra", extra],
        ["openclaw-extra", resolve(home, ".openclaw", "plugin-skills")],
        ["codex", resolve(codexHome, "skills")],
        ["codex-plugin", resolve(codexHome, "plugins", "cache", "openai-curated", "github", "v1", "skills")],
      ],
    );

    assert.deepEqual(await collectCodexPluginSkillRoots(codexHome), [
      resolve(codexHome, "plugins", "cache", "openai-curated", "github", "v1", "skills"),
    ]);
  } finally {
    if (previousHome === undefined) delete process.env.HOME;
    else process.env.HOME = previousHome;
    if (previousCodexHome === undefined) delete process.env.CODEX_HOME;
    else process.env.CODEX_HOME = previousCodexHome;
    if (previousConfig === undefined) delete process.env.OPENCLAW_CONFIG_PATH;
    else process.env.OPENCLAW_CONFIG_PATH = previousConfig;
    if (previousState === undefined) delete process.env.OPENCLAW_STATE_DIR;
    else process.env.OPENCLAW_STATE_DIR = previousState;
  }
});

test("buildSkillRootSpecs requires an explicit workspaceDir", async () => {
  await assert.rejects(() => buildSkillRootSpecs(), /requires workspaceDir/);
});

test("buildSkillRootSpecs includes an explicit bundled root", async () => {
  const root = await mkdtemp(join(tmpdir(), "skill-roots-bundled-"));
  const home = resolve(root, "home");
  const workspace = resolve(root, "workspace");
  const bundledRoot = resolve(root, "openclaw", "skills");

  await mkdir(resolve(home, ".openclaw"), { recursive: true });
  await writeFile(resolve(home, ".openclaw", "openclaw.json"), "{}");

  const previousHome = process.env.HOME;
  const previousCodexHome = process.env.CODEX_HOME;
  process.env.HOME = home;
  process.env.CODEX_HOME = resolve(root, "codex");

  try {
    const specs = await buildSkillRootSpecs({ workspaceDir: workspace, bundledRoot, includeBundled: true });
    assert.equal(specs.some((spec) => spec.source === "openclaw-bundled" && spec.root === bundledRoot), true);
  } finally {
    if (previousHome === undefined) delete process.env.HOME;
    else process.env.HOME = previousHome;
    if (previousCodexHome === undefined) delete process.env.CODEX_HOME;
    else process.env.CODEX_HOME = previousCodexHome;
  }
});

test("resolveBundledSkillsRoot honors OPENCLAW_BUNDLED_SKILLS_DIR", () => {
  const previous = process.env.OPENCLAW_BUNDLED_SKILLS_DIR;
  process.env.OPENCLAW_BUNDLED_SKILLS_DIR = "~/openclaw-bundled-skills";
  try {
    assert.equal(resolveBundledSkillsRoot(), resolve(process.env.HOME || process.env.USERPROFILE || "", "openclaw-bundled-skills"));
  } finally {
    if (previous === undefined) delete process.env.OPENCLAW_BUNDLED_SKILLS_DIR;
    else process.env.OPENCLAW_BUNDLED_SKILLS_DIR = previous;
  }
});

test("buildSkillRootSpecs honors discovery source toggles", async () => {
  const root = await mkdtemp(join(tmpdir(), "skill-roots-discovery-"));
  const home = resolve(root, "home");
  const workspace = resolve(root, "workspace");

  await mkdir(resolve(home, ".openclaw"), { recursive: true });
  await writeFile(resolve(home, ".openclaw", "openclaw.json"), JSON.stringify({ skills: { load: { extraDirs: [resolve(root, "extra")] } } }));

  const previousHome = process.env.HOME;
  const previousCodexHome = process.env.CODEX_HOME;
  process.env.HOME = home;
  process.env.CODEX_HOME = resolve(root, "codex");

  try {
    const specs = await buildSkillRootSpecs({
      workspaceDir: workspace,
      discovery: {
        agentsPersonal: false,
        openclawBundled: false,
        extraDirs: false,
        codex: false,
        codexPlugin: false,
      },
      bundledRoot: resolve(root, "bundled"),
      includeBundled: true,
    });
    const sources = specs.map((spec) => spec.source);
    assert.equal(sources.includes("agents-skills-personal"), false);
    assert.equal(sources.includes("openclaw-bundled"), false);
    assert.equal(sources.includes("codex"), false);
    assert.equal(sources.includes("codex-plugin"), false);
    assert.equal(specs.some((spec) => spec.root === resolve(root, "extra")), false);
  } finally {
    if (previousHome === undefined) delete process.env.HOME;
    else process.env.HOME = previousHome;
    if (previousCodexHome === undefined) delete process.env.CODEX_HOME;
    else process.env.CODEX_HOME = previousCodexHome;
  }
});
