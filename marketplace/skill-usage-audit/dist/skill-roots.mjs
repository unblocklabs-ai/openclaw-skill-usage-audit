import { readFileSync } from "node:fs";
import { readdir } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";

const require = createRequire(import.meta.url);

export function resolveHomePath(pathLike) {
  if (!pathLike || typeof pathLike !== "string") return pathLike;
  if (!pathLike.startsWith("~")) return resolve(pathLike);
  const home = process.env.HOME || process.env.USERPROFILE;
  if (!home) return resolve(pathLike);
  return resolve(home, pathLike.slice(2));
}

export function normalizeStringList(value) {
  if (!Array.isArray(value)) return [];
  return value
    .map((entry) => (typeof entry === "string" ? entry.trim() : ""))
    .filter(Boolean);
}

export function resolveOpenClawConfigPath() {
  const explicit = process.env.OPENCLAW_CONFIG_PATH?.trim();
  if (explicit) return resolveHomePath(explicit);

  const stateDir = process.env.OPENCLAW_STATE_DIR?.trim();
  if (stateDir) return resolve(resolveHomePath(stateDir), "openclaw.json");

  return resolveHomePath("~/.openclaw/openclaw.json");
}

export function readOpenClawConfig() {
  try {
    return JSON.parse(readFileSync(resolveOpenClawConfigPath(), "utf8"));
  } catch {
    return {};
  }
}

export function resolveCodexHome() {
  const configured = process.env.CODEX_HOME?.trim();
  if (configured) return resolveHomePath(configured);
  return resolveHomePath("~/.codex");
}

export async function collectCodexPluginSkillRoots(codexHome = resolveCodexHome()) {
  const cacheRoot = resolve(codexHome, "plugins", "cache");
  const roots = [];

  let publishers;
  try {
    publishers = await readdir(cacheRoot, { withFileTypes: true });
  } catch {
    return roots;
  }

  for (const publisher of publishers) {
    if (!publisher.isDirectory()) continue;
    const publisherPath = resolve(cacheRoot, publisher.name);

    let plugins;
    try {
      plugins = await readdir(publisherPath, { withFileTypes: true });
    } catch {
      continue;
    }

    for (const plugin of plugins) {
      if (!plugin.isDirectory()) continue;
      const pluginPath = resolve(publisherPath, plugin.name);

      let versions;
      try {
        versions = await readdir(pluginPath, { withFileTypes: true });
      } catch {
        continue;
      }

      for (const version of versions) {
        if (version.isDirectory()) roots.push(resolve(pluginPath, version.name, "skills"));
      }
    }
  }

  return roots;
}

function skillsDirLooksPopulated(root) {
  try {
    const fs = require("node:fs");
    const entries = fs.readdirSync(root, { withFileTypes: true });
    return entries.some((entry) => {
      if (entry.name.startsWith(".")) return false;
      if (entry.isFile() && entry.name.endsWith(".md")) return true;
      if (!entry.isDirectory()) return false;
      return fs.existsSync(resolve(root, entry.name, "SKILL.md"));
    });
  } catch {
    return false;
  }
}

export function resolveBundledSkillsRoot({ argv = process.argv, cwd = process.cwd(), moduleDir } = {}) {
  const override = process.env.OPENCLAW_BUNDLED_SKILLS_DIR?.trim();
  if (override) return resolveHomePath(override);

  const candidateRoots = new Set();
  const addAncestors = (startPath) => {
    if (!startPath) return;
    let current = resolve(startPath);
    try {
      current = dirname(current);
    } catch {
      return;
    }
    for (let depth = 0; depth < 8; depth += 1) {
      candidateRoots.add(current);
      const next = dirname(current);
      if (next === current) break;
      current = next;
    }
  };

  try {
    const execSkills = resolve(dirname(process.execPath), "skills");
    candidateRoots.add(dirname(execSkills));
  } catch {
    // ignore
  }

  try {
    const packagePath = require.resolve("openclaw/package.json");
    candidateRoots.add(dirname(packagePath));
  } catch {
    // Host OpenClaw is not always resolvable from a third-party plugin package.
  }

  addAncestors(argv?.[1]);
  addAncestors(cwd);
  addAncestors(moduleDir);

  for (const root of candidateRoots) {
    const skillsDir = resolve(root, "skills");
    if (skillsDirLooksPopulated(skillsDir)) return skillsDir;
  }

  return undefined;
}

export async function buildSkillRootSpecs({
  workspaceDir,
  includeBundled = false,
  bundledRoot,
  discovery = {},
} = {}) {
  if (!workspaceDir || typeof workspaceDir !== "string") {
    throw new Error("buildSkillRootSpecs requires workspaceDir");
  }
  const enabled = (key, fallback = true) => discovery[key] !== false ? fallback : false;
  const config = readOpenClawConfig();
  const codexHome = resolveCodexHome();
  const workspaceRoot = resolve(workspaceDir);
  const roots = [];

  if (enabled("workspace")) roots.push({ root: resolve(workspaceRoot, "skills"), source: "openclaw-workspace" });
  if (enabled("agentsProject")) roots.push({ root: resolve(workspaceRoot, ".agents", "skills"), source: "agents-skills-project" });
  if (enabled("agentsPersonal")) roots.push({ root: resolveHomePath("~/.agents/skills"), source: "agents-skills-personal" });
  if (enabled("openclawManaged")) roots.push({ root: resolveHomePath("~/.openclaw/skills"), source: "openclaw-managed" });

  if (enabled("openclawBundled") && includeBundled && bundledRoot) {
    roots.push({ root: bundledRoot, source: "openclaw-bundled" });
  }

  if (enabled("extraDirs")) {
    for (const root of normalizeStringList(config?.skills?.load?.extraDirs).map(resolveHomePath)) {
      roots.push({ root, source: "openclaw-extra" });
    }
  }

  if (enabled("openclawPluginSkills")) roots.push({ root: resolveHomePath("~/.openclaw/plugin-skills"), source: "openclaw-extra" });
  if (enabled("codex")) roots.push({ root: resolve(codexHome, "skills"), source: "codex" });

  if (enabled("codexPlugin")) {
    for (const root of await collectCodexPluginSkillRoots(codexHome)) {
      roots.push({ root, source: "codex-plugin" });
    }
  }

  return roots;
}
