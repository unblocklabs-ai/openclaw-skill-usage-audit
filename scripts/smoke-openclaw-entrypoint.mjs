#!/usr/bin/env node
import { copyFile, mkdtemp, mkdir, readFile, symlink, rm } from "node:fs/promises";
import { existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const repoRoot = resolve(fileURLToPath(new URL("..", import.meta.url)));
const openClawCandidates = [
  process.env.OPENCLAW_CHECKOUT,
  resolve(repoRoot, "node_modules", "openclaw"),
  "/Users/bek/Desktop/openclaw",
].filter(Boolean).map((candidate) => resolve(candidate));
const expectedHooks = [
  "session_start",
  "session_end",
  "before_tool_call",
  "after_tool_call",
  "message_received",
  "message_sent",
  "before_prompt_build",
  "gateway_stop",
];

function fail(message) {
  console.error(`entrypoint smoke failed: ${message}`);
  process.exit(1);
}

async function readJson(path) {
  return JSON.parse(await readFile(path, "utf8"));
}

function assertEntryShape(plugin) {
  if (!plugin || typeof plugin !== "object") fail("default export is not an object");
  if (plugin.id !== "skill-usage-audit") fail(`unexpected plugin id: ${String(plugin.id)}`);
  if (typeof plugin.register !== "function") fail("default export has no register(api) function");
}

function createMockApi({ registrationMode = "full", pluginConfig = {} } = {}) {
  const hooks = [];
  return {
    hooks,
    api: {
      registrationMode,
      pluginConfig,
      config: {},
      logger: {
        debug() {},
        info() {},
        warn() {},
        error() {},
      },
      on(name, handler) {
        hooks.push({ name, handler });
      },
    },
  };
}

async function runHook(hooks, name, event = {}, ctx = {}) {
  const hook = hooks.find((entry) => entry.name === name);
  if (!hook) fail(`missing hook ${name}`);
  await hook.handler(event, ctx);
}

async function main() {
  const openClawCheckout = openClawCandidates.find((candidate) => existsSync(candidate));
  if (!openClawCheckout) {
    fail("OpenClaw checkout/package not found; set OPENCLAW_CHECKOUT or run npm install");
  }

  const packageJson = await readJson(resolve(repoRoot, "package.json"));
  const manifest = await readJson(resolve(repoRoot, "openclaw.plugin.json"));
  const packageEntries = packageJson?.openclaw?.extensions || [];
  if (!Array.isArray(packageEntries) || !packageEntries.includes("index.ts")) {
    fail("package.json openclaw.extensions must include index.ts");
  }
  if (manifest.id !== "skill-usage-audit") {
    fail(`openclaw.plugin.json id mismatch: ${String(manifest.id)}`);
  }

  const tempRoot = await mkdtemp(join(tmpdir(), "skill-audit-entrypoint-"));
  try {
    const pluginRoot = resolve(tempRoot, "plugin");
    await mkdir(resolve(pluginRoot, "node_modules"), { recursive: true });

    for (const file of ["index.ts", "skill-roots.mjs", "skill-router-helpers.mjs", "package.json", "openclaw.plugin.json"]) {
      await copyFile(resolve(repoRoot, file), resolve(pluginRoot, file));
    }
    await symlink(openClawCheckout, resolve(pluginRoot, "node_modules", "openclaw"));

    const mod = await import(pathToFileURL(resolve(pluginRoot, "index.ts")).href);
    const plugin = mod.default;
    assertEntryShape(plugin);

    const skipped = createMockApi({ registrationMode: "metadata" });
    plugin.register(skipped.api);
    if (skipped.hooks.length !== 0) {
      fail(`metadata registration wired ${skipped.hooks.length} hooks`);
    }

    const dbPath = resolve(tempRoot, "audit.db");
    const full = createMockApi({
      registrationMode: "full",
      pluginConfig: { dbPath },
    });
    plugin.register(full.api);
    const hookNames = full.hooks.map((hook) => hook.name).sort();
    const missing = expectedHooks.filter((hook) => !hookNames.includes(hook));
    if (missing.length) fail(`missing expected hooks: ${missing.join(", ")}`);

    await runHook(full.hooks, "session_start", { sessionId: "smoke-session" }, { sessionKey: "smoke-session" });
    await runHook(full.hooks, "gateway_stop");
    if (!existsSync(dbPath)) fail("session_start did not initialize sqlite db");

    console.log("entrypoint smoke ok");
  } finally {
    await rm(tempRoot, { recursive: true, force: true });
  }
}

main().catch((error) => fail(String(error?.stack || error)));
