import test from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { resolve } from "node:path";

const execFileAsync = promisify(execFile);

test("plugin router hook nudges by skill key keywords and suppresses repeats", async () => {
  const scenario = String.raw`
    import assert from "node:assert/strict";
    import { createRequire } from "node:module";
    import { copyFile, mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
    import { existsSync } from "node:fs";
    import { tmpdir } from "node:os";
    import { resolve } from "node:path";
    import { pathToFileURL } from "node:url";

    const require = createRequire(import.meta.url);
    const repoRoot = process.env.REPO_ROOT;
    const tempRoot = await mkdtemp(resolve(tmpdir(), "skill-audit-router-runtime-"));
    const openClawCandidates = [
      process.env.OPENCLAW_CHECKOUT,
      resolve(repoRoot, "node_modules", "openclaw"),
      "/Users/bek/Desktop/openclaw",
    ].filter(Boolean);

    function openDb(path) {
      try {
        const sqlite3 = require("better-sqlite3");
        const BetterSqlite3 = sqlite3?.default || sqlite3;
        const db = new BetterSqlite3(path);
        return {
          all: (sql) => db.prepare(sql).all(),
          close: () => db.close(),
        };
      } catch {
        const sqlite = require("node:sqlite");
        const db = new sqlite.DatabaseSync(path);
        return {
          all: (sql) => db.prepare(sql).all(),
          close: () => db.close(),
        };
      }
    }

    function createMockApi(pluginConfig, workspaceDir) {
      const hooks = [];
      return {
        hooks,
        api: {
          registrationMode: "full",
          pluginConfig,
          config: { agents: { main: { workspace: workspaceDir } } },
          logger: {
            debug() {},
            info() {},
            warn() {},
            error(message) { throw new Error(String(message)); },
          },
          on(name, handler) {
            hooks.push({ name, handler });
          },
        },
      };
    }

    async function runHook(hooks, name, event = {}, ctx = {}) {
      const hook = hooks.find((entry) => entry.name === name);
      assert.ok(hook, "missing hook " + name);
      return hook.handler(event, ctx);
    }

    try {
      const workspaceDir = resolve(tempRoot, "workspace");
      const skillRoot = resolve(tempRoot, "skills");
      const skillDir = resolve(skillRoot, "keyed-router-skill");
      const pluginRoot = resolve(tempRoot, "plugin");
      const openClawCheckout = openClawCandidates.find((candidate) =>
        existsSync(resolve(candidate, "dist", "plugin-sdk", "plugin-entry.js")),
      );
      const configPath = resolve(tempRoot, "openclaw.json");
      const dbPath = resolve(tempRoot, "audit.db");
      assert.ok(openClawCheckout, "OpenClaw checkout/package not found; set OPENCLAW_CHECKOUT or run npm install");
      await mkdir(skillDir, { recursive: true });
      await mkdir(workspaceDir, { recursive: true });
      await mkdir(resolve(pluginRoot, "node_modules"), { recursive: true });
      for (const file of ["index.ts", "skill-roots.mjs", "skill-router-helpers.mjs", "nudge-tracking.mjs", "package.json"]) {
        await copyFile(resolve(repoRoot, file), resolve(pluginRoot, file));
      }
      await symlink(openClawCheckout, resolve(pluginRoot, "node_modules", "openclaw"));
      await writeFile(resolve(skillDir, "SKILL.md"), [
        "---",
        "name: Display Router Skill",
        "description: General routing helper.",
        "metadata:",
        "  openclaw:",
        "    skillKey: keyed-router-skill",
        "---",
        "",
        "Use this skill for routed work.",
      ].join("\n"));
      await writeFile(configPath, JSON.stringify({ skills: { load: { extraDirs: [skillRoot] } } }));
      process.env.OPENCLAW_CONFIG_PATH = configPath;
      process.env.CODEX_HOME = resolve(tempRoot, "codex");

      const pluginModule = await import(pathToFileURL(resolve(pluginRoot, "index.ts")).href);
      const plugin = pluginModule.default;
      const { hooks, api } = createMockApi({
        dbPath,
        router: {
          agentMinScore: 5,
          recencyFallbackMinutes: 30,
          skillKeywords: {
            "keyed-router-skill": ["peculiarword"],
          },
          discovery: {
            workspace: false,
            agentsProject: false,
            agentsPersonal: false,
            openclawManaged: false,
            openclawBundled: false,
            extraDirs: true,
            openclawPluginSkills: false,
            codex: false,
            codexPlugin: false,
          },
          observability: {
            enabled: true,
            topCandidates: 5,
            retentionDays: 30,
          },
        },
      }, workspaceDir);
      plugin.register(api);

      const ctx = { sessionKey: "session-main", sessionId: "session-main", agentId: "main", workspaceDir };
      const event = {
        messages: [
          { role: "user", content: "please use peculiarword for this task" },
        ],
      };
      const first = await runHook(hooks, "before_prompt_build", event, ctx);
      assert.ok(first?.prependContext?.includes("Display Router Skill"), "first agent turn should be nudged");
      assert.ok(first.prependContext.includes("[skill-router:id keyed-router-skill]"), "nudge should use canonical skill key");

      const repeated = await runHook(hooks, "before_prompt_build", event, ctx);
      assert.equal(repeated, undefined, "repeat in same session should be suppressed");

      const assistantLatest = await runHook(hooks, "before_prompt_build", {
        messages: [
          { role: "user", content: "please use peculiarword for this task" },
          { role: "assistant", content: "working on it" },
        ],
      }, { ...ctx, sessionKey: "session-assistant-latest", sessionId: "session-assistant-latest" });
      assert.equal(assistantLatest, undefined, "regular agent should only nudge immediately after a user message");

      const existingSkillBlock = await runHook(hooks, "before_prompt_build", {
        prompt: '<skill name="Display Router Skill" location="' + resolve(skillDir, "SKILL.md") + '"></skill>',
        messages: [
          { role: "user", content: "please use peculiarword for this task" },
        ],
      }, { ...ctx, sessionKey: "session-skill-block", sessionId: "session-skill-block" });
      assert.equal(existingSkillBlock, undefined, "prompt with an existing skill block should not be nudged");

      await runHook(hooks, "gateway_stop");
      assert.ok(existsSync(dbPath), "router hook should initialize sqlite db");
      const db = openDb(dbPath);
      try {
        const nudges = db.all("SELECT skill_name, skill_key FROM skill_nudges ORDER BY id ASC");
        assert.equal(nudges.length, 1);
        assert.equal(nudges[0].skill_key, "keyed-router-skill");
        const decisions = db.all("SELECT decision, reason FROM skill_router_decisions ORDER BY id ASC");
        assert.ok(decisions.some((row) => row.decision === "nudged" && row.reason === "score_threshold"));
        assert.ok(decisions.some((row) => row.decision === "skipped" && row.reason === "all_suppressed_recently"));
        assert.ok(decisions.some((row) => row.decision === "skipped" && row.reason === "latest_message_not_user"));
        assert.ok(decisions.some((row) => row.decision === "skipped" && row.reason === "skill_block_present"));
      } finally {
        db.close();
      }
    } finally {
      await rm(tempRoot, { recursive: true, force: true });
    }
  `;

  const { stderr } = await execFileAsync(process.execPath, [
    "--experimental-strip-types",
    "--input-type=module",
    "--eval",
    scenario,
  ], {
    cwd: resolve(new URL("..", import.meta.url).pathname),
    env: {
      ...process.env,
      REPO_ROOT: resolve(new URL("..", import.meta.url).pathname),
    },
    maxBuffer: 1024 * 1024,
  });

  assert.equal(stderr.trim().replace(/\(node:\d+\) ExperimentalWarning:[\s\S]*/m, "").trim(), "");
});
