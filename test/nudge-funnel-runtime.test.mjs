import test from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { resolve } from "node:path";

const execFileAsync = promisify(execFile);

test("runtime classifies forward nudge outcomes independently and idempotently", async () => {
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
    const tempRoot = await mkdtemp(resolve(tmpdir(), "skill-audit-funnel-runtime-"));

    function openDb(path) {
      try {
        const sqlite3 = require("better-sqlite3");
        const BetterSqlite3 = sqlite3?.default || sqlite3;
        const db = new BetterSqlite3(path);
        return { all: (sql) => db.prepare(sql).all(), exec: (sql) => db.exec(sql), close: () => db.close() };
      } catch {
        const sqlite = require("node:sqlite");
        const db = new sqlite.DatabaseSync(path);
        return { all: (sql) => db.prepare(sql).all(), exec: (sql) => db.exec(sql), close: () => db.close() };
      }
    }

    function createMockApi(pluginConfig, workspaceDir) {
      const hooks = [];
      const errors = [];
      return {
        hooks,
        errors,
        api: {
          registrationMode: "full",
          pluginConfig,
          config: { agents: { main: { workspace: workspaceDir } } },
          logger: { debug() {}, info() {}, warn() {}, error(message) { errors.push(String(message)); } },
          on(name, handler) { hooks.push({ name, handler }); },
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
      const skillRoot = resolve(tempRoot, "skills with spaces");
      const alphaDir = resolve(skillRoot, "alpha");
      const betaDir = resolve(skillRoot, "beta");
      const alphaSkill = resolve(alphaDir, "SKILL.md");
      const betaSkill = resolve(betaDir, "SKILL.md");
      const alphaGuide = resolve(alphaDir, "references", "guide.md");
      const alphaAlias = resolve(tempRoot, "alpha alias");
      const pluginRoot = resolve(tempRoot, "plugin");
      const dbPath = resolve(tempRoot, "audit.db");
      const configPath = resolve(tempRoot, "openclaw.json");
      const openClawCandidates = [process.env.OPENCLAW_CHECKOUT, resolve(repoRoot, "node_modules", "openclaw")].filter(Boolean);
      const openClawCheckout = openClawCandidates.find((candidate) =>
        existsSync(resolve(candidate, "dist", "plugin-sdk", "plugin-entry.js")),
      );
      assert.ok(openClawCheckout, "OpenClaw package not found");

      await mkdir(resolve(alphaDir, "references"), { recursive: true });
      await mkdir(betaDir, { recursive: true });
      await mkdir(workspaceDir, { recursive: true });
      await mkdir(resolve(pluginRoot, "node_modules"), { recursive: true });
      await writeFile(alphaSkill, "---\nname: Alpha Skill\ndescription: Alpha helper.\nmetadata:\n  openclaw:\n    skillKey: alpha\n---\n");
      await writeFile(betaSkill, "---\nname: Beta Skill\ndescription: Beta helper.\nmetadata:\n  openclaw:\n    skillKey: beta\n---\n");
      await writeFile(alphaGuide, "alpha guide\n");
      await symlink(alphaDir, alphaAlias);
      for (const file of ["index.ts", "skill-roots.mjs", "skill-router-helpers.mjs", "nudge-tracking.mjs", "package.json"]) {
        await copyFile(resolve(repoRoot, file), resolve(pluginRoot, file));
      }
      await symlink(openClawCheckout, resolve(pluginRoot, "node_modules", "openclaw"));
      await writeFile(configPath, JSON.stringify({ skills: { load: { extraDirs: [skillRoot] } } }));
      process.env.OPENCLAW_CONFIG_PATH = configPath;
      process.env.CODEX_HOME = resolve(tempRoot, "codex");

      const legacyDb = openDb(dbPath);
      legacyDb.exec([
        "CREATE TABLE skill_nudges (",
        "id INTEGER PRIMARY KEY AUTOINCREMENT,",
        "session_key TEXT, session_id TEXT, agent_id TEXT,",
        "skill_name TEXT NOT NULL, skill_path TEXT, score REAL, match_reason TEXT,",
        "turn_number INTEGER, task_excerpt TEXT,",
        "timestamp DATETIME DEFAULT CURRENT_TIMESTAMP",
        ");",
        "INSERT INTO skill_nudges (session_key, skill_name, skill_path)",
        "VALUES ('historical-session', 'Historical Skill', '/legacy/SKILL.md');",
      ].join("\n"));
      legacyDb.close();

      const plugin = (await import(pathToFileURL(resolve(pluginRoot, "index.ts")).href)).default;
      const { hooks, errors, api } = createMockApi({
        dbPath,
        router: {
          maxSkillsToNudge: 2,
          overrides: [
            { taskPattern: "direct|shell|failed|unrelated|resource|symlink|missing|race|shutdown|atomic", skills: ["alpha"] },
            { taskPattern: "multi", skills: ["alpha", "beta"] },
          ],
          discovery: {
            workspace: false, agentsProject: false, agentsPersonal: false,
            openclawManaged: false, openclawBundled: false, extraDirs: true,
            openclawPluginSkills: false, codex: false, codexPlugin: false,
          },
        },
      }, workspaceDir);
      plugin.register(api);

      async function nudge(runId, sessionKey, task, includeIds = true) {
        const ctx = includeIds
          ? { runId, sessionKey, sessionId: sessionKey, agentId: "main", workspaceDir }
          : { agentId: "main", workspaceDir };
        const result = await runHook(hooks, "before_prompt_build", {
          messages: [{ role: "user", content: task }],
          runId: includeIds ? runId : undefined,
        }, ctx);
        assert.ok(result?.prependContext?.includes("[skill-router:nudge "), "nudge id should be injected");
        return ctx;
      }

      async function after(ctx, runId, toolCallId, toolName, params, extra = {}) {
        await runHook(hooks, "after_tool_call", { runId, toolCallId, toolName, params, result: { success: true }, ...extra }, ctx);
      }

      async function end(ctx, runId, messages = []) {
        await runHook(hooks, "agent_end", { runId, messages, success: true }, ctx);
      }

      async function waitForNudge(runId) {
        for (let attempt = 0; attempt < 100; attempt += 1) {
          let db;
          try {
            db = openDb(dbPath);
            if (db.all("SELECT run_id FROM skill_nudges WHERE run_id='" + runId + "'").length) return;
          } catch {
            // The runtime may still be initializing the additive schema.
          } finally {
            db?.close();
          }
          await new Promise((resolvePromise) => setTimeout(resolvePromise, 5));
        }
        assert.fail("timed out waiting for nudge " + runId);
      }

      const executionCtx = { runId: "run-executions", sessionKey: "session-executions", sessionId: "session-executions", agentId: "main", workspaceDir };
      for (const [toolCallId, path] of [["execution-alpha", alphaSkill], ["execution-beta", betaSkill]]) {
        const event = { runId: "run-executions", toolCallId, toolName: "read", params: { path }, result: { success: true } };
        await runHook(hooks, "before_tool_call", event, executionCtx);
        await runHook(hooks, "after_tool_call", event, executionCtx);
      }
      await runHook(hooks, "session_end", { runId: "run-executions", sessionId: "session-executions" }, executionCtx);

      const privacySentinel = "api_key=SUPER_SECRET_FUNNEL_VALUE";
      let ctx = await nudge("run-direct", "session-direct", "direct");
      await after(ctx, "run-direct", "tool-direct", "read", { path: alphaSkill }, { result: { success: true, output: privacySentinel } });
      await after(ctx, "run-direct", "tool-direct", "read", { path: alphaSkill });
      await after(ctx, "run-direct", "tool-direct-reread", "read", { path: alphaSkill });
      await end(ctx, "run-direct", [{ role: "assistant", content: privacySentinel }]);

      ctx = await nudge("run-shell", "session-shell", "shell");
      await after(ctx, "run-shell", "tool-shell", "exec", { command: 'sed -n \'1,20p\' "' + alphaSkill + '"', workdir: workspaceDir });
      await end(ctx, "run-shell");

      ctx = await nudge("run-failed", "session-failed", "failed");
      await after(ctx, "run-failed", "tool-failed", "read", { path: alphaSkill }, { result: undefined, error: "read failed" });
      await end(ctx, "run-failed");

      ctx = await nudge("run-unrelated", "session-unrelated", "unrelated");
      await after(ctx, "run-unrelated", "tool-other", "read", { path: resolve(tempRoot, "other", "SKILL.md") });
      await end(ctx, "run-unrelated");

      ctx = await nudge("run-resource", "session-resource", "resource");
      await after(ctx, "run-resource", "tool-open", "read", { path: alphaSkill });
      await after(ctx, "run-resource", "tool-resource", "read", { path: alphaGuide });
      await after(ctx, "run-resource", "tool-resource", "read", { path: alphaGuide });
      await end(ctx, "run-resource");

      ctx = await nudge("run-multi", "session-multi", "multi");
      await after(ctx, "run-multi", "tool-beta", "read", { path: betaSkill });
      await end(ctx, "run-multi");

      ctx = await nudge("run-symlink", "session-symlink", "symlink");
      await after(ctx, "run-symlink", "tool-symlink", "read", { path: resolve(alphaAlias, "SKILL.md") });
      await end(ctx, "run-symlink");

      ctx = await nudge("run-race", "session-race", "race");
      const inFlightObservation = after(ctx, "run-race", "tool-race", "read", { path: alphaSkill });
      await end(ctx, "run-race");
      await inFlightObservation;

      ctx = await nudge("run-atomic", "session-atomic", "atomic");
      await waitForNudge("run-atomic");
      const triggerDb = openDb(dbPath);
      triggerDb.exec([
        "CREATE TRIGGER force_open_update_failure",
        "BEFORE UPDATE ON skill_nudges",
        "WHEN NEW.run_id = 'run-atomic' AND NEW.outcome = 'opened'",
        "BEGIN",
        "SELECT RAISE(ABORT, 'forced-open-update');",
        "END;",
      ].join("\n"));
      triggerDb.close();
      await after(ctx, "run-atomic", "tool-atomic", "read", { path: alphaSkill });
      await end(ctx, "run-atomic");

      await nudge(undefined, undefined, "missing", false);
      await nudge("run-shutdown", "session-shutdown", "shutdown");

      await runHook(hooks, "gateway_stop", {}, {});
      const db = openDb(dbPath);
      try {
        const rows = db.all("SELECT run_id, skill_name, skill_key, outcome, resources_used_count, coverage, task_excerpt FROM skill_nudges ORDER BY id");
        const find = (runId, key = "alpha") => rows.find((row) => row.run_id === runId && row.skill_key === key);
        assert.equal(find("run-direct").outcome, "opened");
        assert.equal(find("run-shell").outcome, "opened");
        assert.equal(find("run-failed").outcome, "failed_open");
        assert.equal(find("run-unrelated").outcome, "ignored");
        assert.equal(find("run-resource").outcome, "opened");
        assert.equal(find("run-resource").resources_used_count, 1);
        assert.equal(db.all("SELECT COUNT(*) AS count FROM skill_nudge_events WHERE event_type='resources_used'")[0].count, 1);
        assert.equal(find("run-multi", "alpha").outcome, "ignored");
        assert.equal(find("run-multi", "beta").outcome, "opened");
        assert.equal(find("run-symlink").outcome, "opened");
        assert.equal(find("run-race").outcome, "opened", "agent_end must wait for earlier tool observations");
        assert.equal(find("run-atomic").outcome, "unknown", "failed atomic persistence must degrade classification");
        assert.equal(db.all("SELECT e.event_type FROM skill_nudge_events e JOIN skill_nudges n ON n.nudge_id=e.nudge_id WHERE n.run_id='run-atomic' AND e.event_type='opened'").length, 0, "failed outcome updates must roll back their ledger event");
        assert.equal(find("run-shutdown").outcome, "unknown", "gateway shutdown must finalize pending nudges");
        assert.equal(rows.some((row) => row.outcome === "nudged"), false, "shutdown must not strand active rows");
        assert.ok(rows.some((row) => row.outcome === "unknown" && row.run_id == null));
        const historical = rows.find((row) => row.skill_name === "Historical Skill");
        assert.equal(historical.outcome, null, "migration must not classify historical nudges");
        assert.equal(historical.skill_key, null, "migration must not rewrite historical identity");
        assert.equal(db.all("PRAGMA user_version")[0].user_version, 2);
        assert.equal(rows.every((row) => row.task_excerpt == null), true, "task excerpts must remain opt-in");

        const directOpened = db.all("SELECT n.opened_at, e.occurred_at FROM skill_nudge_events e JOIN skill_nudges n ON n.nudge_id=e.nudge_id WHERE n.run_id='run-direct' AND e.event_type='opened'");
        assert.equal(directOpened.length, 1, "duplicate delivery and later rereads must preserve the first open");
        assert.equal(directOpened[0].opened_at, directOpened[0].occurred_at, "opened_at must remain the first successful open");
        const directAttempts = db.all("SELECT event_type FROM skill_nudge_events e JOIN skill_nudges n ON n.nudge_id=e.nudge_id WHERE n.run_id='run-direct' AND e.event_type='open_attempt'");
        assert.equal(directAttempts.length, 2, "a distinct reread may remain in the attempt ledger");

        const allExecutions = db.all("SELECT skill_name, skill_path, run_id FROM skill_executions ORDER BY id");
        const executions = allExecutions
          .filter((row) => row.run_id === "run-executions")
          .map(({ skill_name, skill_path }) => ({ skill_name, skill_path }))
          .sort((left, right) => left.skill_name.localeCompare(right.skill_name));
        assert.equal(executions.length, 2, JSON.stringify(allExecutions));
        assert.deepEqual(executions, [
          { skill_name: "alpha", skill_path: alphaSkill },
          { skill_name: "beta", skill_path: betaSkill },
        ], "sequential skill reads must remain separate and internally consistent");
        const storedFunnelData = JSON.stringify([
          ...db.all("SELECT * FROM skill_nudges"),
          ...db.all("SELECT * FROM skill_nudge_events"),
          ...db.all("SELECT * FROM skill_events"),
        ]);
        assert.equal(storedFunnelData.includes(privacySentinel), false, "prompt and tool-result content must not be persisted");
        assert.equal(errors.length, 1, JSON.stringify(errors));
        assert.match(errors[0], /forced-open-update/);
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
    env: { ...process.env, REPO_ROOT: resolve(new URL("..", import.meta.url).pathname) },
    maxBuffer: 1024 * 1024,
  });

  assert.equal(stderr.trim().replace(/\(node:\d+\) ExperimentalWarning:[\s\S]*/m, "").trim(), "");
});
