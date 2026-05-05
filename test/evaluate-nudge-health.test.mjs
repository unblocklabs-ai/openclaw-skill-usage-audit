import test from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const execFileAsync = promisify(execFile);

function openWritableDb(path) {
  try {
    const sqlite3 = require("better-sqlite3");
    const BetterSqlite3 = sqlite3?.default || sqlite3;
    const db = new BetterSqlite3(path);
    return {
      exec: (sql) => db.exec(sql),
      close: () => db.close(),
    };
  } catch {
    // fallback below
  }

  try {
    const sqlite = require("node:sqlite");
    const DatabaseSync = sqlite.DatabaseSync;
    const db = new DatabaseSync(path);
    return {
      exec: (sql) => db.exec(sql),
      close: () => db.close(),
    };
  } catch (error) {
    return { error };
  }
}

test("nudge evaluator does not attribute reads through agent_id fallback", async (t) => {
  const dir = await mkdtemp(join(tmpdir(), "nudge-health-"));
  const dbPath = resolve(dir, "audit.db");
  const db = openWritableDb(dbPath);
  if (db.error) {
    t.skip(`sqlite unavailable: ${String(db.error?.message || db.error)}`);
    return;
  }

  const baseMs = Date.now() - 60 * 60 * 1000;
  const ts = (offsetMinutes) => new Date(baseMs + offsetMinutes * 60 * 1000).toISOString();

  db.exec(`
    CREATE TABLE skill_nudges (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      session_key TEXT,
      session_id TEXT,
      run_id TEXT,
      agent_id TEXT,
      skill_name TEXT NOT NULL,
      skill_key TEXT,
      skill_path TEXT,
      timestamp TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE skill_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts TEXT NOT NULL,
      type TEXT NOT NULL,
      session_id TEXT,
      session_key TEXT,
      run_id TEXT,
      agent_id TEXT,
      skill_name TEXT
    );
    INSERT INTO skill_nudges (run_id, agent_id, skill_name, skill_key, timestamp)
    VALUES ('run-a', 'agent-main', 'Alpha Skill', 'alpha', '${ts(0)}');
    INSERT INTO skill_nudges (run_id, agent_id, skill_name, skill_key, timestamp)
    VALUES ('run-b', 'agent-main', 'Alpha Skill', 'alpha', '${ts(10)}');
    INSERT INTO skill_events (ts, type, run_id, agent_id, skill_name)
    VALUES ('${ts(5)}', 'skill_file_read', 'different-run', 'agent-main', 'Alpha Skill');
    INSERT INTO skill_events (ts, type, run_id, agent_id, skill_name)
    VALUES ('${ts(11)}', 'skill_file_read', 'run-b', 'agent-main', 'Alpha Skill');
  `);
  db.close();

  const { stdout } = await execFileAsync(process.execPath, [
    resolve("evaluate-nudge-health.mjs"),
    "--db-path",
    dbPath,
    "--days",
    "30",
    "--json",
  ], {
    cwd: resolve(new URL("..", import.meta.url).pathname),
  });

  const rows = JSON.parse(stdout);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].nudge_count, 2);
  assert.equal(rows[0].read_count, 1);
});

test("nudge evaluator reports router decision reasons", async (t) => {
  const dir = await mkdtemp(join(tmpdir(), "nudge-decisions-"));
  const dbPath = resolve(dir, "audit.db");
  const db = openWritableDb(dbPath);
  if (db.error) {
    t.skip(`sqlite unavailable: ${String(db.error?.message || db.error)}`);
    return;
  }

  db.exec(`
    CREATE TABLE skill_router_decisions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
      target_type TEXT,
      decision TEXT NOT NULL,
      reason TEXT NOT NULL,
      candidate_count INTEGER DEFAULT 0,
      available_count INTEGER DEFAULT 0,
      scored_count INTEGER DEFAULT 0,
      selected_count INTEGER DEFAULT 0,
      selected_skill_keys TEXT,
      top_candidates TEXT
    );
    INSERT INTO skill_router_decisions (
      target_type,
      decision,
      reason,
      candidate_count,
      available_count,
      scored_count,
      selected_count,
      selected_skill_keys,
      top_candidates
    ) VALUES (
      'agent',
      'skipped',
      'all_below_threshold',
      3,
      3,
      3,
      0,
      NULL,
      '[{"skill_key":"github","skill_name":"GitHub","score":4.2}]'
    );
    INSERT INTO skill_router_decisions (
      target_type,
      decision,
      reason,
      candidate_count,
      available_count,
      scored_count,
      selected_count,
      selected_skill_keys,
      top_candidates
    ) VALUES (
      'agent',
      'nudged',
      'score_threshold',
      3,
      3,
      3,
      1,
      '["github"]',
      '[{"skill_key":"github","skill_name":"GitHub","score":9.1}]'
    );
  `);
  db.close();

  const { stdout } = await execFileAsync(process.execPath, [
    resolve("evaluate-nudge-health.mjs"),
    "--db-path",
    dbPath,
    "--days",
    "30",
    "--decisions",
    "--json",
  ], {
    cwd: resolve(new URL("..", import.meta.url).pathname),
  });

  const report = JSON.parse(stdout);
  assert.deepEqual(report.reasonSummary.map((row) => [row.decision, row.reason, row.count]), [
    ["nudged", "score_threshold", 1],
    ["skipped", "all_below_threshold", 1],
  ]);
  assert.equal(report.recentDecisions.length, 2);
  assert.equal(report.recentDecisions[0].top_candidates[0].skill_key, "github");
});
