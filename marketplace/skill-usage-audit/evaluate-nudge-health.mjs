#!/usr/bin/env node
/**
 * Skill router nudge evaluator.
 *
 * Joins recorded nudges against downstream skill read/tool usage to estimate conversion.
 */

import { resolve } from "node:path";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

const DEFAULT_DB_PATH = "~/.openclaw/audits/skill-usage.db";
const DEFAULT_DAYS = 14;

function resolveHomePath(pathLike) {
  if (!pathLike || typeof pathLike !== "string") return pathLike;
  if (!pathLike.startsWith("~")) return resolve(pathLike);
  const home = process.env.HOME || process.env.USERPROFILE;
  if (!home) return resolve(pathLike);
  return resolve(home, pathLike.slice(2));
}

function parseArgs() {
  const out = {
    dbPath: DEFAULT_DB_PATH,
    days: DEFAULT_DAYS,
    json: false,
    decisions: false,
  };

  const args = process.argv.slice(2);
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];

    if (arg === "--days" || arg === "--day" || arg === "--window") {
      const v = Number.parseInt(args[i + 1], 10);
      if (Number.isFinite(v) && v > 0) out.days = v;
      i += 1;
      continue;
    }

    if (arg.startsWith("--days=")) {
      const v = Number.parseInt(arg.split("=")[1], 10);
      if (Number.isFinite(v) && v > 0) out.days = v;
      continue;
    }

    if (arg === "--db-path" || arg === "--database") {
      const candidate = args[i + 1] ? String(args[i + 1]).trim() : "";
      if (candidate) {
        out.dbPath = candidate;
      }
      i += 1;
      continue;
    }

    if (arg.startsWith("--db-path=")) {
      const candidate = arg.slice("--db-path=".length).trim();
      if (candidate) out.dbPath = candidate;
      continue;
    }

    if (arg === "--json") {
      out.json = true;
      continue;
    }

    if (arg === "--decisions") {
      out.decisions = true;
      continue;
    }

    if (arg === "--help" || arg === "-h") {
      console.log(`Usage: node evaluate-nudge-health.mjs [--days N] [--db-path PATH] [--json] [--decisions]\n  --days      Days back to evaluate (default: ${DEFAULT_DAYS})\n  --db-path   Path to SQLite db (default: ${DEFAULT_DB_PATH})\n  --json      Print JSON output\n  --decisions Show router decision skip/nudge reasons and recent near misses`);
      process.exit(0);
    }
  }

  return out;
}

function openDb(path) {
  try {
    const sqlite3 = require("better-sqlite3");
    const BetterSqlite3 = sqlite3?.default || sqlite3;
    const db = new BetterSqlite3(path);
    return {
      all: (sql, params) => (params === undefined ? db.prepare(sql).all() : db.prepare(sql).all(params)),
      close: () => db.close(),
    };
  } catch {
    // fallback
  }

  const sqlite = require("node:sqlite");
  const DatabaseSync = sqlite.DatabaseSync;
  const db = new DatabaseSync(path);
  return {
    all: (sql, params) => {
      const stmt = db.prepare(sql);
      return params === undefined ? stmt.all() : stmt.all(params);
    },
    close: () => db.close(),
  };
}

function formatRate(value) {
  if (value === null || value === undefined || !Number.isFinite(value)) return "N/A";
  return `${(value * 100).toFixed(2)}%`;
}

function formatCount(value) {
  if (value === null || value === undefined) return "N/A";
  return String(Math.max(0, Math.floor(Number(value))));
}

function tableColumns(db, tableName) {
  try {
    const rows = db.all(`PRAGMA table_info(${tableName})`);
    return new Set((Array.isArray(rows) ? rows : []).map((row) => String(row.name)));
  } catch {
    return new Set();
  }
}

function tableExists(db, tableName) {
  try {
    const rows = db.all(`SELECT name FROM sqlite_master WHERE type='table' AND name=@table_name`, { table_name: tableName });
    return Array.isArray(rows) && rows.length > 0;
  } catch {
    return false;
  }
}

function formatTable(rows) {
  const headers = ["Skill", "Nudges", "Reads", "Read Rate", "Uses", "Use Rate"];
  const values = rows.map((row) => {
    const readRate = row.read_count == null ? null : (row.nudge_count ? row.read_count / row.nudge_count : null);
    const useRate = row.use_count == null ? null : (row.nudge_count ? row.use_count / row.nudge_count : null);
    return [
      row.skill_name,
      formatCount(row.nudge_count),
      formatCount(row.read_count),
      formatRate(readRate),
      formatCount(row.use_count),
      formatRate(useRate),
    ];
  });

  const widths = headers.map((header, idx) => {
    let max = header.length;
    for (const row of values) {
      if (row[idx].length > max) max = row[idx].length;
    }
    return max;
  });

  const fmtRow = (cells) => cells.map((cell, idx) => cell.padEnd(widths[idx], " ")).join(" | ");
  const sep = widths.map((w) => "-".repeat(w)).join("-+-");

  const lines = [
    fmtRow(headers),
    sep,
    ...values.map((row) => fmtRow(row)),
  ];

  console.log(lines.join("\n"));
}

function parseJsonArray(value) {
  if (!value || typeof value !== "string") return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function formatDecisionReport(report) {
  const lines = [];
  lines.push("Router decision reasons");
  lines.push("-----------------------");
  if (!report.reasonSummary.length) {
    lines.push("No router decisions found in the configured window.");
  } else {
    for (const row of report.reasonSummary) {
      lines.push(`${row.decision}/${row.reason}: ${row.count}`);
    }
  }

  if (report.recentDecisions.length) {
    lines.push("");
    lines.push("Recent decisions");
    lines.push("----------------");
    for (const row of report.recentDecisions) {
      const top = row.top_candidates.slice(0, 3)
        .map((candidate) => `${candidate.skill_key || candidate.skill_name}:${candidate.score}`)
        .join(", ");
      lines.push(`${row.timestamp} ${row.target_type} ${row.decision}/${row.reason} selected=${row.selected_count} candidates=${row.candidate_count}${top ? ` top=[${top}]` : ""}`);
    }
  }

  console.log(lines.join("\n"));
}

function buildDecisionReport(db, windowStart) {
  const reasonSummary = db.all(`
    SELECT
      decision,
      reason,
      COUNT(*) AS count
    FROM skill_router_decisions
    WHERE timestamp >= @window_start
    GROUP BY decision, reason
    ORDER BY count DESC, decision ASC, reason ASC
  `, { window_start: windowStart }).map((row) => ({
    decision: String(row.decision || ""),
    reason: String(row.reason || ""),
    count: Number(row.count) || 0,
  }));

  const recentDecisions = db.all(`
    SELECT
      timestamp,
      target_type,
      decision,
      reason,
      candidate_count,
      available_count,
      scored_count,
      selected_count,
      selected_skill_keys,
      top_candidates
    FROM skill_router_decisions
    WHERE timestamp >= @window_start
    ORDER BY id DESC
    LIMIT 20
  `, { window_start: windowStart }).map((row) => ({
    timestamp: String(row.timestamp || ""),
    target_type: String(row.target_type || ""),
    decision: String(row.decision || ""),
    reason: String(row.reason || ""),
    candidate_count: Number(row.candidate_count) || 0,
    available_count: Number(row.available_count) || 0,
    scored_count: Number(row.scored_count) || 0,
    selected_count: Number(row.selected_count) || 0,
    selected_skill_keys: parseJsonArray(row.selected_skill_keys),
    top_candidates: parseJsonArray(row.top_candidates),
  }));

  return { reasonSummary, recentDecisions };
}

function main() {
  const args = parseArgs();
  const dbPath = resolveHomePath(args.dbPath);
  const windowDays = Number.isFinite(args.days) && args.days > 0 ? Math.floor(args.days) : DEFAULT_DAYS;
  const windowStart = new Date(Date.now() - windowDays * 24 * 60 * 60 * 1000).toISOString();

  let db;
  try {
    db = openDb(dbPath);
  } catch (err) {
    console.error(`failed to open db: ${String(err)}`);
    process.exit(1);
  }

  let hasSkillNudges = false;
  let hasSkillEvents = false;
  let hasRouterDecisions = false;

  try {
    hasSkillNudges = tableExists(db, "skill_nudges");
    hasRouterDecisions = tableExists(db, "skill_router_decisions");
  } catch (err) {
    db.close();
    console.error(`Database schema check failed: ${String(err)}`);
    process.exit(1);
  }

  if (args.decisions) {
    if (!hasRouterDecisions) {
      console.log("No skill_router_decisions table found. Run the plugin with router observability enabled.");
      db.close();
      return;
    }

    const report = buildDecisionReport(db, windowStart);
    db.close();
    if (args.json) {
      console.log(JSON.stringify(report, null, 2));
    } else {
      formatDecisionReport(report);
    }
    return;
  }

  if (!hasSkillNudges) {
    console.log("No skill_nudges table found. Run the plugin long enough to populate nudges.");
    db.close();
    return;
  }

  try {
    const eventRows = db.all(`SELECT name FROM sqlite_master WHERE type='table' AND name='skill_events'`);
    hasSkillEvents = Array.isArray(eventRows) && eventRows.length > 0;
  } catch (err) {
    db.close();
    console.error(`Database schema check failed: ${String(err)}`);
    process.exit(1);
  }

  if (!hasSkillEvents) {
    console.log("Warning: skill_events table missing; reporting nudge counts only.");
  }

  const nudgeColumns = tableColumns(db, "skill_nudges");
  const hasNudgeRunId = nudgeColumns.has("run_id");
  const hasNudgeSkillKey = nudgeColumns.has("skill_key");
  const nudgeRunId = hasNudgeRunId ? "n.run_id" : "NULL";
  const nudgeSkillIdentity = hasNudgeSkillKey ? "COALESCE(n.skill_key, n.skill_name)" : "n.skill_name";
  const eventMatchSql = `
              (
                (${nudgeRunId} IS NOT NULL AND e.run_id = ${nudgeRunId})
                OR (${nudgeRunId} IS NULL AND n.session_key IS NOT NULL AND e.session_key = n.session_key)
                OR (${nudgeRunId} IS NULL AND n.session_key IS NULL AND n.session_id IS NOT NULL AND e.session_id = n.session_id)
              )
              AND (
                LOWER(e.skill_name) = LOWER(${nudgeSkillIdentity})
                OR LOWER(e.skill_name) = LOWER(n.skill_name)
              )
  `;

  const nudgeSql = `
    SELECT
      n.skill_name AS skill_name,
      COUNT(*) AS nudge_count
    FROM skill_nudges n
    WHERE n.timestamp >= @window_start
    GROUP BY n.skill_name
    ORDER BY nudge_count DESC, skill_name ASC
  `;

  const sql = hasSkillEvents
    ? `
    SELECT
      n.skill_name AS skill_name,
      COUNT(*) AS nudge_count,
      SUM(
        CASE WHEN EXISTS(
	          SELECT 1
	          FROM skill_events e
	          WHERE ${eventMatchSql}
	            AND e.type = 'skill_file_read'
	            AND datetime(e.ts) >= datetime(@window_start)
	            AND datetime(e.ts) >= datetime(n.timestamp)
	            AND datetime(e.ts) < datetime(n.timestamp, '+1 hour')
	        ) THEN 1 ELSE 0 END
	      ) AS read_count,
      SUM(
        CASE WHEN EXISTS(
	          SELECT 1
	          FROM skill_events e
	          WHERE ${eventMatchSql}
	            AND e.type = 'tool_call_end'
	            AND datetime(e.ts) >= datetime(@window_start)
	            AND datetime(e.ts) >= datetime(n.timestamp)
	            AND datetime(e.ts) < datetime(n.timestamp, '+1 hour')
	        ) THEN 1 ELSE 0 END
      ) AS use_count
    FROM skill_nudges n
    WHERE n.timestamp >= @window_start
    GROUP BY n.skill_name
    ORDER BY nudge_count DESC, skill_name ASC
  `
    : nudgeSql;

  let rows;
  try {
    rows = db.all(sql, { window_start: windowStart });
  } catch (err) {
    db.close();
    const msg = String(err || "");
    if (/no such table/i.test(msg) && /skill_nudges/i.test(msg)) {
      console.log("No skill_nudges data found. Run the plugin long enough to populate nudges.");
      return;
    }

    if (/no such table/i.test(msg) || /no such column/i.test(msg)) {
      console.error("Database schema incomplete. Run the plugin first to initialize tables.");
      console.error(msg);
      process.exit(1);
    }

    console.error(msg);
    process.exit(1);
  }
  db.close();

  const normalized = rows.map((row) => ({
    skill_name: String(row.skill_name || ""),
    nudge_count: Number(row.nudge_count) || 0,
    read_count: hasSkillEvents ? Number(row.read_count) || 0 : null,
    use_count: hasSkillEvents ? Number(row.use_count) || 0 : null,
  }));

  if (args.json) {
    console.log(
      JSON.stringify(
        normalized.map((row) => ({
          ...row,
          read_rate: row.read_count === null || row.nudge_count === 0 ? null : row.read_count / row.nudge_count,
          use_rate: row.use_count === null || row.nudge_count === 0 ? null : row.use_count / row.nudge_count,
        })),
        null,
        2,
      ),
    );
    return;
  }

  if (!normalized.length) {
    console.log("No nudges found in the configured window.");
    return;
  }

  formatTable(normalized);
}

main();
