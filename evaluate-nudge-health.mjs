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

    if (arg === "--help" || arg === "-h") {
      console.log(`Usage: node evaluate-nudge-health.mjs [--days N] [--db-path PATH] [--json]\n  --days    Days back to evaluate (default: ${DEFAULT_DAYS})\n  --db-path Path to SQLite db (default: ${DEFAULT_DB_PATH})\n  --json    Print JSON output`);
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
      all: (sql, params) => db.prepare(sql).all(params),
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
      return stmt.all(params);
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

  try {
    const nudgeRows = db.all(`SELECT name FROM sqlite_master WHERE type='table' AND name='skill_nudges'`);
    hasSkillNudges = Array.isArray(nudgeRows) && nudgeRows.length > 0;
  } catch (err) {
    db.close();
    console.error(`Database schema check failed: ${String(err)}`);
    process.exit(1);
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
          WHERE e.session_key = n.session_key
            AND LOWER(e.skill_name) = LOWER(n.skill_name)
            AND e.type = 'skill_file_read'
            AND e.ts >= @window_start
            AND e.ts >= n.timestamp
        ) THEN 1 ELSE 0 END
      ) AS read_count,
      SUM(
        CASE WHEN EXISTS(
          SELECT 1
          FROM skill_events e
          WHERE e.session_key = n.session_key
            AND LOWER(e.skill_name) = LOWER(n.skill_name)
            AND e.type = 'tool_call_end'
            AND e.ts >= @window_start
            AND e.ts >= n.timestamp
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
