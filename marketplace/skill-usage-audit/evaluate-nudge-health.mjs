#!/usr/bin/env node
/**
 * Forward-looking skill-router funnel and portfolio evaluator.
 *
 * Funnel-v2 rows are classified by runtime hooks. Historical rows remain
 * unclassified and are never inferred or rewritten by this evaluator.
 */

import { createHash } from "node:crypto";
import { resolve } from "node:path";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const DEFAULT_DB_PATH = "~/.openclaw/audits/skill-usage.db";
const DEFAULT_DAYS = 14;
const DEFAULT_MIN_NUDGES = 3;
const DEFAULT_POOR_RATE = 0.25;

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
    minNudges: DEFAULT_MIN_NUDGES,
    poorRate: DEFAULT_POOR_RATE,
  };
  const args = process.argv.slice(2);
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];
    const readPositiveInt = (value, fallback) => {
      const parsed = Number.parseInt(value, 10);
      return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
    };
    if (arg === "--days" || arg === "--day" || arg === "--window") {
      out.days = readPositiveInt(args[i + 1], out.days);
      i += 1;
    } else if (arg.startsWith("--days=")) {
      out.days = readPositiveInt(arg.slice("--days=".length), out.days);
    } else if (arg === "--db-path" || arg === "--database") {
      const candidate = String(args[i + 1] || "").trim();
      if (candidate) out.dbPath = candidate;
      i += 1;
    } else if (arg.startsWith("--db-path=")) {
      const candidate = arg.slice("--db-path=".length).trim();
      if (candidate) out.dbPath = candidate;
    } else if (arg === "--min-nudges") {
      out.minNudges = readPositiveInt(args[i + 1], out.minNudges);
      i += 1;
    } else if (arg.startsWith("--min-nudges=")) {
      out.minNudges = readPositiveInt(arg.slice("--min-nudges=".length), out.minNudges);
    } else if (arg === "--poor-rate") {
      const parsed = Number(args[i + 1]);
      if (Number.isFinite(parsed) && parsed >= 0 && parsed <= 1) out.poorRate = parsed;
      i += 1;
    } else if (arg.startsWith("--poor-rate=")) {
      const parsed = Number(arg.slice("--poor-rate=".length));
      if (Number.isFinite(parsed) && parsed >= 0 && parsed <= 1) out.poorRate = parsed;
    } else if (arg === "--json") {
      out.json = true;
    } else if (arg === "--decisions") {
      out.decisions = true;
    } else if (arg === "--help" || arg === "-h") {
      console.log(`Usage: node evaluate-nudge-health.mjs [--days N] [--db-path PATH] [--json] [--decisions] [--min-nudges N] [--poor-rate 0..1]\n  --days        Funnel window in days (default: ${DEFAULT_DAYS})\n  --db-path     SQLite database (default: ${DEFAULT_DB_PATH})\n  --json        Print JSON output\n  --decisions   Show router decision reasons instead of funnel data\n  --min-nudges  Frequent-nudge threshold (default: ${DEFAULT_MIN_NUDGES})\n  --poor-rate   Poor-conversion ceiling (default: ${DEFAULT_POOR_RATE})`);
      process.exit(0);
    }
  }
  return out;
}

function openDb(path) {
  try {
    const sqlite3 = require("better-sqlite3");
    const BetterSqlite3 = sqlite3?.default || sqlite3;
    const db = new BetterSqlite3(path, { readonly: true, fileMustExist: true });
    return {
      all: (sql, params) => (params === undefined ? db.prepare(sql).all() : db.prepare(sql).all(params)),
      close: () => db.close(),
    };
  } catch {
    const sqlite = require("node:sqlite");
    const db = new sqlite.DatabaseSync(path, { readOnly: true });
    return {
      all: (sql, params) => (params === undefined ? db.prepare(sql).all() : db.prepare(sql).all(params)),
      close: () => db.close(),
    };
  }
}

function tableExists(db, tableName) {
  try {
    return db.all(`SELECT name FROM sqlite_master WHERE type='table' AND name=@table_name`, { table_name: tableName }).length > 0;
  } catch {
    return false;
  }
}

function tableColumns(db, tableName) {
  try {
    return new Set(db.all(`PRAGMA table_info(${tableName})`).map((row) => String(row.name)));
  } catch {
    return new Set();
  }
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

function buildDecisionReport(db, windowStart) {
  const reasonSummary = db.all(`
    SELECT decision, reason, COUNT(*) AS count
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
    SELECT timestamp, target_type, decision, reason, candidate_count, available_count,
      scored_count, selected_count, selected_skill_keys, top_candidates
    FROM skill_router_decisions
    WHERE timestamp >= @window_start
    ORDER BY id DESC LIMIT 20
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

function median(values) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
}

function rate(count, total) {
  return total > 0 ? count / total : null;
}

function workflowHash(value) {
  return createHash("sha256").update(String(value)).digest("hex").slice(0, 12);
}

function aggregateSkills(rows) {
  const bySkill = new Map();
  for (const row of rows) {
    const key = String(row.skill_key || row.skill_name || "unknown");
    const current = bySkill.get(key) || {
      skill_key: key,
      skill_name: String(row.skill_name || key),
      nudge_count: 0,
      pending_count: 0,
      opened_count: 0,
      ignored_count: 0,
      failed_open_count: 0,
      unknown_count: 0,
      resources_used_count: 0,
      openLatencies: [],
    };
    current.nudge_count += 1;
    if (row.outcome === "nudged") current.pending_count += 1;
    else if (row.outcome === "opened") current.opened_count += 1;
    else if (row.outcome === "ignored") current.ignored_count += 1;
    else if (row.outcome === "failed_open") current.failed_open_count += 1;
    else if (row.outcome === "unknown") current.unknown_count += 1;
    current.resources_used_count += Number(row.resources_used_count) || 0;
    if (row.outcome === "opened" && row.open_latency_ms != null && Number.isFinite(Number(row.open_latency_ms))) {
      current.openLatencies.push(Number(row.open_latency_ms));
    }
    bySkill.set(key, current);
  }
  return [...bySkill.values()].map(({ openLatencies, ...row }) => ({
    ...row,
    settled_count: row.nudge_count - row.pending_count,
    opened_rate: rate(row.opened_count, row.nudge_count - row.pending_count),
    ignored_rate: rate(row.ignored_count, row.nudge_count - row.pending_count),
    median_open_ms: median(openLatencies),
  })).sort((a, b) => b.nudge_count - a.nudge_count || a.skill_name.localeCompare(b.skill_name));
}

function buildFunnelReport(db, args, now = Date.now()) {
  const windowStart = new Date(now - args.days * 86400000).toISOString();
  const rows = db.all(`
    SELECT nudge_id, skill_name, COALESCE(skill_key, skill_name) AS skill_key,
      outcome, open_latency_ms, resources_used_count, timestamp, session_key, session_id
    FROM skill_nudges
    WHERE timestamp >= @window_start AND nudge_id IS NOT NULL AND outcome IS NOT NULL
    ORDER BY id ASC
  `, { window_start: windowStart }).map((row) => ({
    ...row,
    outcome: String(row.outcome || "unknown"),
  }));
  const skills = aggregateSkills(rows);
  const total = skills.reduce((sum, row) => sum + row.nudge_count, 0);
  const pending = skills.reduce((sum, row) => sum + row.pending_count, 0);
  const settled = total - pending;
  const summary = {
    nudges: total,
    pending,
    settled,
    opened: skills.reduce((sum, row) => sum + row.opened_count, 0),
    ignored: skills.reduce((sum, row) => sum + row.ignored_count, 0),
    failed_open: skills.reduce((sum, row) => sum + row.failed_open_count, 0),
    unknown: skills.reduce((sum, row) => sum + row.unknown_count, 0),
  };
  summary.opened_rate = rate(summary.opened, settled);
  summary.ignored_rate = rate(summary.ignored, settled);
  summary.median_open_ms = median(rows
    .filter((row) => row.outcome === "opened" && row.open_latency_ms != null && Number.isFinite(Number(row.open_latency_ms)))
    .map((row) => Number(row.open_latency_ms)));

  const historical = db.all(`SELECT COUNT(*) AS count FROM skill_nudges WHERE nudge_id IS NULL OR outcome IS NULL`);
  const historicalUnclassified = Number(historical[0]?.count) || 0;
  const catalogRows = tableExists(db, "skill_router_catalog")
    ? db.all(`SELECT skill_key, skill_name, skill_path, last_seen_at FROM skill_router_catalog ORDER BY skill_name, skill_key`)
    : [];
  const catalogByKey = new Map();
  for (const skill of catalogRows) {
    const key = String(skill.skill_key);
    const existing = catalogByKey.get(key);
    if (!existing || String(skill.last_seen_at) > String(existing.last_seen_at)) catalogByKey.set(key, skill);
  }
  const catalog = [...catalogByKey.values()];
  const zeroWindowStart = new Date(now - 90 * 86400000).toISOString();
  const recentNudgesForZeroWindows = db.all(`
    SELECT COALESCE(skill_key, skill_name) AS skill_key, timestamp
    FROM skill_nudges
    WHERE timestamp >= @window_start
  `, { window_start: zeroWindowStart });
  const zeroNudges = {};
  for (const days of [7, 30, 60, 90]) {
    const cutoff = now - days * 86400000;
    zeroNudges[days] = catalog.filter((skill) => !recentNudgesForZeroWindows.some((row) => {
      const timestamp = Date.parse(String(row.timestamp || ""));
      return timestamp >= cutoff && String(row.skill_key) === String(skill.skill_key);
    })).map((skill) => ({
      skill_key: String(skill.skill_key),
      skill_name: String(skill.skill_name),
      skill_path: String(skill.skill_path),
    }));
  }

  const frequentPoorConversion = skills.filter((row) => (
    row.settled_count >= args.minNudges && (row.opened_rate ?? 0) < args.poorRate
  ));
  const repeated = new Map();
  for (const row of rows.filter((entry) => entry.outcome === "ignored")) {
    const workflow = row.session_key || row.session_id;
    if (!workflow) continue;
    const key = `${workflow}\0${row.skill_key}`;
    const current = repeated.get(key) || {
      workflow_hash: workflowHash(workflow),
      skill_key: String(row.skill_key),
      skill_name: String(row.skill_name || row.skill_key),
      ignored_count: 0,
    };
    current.ignored_count += 1;
    repeated.set(key, current);
  }
  const repeatedIgnored = [...repeated.values()]
    .filter((row) => row.ignored_count >= 2)
    .sort((a, b) => b.ignored_count - a.ignored_count || a.skill_name.localeCompare(b.skill_name));

  return {
    schema: "funnel-v2",
    window_days: args.days,
    historical_unclassified_count: historicalUnclassified,
    summary,
    skills,
    zero_nudges: zeroNudges,
    frequent_poor_conversion: frequentPoorConversion,
    repeated_ignored: repeatedIgnored,
    thresholds: { min_nudges: args.minNudges, poor_rate: args.poorRate },
  };
}

function buildLegacyReport(db, windowStart) {
  const columns = tableColumns(db, "skill_nudges");
  const runId = columns.has("run_id") ? "n.run_id" : "NULL";
  const skillIdentity = columns.has("skill_key") ? "COALESCE(n.skill_key, n.skill_name)" : "n.skill_name";
  const hasEvents = tableExists(db, "skill_events");
  const sql = hasEvents ? `
    SELECT n.skill_name, COUNT(*) AS nudge_count,
      SUM(CASE WHEN EXISTS(
        SELECT 1 FROM skill_events e
        WHERE (( ${runId} IS NOT NULL AND e.run_id = ${runId})
          OR (${runId} IS NULL AND n.session_key IS NOT NULL AND e.session_key = n.session_key)
          OR (${runId} IS NULL AND n.session_key IS NULL AND n.session_id IS NOT NULL AND e.session_id = n.session_id))
          AND (LOWER(e.skill_name) = LOWER(${skillIdentity}) OR LOWER(e.skill_name) = LOWER(n.skill_name))
          AND e.type = 'skill_file_read' AND datetime(e.ts) >= datetime(n.timestamp)
          AND datetime(e.ts) < datetime(n.timestamp, '+1 hour')
      ) THEN 1 ELSE 0 END) AS read_count
    FROM skill_nudges n WHERE n.timestamp >= @window_start
    GROUP BY n.skill_name ORDER BY nudge_count DESC, n.skill_name ASC
  ` : `
    SELECT n.skill_name, COUNT(*) AS nudge_count, NULL AS read_count
    FROM skill_nudges n WHERE n.timestamp >= @window_start
    GROUP BY n.skill_name ORDER BY nudge_count DESC, n.skill_name ASC
  `;
  return db.all(sql, { window_start: windowStart }).map((row) => ({
    skill_name: String(row.skill_name || ""),
    nudge_count: Number(row.nudge_count) || 0,
    read_count: row.read_count == null ? null : Number(row.read_count) || 0,
    read_rate: row.read_count == null || Number(row.nudge_count) === 0 ? null : Number(row.read_count) / Number(row.nudge_count),
    legacy_estimate: true,
  }));
}

function formatRate(value) {
  return value == null || !Number.isFinite(value) ? "N/A" : `${(value * 100).toFixed(1)}%`;
}

function formatDuration(value) {
  if (value == null || !Number.isFinite(value)) return "N/A";
  if (value < 1000) return `${Math.round(value)}ms`;
  return `${(value / 1000).toFixed(1)}s`;
}

function formatFunnelReport(report) {
  console.log(`Nudge funnel (${report.window_days} days)`);
  console.log(`Nudges ${report.summary.nudges} | Pending ${report.summary.pending} | Settled ${report.summary.settled} | Opened ${report.summary.opened} (${formatRate(report.summary.opened_rate)}) | Ignored ${report.summary.ignored} (${formatRate(report.summary.ignored_rate)}) | Failed ${report.summary.failed_open} | Unknown ${report.summary.unknown} | Median open ${formatDuration(report.summary.median_open_ms)}`);
  if (report.historical_unclassified_count) {
    console.log(`Historical unclassified nudges: ${report.historical_unclassified_count} (intentionally excluded)`);
  }
  console.log("");
  console.log("Skill | Nudges | Pending | Settled | Opened | Open rate | Ignored | Ignore rate | Failed | Unknown | Median open | Resources");
  for (const row of report.skills) {
    console.log(`${row.skill_name} | ${row.nudge_count} | ${row.pending_count} | ${row.settled_count} | ${row.opened_count} | ${formatRate(row.opened_rate)} | ${row.ignored_count} | ${formatRate(row.ignored_rate)} | ${row.failed_open_count} | ${row.unknown_count} | ${formatDuration(row.median_open_ms)} | ${row.resources_used_count}`);
  }
  for (const days of [7, 30, 60, 90]) {
    console.log(`Zero nudges in ${days}d: ${report.zero_nudges[days].map((row) => row.skill_name).join(", ") || "none"}`);
  }
  console.log(`Frequent poor conversion: ${report.frequent_poor_conversion.map((row) => `${row.skill_name} (${row.settled_count} settled, ${formatRate(row.opened_rate)})`).join(", ") || "none"}`);
  console.log(`Repeated ignored: ${report.repeated_ignored.map((row) => `${row.skill_name}@${row.workflow_hash} (${row.ignored_count})`).join(", ") || "none"}`);
}

function formatDecisionReport(report) {
  console.log("Router decision reasons");
  for (const row of report.reasonSummary) console.log(`${row.decision}/${row.reason}: ${row.count}`);
  if (report.recentDecisions.length) {
    console.log("\nRecent decisions");
    for (const row of report.recentDecisions) {
      const top = row.top_candidates.slice(0, 3).map((candidate) => `${candidate.skill_key || candidate.skill_name}:${candidate.score}`).join(", ");
      console.log(`${row.timestamp} ${row.target_type} ${row.decision}/${row.reason} selected=${row.selected_count} candidates=${row.candidate_count}${top ? ` top=[${top}]` : ""}`);
    }
  }
}

function main() {
  const args = parseArgs();
  const dbPath = resolveHomePath(args.dbPath);
  const windowStart = new Date(Date.now() - args.days * 86400000).toISOString();
  let db;
  try {
    db = openDb(dbPath);
  } catch (error) {
    console.error(`failed to open db: ${String(error)}`);
    process.exit(1);
  }
  try {
    if (args.decisions) {
      if (!tableExists(db, "skill_router_decisions")) {
        console.log("No skill_router_decisions table found. Run the plugin with router observability enabled.");
        return;
      }
      const report = buildDecisionReport(db, windowStart);
      console.log(args.json ? JSON.stringify(report, null, 2) : "");
      if (!args.json) formatDecisionReport(report);
      return;
    }
    if (!tableExists(db, "skill_nudges")) {
      console.log("No skill_nudges table found. Run the plugin long enough to populate nudges.");
      return;
    }
    const columns = tableColumns(db, "skill_nudges");
    if (!columns.has("nudge_id") || !columns.has("outcome")) {
      const legacy = buildLegacyReport(db, windowStart);
      if (args.json) console.log(JSON.stringify(legacy, null, 2));
      else {
        console.log("Legacy estimate only; run the current plugin to initialize forward-looking funnel tracking.");
        for (const row of legacy) console.log(`${row.skill_name}: nudges=${row.nudge_count} estimated_reads=${row.read_count ?? "N/A"} estimated_rate=${formatRate(row.read_rate)}`);
      }
      return;
    }
    const report = buildFunnelReport(db, args);
    if (args.json) console.log(JSON.stringify(report, null, 2));
    else formatFunnelReport(report);
  } finally {
    db.close();
  }
}

main();
