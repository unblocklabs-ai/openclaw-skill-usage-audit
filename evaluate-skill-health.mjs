#!/usr/bin/env node
/**
 * Daily skill health evaluator for skill-usage-audit telemetry.
 *
 * Reads skill_executions from $SKILL_USAGE_AUDIT_DB_PATH (or ~/.openclaw/audits/skill-usage.db), computes
 * per-skill health metrics, writes snapshots to SQLite, updates skills.status,
 * and renders a human-readable markdown report.
 */

import { mkdir } from "node:fs/promises";
import { readFileSync } from "node:fs";
import { dirname, resolve, join, basename, sep } from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const DEFAULT_DB_PATH = process.env.SKILL_USAGE_AUDIT_DB_PATH || "~/.openclaw/audits/skill-usage.db";
const DEFAULT_REPORT_DIR = resolve(__dirname, "../../reports/skill-health");

const DEFAULT_WINDOW_DAYS = 7;
const DEFAULT_STABLE_MIN_USAGE = 10;
const DEFAULT_EXPERIMENTAL_MIN_USAGE = 10;
const DEFAULT_DEGRADED_SAMPLE_MIN = 5;
const DEFAULT_DEGRADED_MECHANICAL_FAIL_RATE = 0.2;
const DEFAULT_DEGRADED_IMPLIED_NEG_RATE = 0.3;
const DEFAULT_UNDERUSED_MAX = 2;

function resolveHome(pathLike) {
  if (!pathLike || typeof pathLike !== "string") return pathLike;
  if (!pathLike.startsWith("~")) return resolve(pathLike);
  const home = process.env.HOME || process.env.USERPROFILE;
  if (!home) return resolve(pathLike);
  return resolve(home, pathLike.slice(2));
}

function parseArgs() {
  const args = process.argv.slice(2);
  const out = {
    dbPath: DEFAULT_DB_PATH,
    reportDir: DEFAULT_REPORT_DIR,
    windowDays: DEFAULT_WINDOW_DAYS,
    stableMinUsage: DEFAULT_STABLE_MIN_USAGE,
    experimentalMinUsage: DEFAULT_EXPERIMENTAL_MIN_USAGE,
    degradedSampleMin: DEFAULT_DEGRADED_SAMPLE_MIN,
    degradedMechanicalRate: DEFAULT_DEGRADED_MECHANICAL_FAIL_RATE,
    degradedImpliedRate: DEFAULT_DEGRADED_IMPLIED_NEG_RATE,
    underusedMax: DEFAULT_UNDERUSED_MAX,
    writeDb: true,
    writeReport: true,
    verbose: false,
    includeFilesystemSkills: true,
  };

  const take = (idx) => {
    const value = args[idx + 1];
    if (value === undefined) {
      throw new Error(`Missing value for ${args[idx]}`);
    }
    return value;
  };

  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i];

    if (arg === "--db-path" || arg === "--db") {
      out.dbPath = take(i);
      i += 1;
      continue;
    }
    if (arg === "--report-dir") {
      out.reportDir = take(i);
      i += 1;
      continue;
    }
    if (arg.startsWith("--db-path=")) {
      out.dbPath = arg.split("=")[1];
      continue;
    }
    if (arg.startsWith("--report-dir=")) {
      out.reportDir = arg.split("=")[1];
      continue;
    }
    if (arg === "--window-days") {
      out.windowDays = Number.parseInt(take(i), 10);
      i += 1;
      continue;
    }
    if (arg === "--windowDays") {
      out.windowDays = Number.parseInt(take(i), 10);
      i += 1;
      continue;
    }
    if (arg === "--stable-min-usage") {
      out.stableMinUsage = Number.parseInt(take(i), 10);
      i += 1;
      continue;
    }
    if (arg === "--experimental-min-usage") {
      out.experimentalMinUsage = Number.parseInt(take(i), 10);
      i += 1;
      continue;
    }
    if (arg === "--degraded-sample-min") {
      out.degradedSampleMin = Number.parseInt(take(i), 10);
      i += 1;
      continue;
    }
    if (arg === "--degraded-mechanical-rate") {
      out.degradedMechanicalRate = Number.parseFloat(take(i));
      i += 1;
      continue;
    }
    if (arg === "--degraded-implied-rate") {
      out.degradedImpliedRate = Number.parseFloat(take(i));
      i += 1;
      continue;
    }
    if (arg === "--underused-max") {
      out.underusedMax = Number.parseInt(take(i), 10);
      i += 1;
      continue;
    }
    if (arg === "--no-update-status") {
      out.writeDb = false;
      continue;
    }
    if (arg === "--no-report") {
      out.writeReport = false;
      continue;
    }
    if (arg === "--no-filesystem-scan") {
      out.includeFilesystemSkills = false;
      continue;
    }
    if (arg === "--verbose") {
      out.verbose = true;
      continue;
    }

    if (arg === "--help" || arg === "-h") {
      printUsage();
      process.exit(0);
    }

    throw new Error(`Unknown arg: ${arg}`);
  }

  if (!Number.isFinite(out.windowDays) || out.windowDays <= 0) out.windowDays = DEFAULT_WINDOW_DAYS;
  if (!Number.isFinite(out.stableMinUsage) || out.stableMinUsage <= 0) out.stableMinUsage = DEFAULT_STABLE_MIN_USAGE;
  if (!Number.isFinite(out.experimentalMinUsage) || out.experimentalMinUsage <= 0) out.experimentalMinUsage = DEFAULT_EXPERIMENTAL_MIN_USAGE;
  if (!Number.isFinite(out.degradedSampleMin) || out.degradedSampleMin <= 0) out.degradedSampleMin = DEFAULT_DEGRADED_SAMPLE_MIN;
  if (!Number.isFinite(out.degradedMechanicalRate) || out.degradedMechanicalRate < 0) out.degradedMechanicalRate = DEFAULT_DEGRADED_MECHANICAL_FAIL_RATE;
  if (!Number.isFinite(out.degradedImpliedRate) || out.degradedImpliedRate < 0) out.degradedImpliedRate = DEFAULT_DEGRADED_IMPLIED_NEG_RATE;
  if (!Number.isFinite(out.underusedMax) || out.underusedMax < 0) out.underusedMax = DEFAULT_UNDERUSED_MAX;

  return out;
}

function printUsage() {
  const usage = `
Usage: node evaluate-skill-health.mjs [options]

Options:
  --db-path <path>            SQLite DB path (default: ${DEFAULT_DB_PATH})
  --report-dir <path>         Report output folder (default: ${DEFAULT_REPORT_DIR})
  --window-days <n>           Review window in days (default: ${DEFAULT_WINDOW_DAYS})
  --stable-min-usage <n>      Minimum usage for stable classification (default: ${DEFAULT_STABLE_MIN_USAGE})
  --experimental-min-usage <n> Current-version usage threshold for experimental status (default: ${DEFAULT_EXPERIMENTAL_MIN_USAGE})
  --degraded-sample-min <n>   Minimum sample for degraded checks (default: ${DEFAULT_DEGRADED_SAMPLE_MIN})
  --degraded-mechanical-rate <r>  Mechanical fail threshold (default: ${DEFAULT_DEGRADED_MECHANICAL_FAIL_RATE})
  --degraded-implied-rate <r>     Implied negative threshold (default: ${DEFAULT_DEGRADED_IMPLIED_NEG_RATE})
  --underused-max <n>         Underused upper bound (default: ${DEFAULT_UNDERUSED_MAX})
  --no-update-status          Skip updating skills.status
  --no-report                 Skip writing markdown reports
  --no-filesystem-scan        Skip scanning workspace/.openclaw for SKILL.md
  --verbose                   Print extra debug lines
  -h, --help                  Show this help
`;
  console.log(usage.trimStart());
}

function buildWorkspaceRoots() {
  const workspaceRoot = resolve(__dirname, "../..");
  const home = process.env.HOME || process.env.USERPROFILE;
  if (!home) {
    return {
      workspaceSkills: resolve(workspaceRoot, "skills"),
      openclawSkills: undefined,
      extensionSkillsDir: undefined,
      workspaceRoot,
    };
  }

  return {
    workspaceSkills: resolve(workspaceRoot, "skills"),
    openclawSkills: resolve(home, ".openclaw", "skills"),
    extensionSkillsDir: resolve(home, ".openclaw", "extensions"),
    workspaceRoot,
  };
}

function inferSkillNameFromPath(skillPath) {
  const dir = dirname(skillPath);
  const base = basename(skillPath);
  if (base.toLowerCase() === "skill.md") return basename(dir);
  return basename(skillPath);
}

async function listSkillFiles(root, maxDepth = 5) {
  if (!root) return [];
  const out = [];
  const seen = new Set();

  const walk = async (dir, depth) => {
    if (depth > maxDepth) return;
    let entries;
    try {
      entries = await import("node:fs/promises").then((m) => m.readdir(dir, { withFileTypes: true }));
    } catch {
      return;
    }

    for (const entry of entries) {
      if (!entry.isDirectory() && !entry.isFile()) continue;
      const next = resolve(dir, entry.name);
      const lower = entry.name.toLowerCase();

      if (entry.isDirectory()) {
        if (lower === ".git" || lower === "node_modules" || lower === ".openclaw") {
          continue;
        }
        if (next.includes(`${sep}dist${sep}`) || next.includes(`${sep}build${sep}`)) {
          continue;
        }
        await walk(next, depth + 1);
        continue;
      }

      if (lower === "skill.md") {
        if (!seen.has(next)) {
          seen.add(next);
          out.push(next);
        }
      }
    }
  };

  await walk(root, 0);
  return out;
}

async function collectFilesystemSkills(roots, include) {
  if (!include) return new Map();

  const skills = new Map();

  const add = (path) => {
    const skillName = inferSkillNameFromPath(path);
    const current = skills.get(skillName);
    const candidate = { skillName, path };
    if (!current) {
      skills.set(skillName, candidate);
      return;
    }

    // Prefer workspace skills over extension/bundled when duplicates exist.
    if (!current.path.includes("/skills/") && path.includes("/skills/")) {
      skills.set(skillName, candidate);
    }
  };

  for (const root of [roots.workspaceSkills, roots.openclawSkills, roots.extensionSkillsDir]) {
    const files = await listSkillFiles(root, root === roots.extensionSkillsDir ? 3 : 3);
    for (const f of files) add(f);
  }

  return skills;
}

function createSqliteBackend(dbPath) {
  const normalized = resolveHome(dbPath);
  const kind = (name) => ({
    better: "better-sqlite3",
    node: "node:sqlite",
    missing: "none",
  }[name] || "unknown");

  return (async () => {
    try {
      const sqlite = await import("better-sqlite3");
      const BetterSqlite3 = sqlite.default || sqlite;
      const db = new BetterSqlite3(normalized);
      db.pragma("journal_mode = WAL");
      db.pragma("foreign_keys = ON");
      return {
        kind: kind("better"),
        db,
        exec: (sql) => db.exec(sql),
        prepare: (sql) => {
          const stmt = db.prepare(sql);
          return {
            run: (params) => Array.isArray(params) ? stmt.run(...params) : stmt.run(params),
            get: (params) => (Array.isArray(params) ? stmt.get(...params) : stmt.get(params)),
            all: (params) => (Array.isArray(params) ? stmt.all(...params) : stmt.all(params)),
          };
        },
        close: () => db.close(),
      };
    } catch {
      // fallback to node:sqlite
    }

    try {
      const sqlite = await import("node:sqlite");
      if (!sqlite?.DatabaseSync) throw new Error("DatabaseSync missing");
      const db = new sqlite.DatabaseSync(normalized);
      db.exec("PRAGMA journal_mode = WAL;");
      db.exec("PRAGMA foreign_keys = ON;");
      return {
        kind: kind("node"),
        db,
        exec: (sql) => db.exec(sql),
        prepare: (sql) => {
          const stmt = db.prepare(sql);
          return {
            run: (params) => Array.isArray(params) ? stmt.run(...params) : stmt.run(params),
            get: (params) => (Array.isArray(params) ? stmt.get(...params) : stmt.get(params)),
            all: (params) => (Array.isArray(params) ? stmt.all(...params) : stmt.all(params)),
          };
        },
        close: () => db.close(),
      };
    } catch {
      throw new Error("No sqlite backend available (tried better-sqlite3 and node:sqlite)");
    }
  })();
}

function createSchema(db) {
  const createSql = `
    CREATE TABLE IF NOT EXISTS skill_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts TEXT NOT NULL,
      type TEXT NOT NULL,
      session_id TEXT,
      session_key TEXT,
      run_id TEXT,
      agent_id TEXT,
      channel_id TEXT,
      message_provider TEXT,
      tool_name TEXT,
      tool_call_id TEXT,
      params TEXT,
      duration_ms INTEGER,
      success INTEGER,
      error TEXT,
      skill_name TEXT,
      skill_path TEXT,
      skill_source TEXT,
      skill_block_count INTEGER,
      skill_block_names TEXT,
      skill_block_locations TEXT
    );

    CREATE TABLE IF NOT EXISTS skill_versions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      skill_name TEXT NOT NULL,
      skill_path TEXT NOT NULL,
      version_hash TEXT NOT NULL,
      first_seen_at TEXT NOT NULL,
      notes TEXT,
      UNIQUE(skill_name, skill_path, version_hash)
    );

    CREATE TABLE IF NOT EXISTS skills (
      skill_name TEXT PRIMARY KEY,
      skill_path TEXT NOT NULL,
      current_version_hash TEXT,
      status TEXT DEFAULT 'stable',
      last_modified_at TEXT,
      last_used_at TEXT,
      total_executions INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS skill_executions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts TEXT NOT NULL,
      session_key TEXT,
      run_id TEXT,
      skill_name TEXT NOT NULL,
      skill_path TEXT NOT NULL,
      version_hash TEXT,
      intent_context TEXT,
      mechanical_success INTEGER,
      semantic_outcome TEXT,
      followup_messages TEXT,
      implied_outcome TEXT,
      error TEXT,
      duration_ms INTEGER,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS skill_feedback (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      execution_id INTEGER REFERENCES skill_executions(id),
      source TEXT,
      label TEXT,
      notes TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS skill_health_snapshots (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts TEXT NOT NULL,
      skill_name TEXT NOT NULL,
      version_hash TEXT,
      usage_count INTEGER DEFAULT 0,
      mechanical_failure_rate REAL DEFAULT 0,
      implied_negative_rate REAL DEFAULT 0,
      status_recommendation TEXT,
      notes TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_skill_executions_name_ts
      ON skill_executions(skill_name, ts);
    CREATE INDEX IF NOT EXISTS idx_skill_health_snapshots_name_ts
      ON skill_health_snapshots(skill_name, ts);
  `;

  db.exec(createSql);
}

function percent(value) {
  if (!Number.isFinite(value)) return "0.00%";
  return `${(value * 100).toFixed(2)}%`;
}

function toNumber(value, fallback = 0) {
  const parsed = Number.parseFloat(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function clamp(value) {
  if (!Number.isFinite(value)) return 0;
  if (value < 0) return 0;
  if (value > 1) return 1;
  return value;
}

function computeVersionRate(summary, metricField, defaultIfZero = 0) {
  if (!summary) return defaultIfZero;
  const sample = metricField === "implied"
    ? summary.impliedSample
    : summary.mechanicalSample;
  const failures = metricField === "implied"
    ? summary.impliedNegative
    : summary.mechanicalFailures;
  if (!sample || sample <= 0) return defaultIfZero;
  return clamp(failures / sample);
}

function dedupeLine(s) {
  return s.replace(/\s+/g, " ").trim();
}

function evaluateSkills({
  executionsBySkill,
  skillMeta,
  options,
}) {
  const reportBuckets = {
    stable: [],
    experimental: [],
    degraded: [],
    underused: [],
    unused: [],
  };

  const versionCompareNotes = [];
  const rowsToPersist = [];

  for (const skill of skillMeta.values()) {
    const {
      skillName,
      skillPath,
      currentVersionHash,
      dbStatus,
    } = skill;

    const st = executionsBySkill.get(skillName) || {
      usageCount: 0,
      versions: new Map(),
      latestTs: null,
    };

    const versions = [...st.versions.entries()].map(([versionHash, summary]) => ({
      versionHash,
      usage: summary.usage,
      mechanicalSample: summary.mechanicalSample,
      mechanicalFailures: summary.mechanicalFailures,
      impliedSample: summary.impliedSample,
      impliedNegative: summary.impliedNegative,
      mechanicalRate: computeVersionRate({
        mechanicalSample: summary.mechanicalSample,
        mechanicalFailures: summary.mechanicalFailures,
      }, "mechanical", 0),
      impliedRate: computeVersionRate({
        impliedSample: summary.impliedSample,
        impliedNegative: summary.impliedNegative,
      }, "implied", 0),
    }));


    const totalMechanic = versions.reduce((acc, row) => acc + row.mechanicalFailures, 0);
    const totalMechSample = versions.reduce((acc, row) => acc + row.mechanicalSample, 0);
    const totalImpliedNeg = versions.reduce((acc, row) => acc + row.impliedNegative, 0);
    const totalImpliedSample = versions.reduce((acc, row) => acc + row.impliedSample, 0);

    const totalMechanicalRate = totalMechSample ? totalMechanic / totalMechSample : 0;
    const totalImpliedRate = totalImpliedSample ? totalImpliedNeg / totalImpliedSample : 0;

    const knownVersionsSorted = [...versions].sort((a, b) => {
      if (a.versionHash === (currentVersionHash || "")) return -1;
      if (b.versionHash === (currentVersionHash || "")) return 1;
      return b.usage - a.usage;
    });

    const current =
      knownVersionsSorted.find((v) => v.versionHash === currentVersionHash) ||
      knownVersionsSorted[0];

    const currentUsage = current?.usage || 0;
    const currentMechRate = current?.mechanicalRate ?? totalMechanicalRate;
    const currentImpliedRate = current?.impliedRate ?? totalImpliedRate;
    const currentMechSample = current?.mechanicalSample || 0;
    const currentImpliedSample = current?.impliedSample || 0;

    const rec = (() => {
      if (st.usageCount === 0) return "unused";
      if (st.usageCount <= options.underusedMax) return "underused";
      if (currentUsage < options.experimentalMinUsage) return "experimental";

      const isDegraded =
        (currentMechSample >= options.degradedSampleMin && currentMechRate > options.degradedMechanicalRate) ||
        (currentImpliedSample >= options.degradedSampleMin && currentImpliedRate > options.degradedImpliedRate);

      if (isDegraded) return "degraded";
      if (st.usageCount >= options.stableMinUsage && currentMechRate <= options.degradedMechanicalRate && currentImpliedRate <= options.degradedImpliedRate) {
        return "stable";
      }
      return "experimental";
    })();

    const dbStatusUpdate = rec === "unused"
      ? "unused"
      : rec === "degraded"
        ? "degraded"
        : rec === "stable"
          ? "stable"
          : "experimental";

    const versionNotes = [];

    if (versions.length > 1) {
      versionNotes.push("Multiple versions observed in window:");
      const sortedForNotes = [...versions].sort((a, b) => b.usage - a.usage);
      for (const v of sortedForNotes) {
        const short = v.versionHash ? v.versionHash.slice(0, 10) : "(no-hash)";
        versionNotes.push(
          `- ${short} usage=${v.usage} fail=${percent(v.mechanicalRate)} neg=${percent(v.impliedRate)}`,
        );
      }
      if (currentVersionHash) {
        const curr = versions.find((x) => x.versionHash === currentVersionHash);
        const prior = versions.find((x) => x.versionHash !== currentVersionHash && x.usage > 0);
        if (curr && prior) {
          versionCompareNotes.push(
            `${skillName}: current ${curr.versionHash?.slice(0, 8) || "(no-hash)"} (${curr.usage} runs)` +
              ` vs prior ${prior.versionHash?.slice(0, 8) || "(no-hash)"} (${prior.usage} runs)`,
          );
        }
      }
    }

    const rowNote = versionNotes.length ? `${versionNotes.join(" ")}` : "single version in window";

    rowsToPersist.push({
      skillName,
      versionHash: current?.versionHash || null,
      usageCount: st.usageCount,
      mechanicalFailureRate: currentMechRate,
      impliedNegativeRate: currentImpliedRate,
      statusRecommendation: dbStatusUpdate,
      notes: dedupeLine(rowNote),
      dbStatus,
      previousStatus: dbStatus,
      recommendation: rec,
      mechanicalFailureRateOverall: totalMechanicalRate,
      impliedNegativeRateOverall: totalImpliedRate,
      latestTs: st.latestTs,
      skillPath,
    });

    if (rec === "stable") {
      reportBuckets.stable.push(rowsToPersist.at(-1));
    } else if (rec === "degraded") {
      reportBuckets.degraded.push(rowsToPersist.at(-1));
    } else if (rec === "underused") {
      reportBuckets.underused.push(rowsToPersist.at(-1));
    } else if (rec === "unused") {
      reportBuckets.unused.push(rowsToPersist.at(-1));
    } else {
      reportBuckets.experimental.push(rowsToPersist.at(-1));
    }
  }

  return { reportBuckets, versionCompareNotes, rowsToPersist };
}

function buildReport({
  at,
  windowStart,
  windowEnd,
  options,
  buckets,
  versionCompareNotes,
  rows,
  rowsUpdated,
}) {
  const totalSkills = rows.length;
  const stable = buckets.stable.length;
  const experimental = buckets.experimental.length;
  const degraded = buckets.degraded.length;
  const underused = buckets.underused.length;
  const unused = buckets.unused.length;

  const formatLine = (row) => {
    const currentVersion = row.versionHash ? row.versionHash.slice(0, 10) : "(none)";
    const status = row.recommendation;
    const lines = [
      `- **${row.skillName}**`,
      `  - status: ${status}`,
      `  - usage: ${row.usageCount}`,
      `  - mechanical failure: ${percent(row.mechanicalFailureRate)}`,
      `  - implied negative: ${percent(row.impliedNegativeRate)}`,
      `  - current version: ${currentVersion}`,
    ];
    if (row.dbStatus) lines.push(`  - previous db status: ${row.dbStatus}`);
    if (row.dbStatus !== row.statusRecommendation) {
      lines.push(`  - db status update: ${row.statusRecommendation}`);
    }
    return lines.join("\n");
  };

  const topFollowups = [];
  if (buckets.degraded.length) {
    topFollowups.push("Investigate degraded skills and check for recent regressions or dependency/environment drift.");
  }
  if (buckets.underused.length || buckets.unused.length) {
    topFollowups.push("Review underused/unused skills for possible retirement, documentation refresh, or re-scoping.");
  }
  if (buckets.experimental.length) {
    topFollowups.push("Keep experimental skills monitored; enforce minimum samples before promoting to stable.");
  }
  if (!topFollowups.length) {
    topFollowups.push("No immediate follow-ups from current threshold set.");
  }

  const section = (title, items) => {
    if (!items.length) return `\n## ${title}\n\n- _No items_.\n`;
    return `\n## ${title}\n\n${items.map(formatLine).join("\n")}\n`;
  };

  const versionNotesSection = versionCompareNotes.length
    ? `\n## Version comparison notes\n\n${versionCompareNotes.map((line) => `- ${line}`).join("\n")}\n`
    : "\n## Version comparison notes\n\n- _No multi-version comparison opportunities in this window._\n";

  const updatedRowCount = rowsUpdated.length;

  return `# Skill Health Report\n\n` +
    `Generated: ${at.toISOString()}\n` +
    `Window: ${windowStart} to ${windowEnd} (${Math.max(0, Math.ceil((new Date(windowEnd) - new Date(windowStart)) / 86400000))} day(s))\n` +
    `Database: ${options.dbPath}\n` +
    `Report mode: deterministic / no-LLM\n` +
    `\n## Summary counts\n` +
    `- skills evaluated: ${totalSkills}\n` +
    `- stable: ${stable}\n` +
    `- experimental: ${experimental}\n` +
    `- degraded: ${degraded}\n` +
    `- underused: ${underused}\n` +
    `- unused: ${unused}\n` +
    `- snapshots written: ${updatedRowCount}\n` +
    section("Stable skills", buckets.stable) +
    section("Experimental skills", buckets.experimental) +
    section("Degraded skills", buckets.degraded) +
    section("Underused / unused skills", [...buckets.underused, ...buckets.unused]) +
    versionNotesSection +
    `\n## Recommended follow-ups\n\n${topFollowups.map((x) => `- ${x}`).join("\n")}\n`;
}

(async () => {
  const options = parseArgs();
  options.dbPath = resolveHome(options.dbPath);
  options.reportDir = resolveHome(options.reportDir);

  if (options.verbose) {
    console.log("SKILL HEALTH: starting", JSON.stringify(options, null, 2));
  }

  const roots = buildWorkspaceRoots();
  const fsSkills = options.includeFilesystemSkills ? await collectFilesystemSkills(roots, true) : new Map();

  const db = await createSqliteBackend(options.dbPath);
  const startedAt = new Date().toISOString();
  try {
    createSchema(db);

    const getNow = new Date();
    const windowStart = new Date(getNow.getTime() - options.windowDays * 24 * 60 * 60 * 1000).toISOString();
    const windowEnd = getNow.toISOString();

    const executionRowsStmt = db.prepare(
      `SELECT skill_name, version_hash, mechanical_success, implied_outcome, ts
       FROM skill_executions
       WHERE ts >= ?
       ORDER BY skill_name, ts ASC`,
    );

    const skillRowsStmt = db.prepare(
      `SELECT skill_name, skill_path, current_version_hash, status
       FROM skills`,
    );

    const updateSkillStmt = db.prepare(
      `INSERT INTO skills (
        skill_name,
        skill_path,
        current_version_hash,
        status,
        last_modified_at,
        last_used_at,
        total_executions
      ) VALUES (
        ?, ?, ?, ?, ?, ?, 0
      ) ON CONFLICT(skill_name) DO UPDATE SET
        skill_path = excluded.skill_path,
        current_version_hash = COALESCE(excluded.current_version_hash, skills.current_version_hash),
        status = excluded.status,
        last_modified_at = excluded.last_modified_at,
        last_used_at = COALESCE(excluded.last_used_at, skills.last_used_at)
`,
    );

    const upsertSnapshotStmt = db.prepare(
      `INSERT INTO skill_health_snapshots (
        ts, skill_name, version_hash, usage_count,
        mechanical_failure_rate, implied_negative_rate,
        status_recommendation, notes
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
    );

    const executions = executionRowsStmt.all([windowStart]) || [];
    const executionsBySkill = new Map();

    for (const row of executions) {
      const skillName = row.skill_name;
      if (!skillName) continue;

      const existing = executionsBySkill.get(skillName) || {
        usageCount: 0,
        versions: new Map(),
        latestTs: null,
      };
      const versionKey = row.version_hash || "(no-version)";
      const versionSummary = existing.versions.get(versionKey) || {
        usage: 0,
        mechanicalSuccesses: 0,
        mechanicalFailures: 0,
        mechanicalSample: 0,
        impliedNegative: 0,
        impliedSample: 0,
      };

      existing.usageCount += 1;
      versionSummary.usage += 1;

      if (row.mechanical_success === 0 || row.mechanical_success === 1) {
        versionSummary.mechanicalSample += 1;
        if (row.mechanical_success === 0) versionSummary.mechanicalFailures += 1;
        else versionSummary.mechanicalSuccesses += 1;
      }

      if (row.implied_outcome) {
        versionSummary.impliedSample += 1;
        if (String(row.implied_outcome).toLowerCase() === "negative") {
          versionSummary.impliedNegative += 1;
        }
      }

      if (!existing.latestTs || String(row.ts) > String(existing.latestTs)) {
        existing.latestTs = String(row.ts);
      }

      existing.versions.set(versionKey, versionSummary);
      executionsBySkill.set(skillName, existing);
    }

    const skillMeta = new Map();

    for (const [skillName, data] of fsSkills.entries()) {
      skillMeta.set(skillName, {
        skillName,
        skillPath: data.path,
        currentVersionHash: null,
        dbStatus: null,
      });
    }

    for (const row of skillRowsStmt.all([]) || []) {
      const existing = skillMeta.get(row.skill_name) || {
        skillName: row.skill_name,
        skillPath: row.skill_path,
        currentVersionHash: null,
        dbStatus: null,
      };

      existing.currentVersionHash = row.current_version_hash || existing.currentVersionHash;
      existing.dbStatus = row.status || existing.dbStatus;
      if (row.skill_path) existing.skillPath = existing.skillPath || row.skill_path;
      skillMeta.set(row.skill_name, existing);
    }

    const executionOnlySkills = [...executionsBySkill.keys()];
    for (const skillName of executionOnlySkills) {
      if (!skillMeta.has(skillName)) {
        skillMeta.set(skillName, {
          skillName,
          skillPath: `(not-in-filesystem)`,
          currentVersionHash: null,
          dbStatus: null,
        });
      }
    }

    const dbNow = new Date().toISOString();
    const { reportBuckets, versionCompareNotes, rowsToPersist } = evaluateSkills({
      executionsBySkill,
      skillMeta,
      options,
    });

    const rowsUpdated = [];

    if (options.writeDb) {
      for (const row of rowsToPersist) {
        updateSkillStmt.run([
          row.skillName,
          row.skillPath,
          row.versionHash,
          row.statusRecommendation,
          dbNow,
          row.latestTs,
        ]);

        upsertSnapshotStmt.run([
          startedAt,
          row.skillName,
          row.versionHash,
          row.usageCount,
          clamp(toNumber(row.mechanicalFailureRate, 0)),
          clamp(toNumber(row.impliedNegativeRate, 0)),
          row.statusRecommendation,
          row.notes,
        ]);
        rowsUpdated.push(row.skillName);
      }
    }

    if (options.writeReport) {
      await mkdir(options.reportDir, { recursive: true });

      const now = new Date();
      const reportPath = join(options.reportDir, "latest.md");
      const datedPath = join(options.reportDir, now.toISOString().slice(0, 10));
      const report = buildReport({
        at: now,
        windowStart,
        windowEnd: startedAt,
        options,
        buckets: reportBuckets,
        versionCompareNotes,
        rows: rowsToPersist,
        rowsUpdated,
      });
      await import("node:fs/promises").then((m) => m.writeFile(reportPath, report, "utf8"));
      await import("node:fs/promises").then((m) => m.writeFile(`${datedPath}.md`, report, "utf8"));
    }

    const totalBuckets = Object.values(reportBuckets).reduce((acc, v) => acc + v.length, 0);
    const status = {
      skills: totalBuckets,
      stable: reportBuckets.stable.length,
      degraded: reportBuckets.degraded.length,
      experimental: reportBuckets.experimental.length,
      underused: reportBuckets.underused.length,
      unused: reportBuckets.unused.length,
    };

    console.log(`Evaluated ${status.skills} skills.`);
    console.log(`stable=${status.stable} experimental=${status.experimental} degraded=${status.degraded} underused=${status.underused} unused=${status.unused}`);
  } finally {
    db.close();
  }
})().catch((error) => {
  console.error("Skill health evaluator failed:", String(error?.message || error));
  process.exitCode = 1;
});

