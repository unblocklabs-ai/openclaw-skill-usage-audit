/**
 * Skill usage audit plugin
 */

import { mkdir, readdir, readFile, stat } from "node:fs/promises";
import { dirname, basename, resolve, join, relative } from "node:path";

import { createHash } from "node:crypto";

import type { OpenClawPluginApi } from "openclaw/plugin-sdk";


const DEFAULT_DB_PATH = "~/.openclaw/audits/skill-usage.db";
const DEFAULT_INCLUDE_TOOL_PARAMS = false;
const DEFAULT_CAPTURE_MESSAGE_CONTENT = false;
const DEFAULT_CONTEXT_WINDOW_SIZE = 5;
const DEFAULT_CONTEXT_TIMEOUT_MS = 60000;
const MAX_MESSAGE_LEN = 200;
const MAX_HISTORY_PER_SCOPE = 80;

const DEFAULT_REDACT_KEYS = [
  "token",
  "apikey",
  "api_key",
  "apiKey",
  "password",
  "passwd",
  "auth",
  "authorization",
  "secret",
  "secretToken",
  "refreshToken",
  "client_secret",
];

const SECRET_PATTERNS: RegExp[] = [
  /Authorization:\s*Bearer\s+[A-Za-z0-9._~+/=-]{10,}/gi,
  /\bsk-[A-Za-z0-9]{10,}/gi,
  /\bxox(?:b|p|o|s|r|u)-[A-Za-z0-9-]{10,}/gi,
  /\bgh[oprstuv]_[A-Za-z0-9]{20,}/gi,
  /(?:api[_-]?key|secret|token)[\s=:]\"?[A-Za-z0-9._~+/=-]{16,}\"?/gi,
];

const EMAIL_PATTERN = /[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/g;
const PHONE_PATTERN = /(?:(?:\+?\d{1,3}[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})(?:\b|$))/g;
const URL_WITH_QUERY_PATTERN = /https?:\/\/[^\s<>"']+\?[^\s<>"']*/gi;

const NEGATIVE_MESSAGE_PATTERNS = [
  /wrong/i,
  /try again/i,
  /that didn['’]?t work/i,
  /no that['’]?s not/i,
  /\bredo\b/i,
  /\bfix\b/i,
  /\bbroken\b/i,
  /\bbad\b/i,
  /\bincorrect\b/i,
  /sorry/i,
  /apolog/i,
  /did not/i,
];

interface PluginConfig {
  dbPath?: string;
  captureMessageContent?: unknown;
  includeToolParams?: boolean;
  redactKeys?: unknown;
  skillBlockDetection?: boolean;
  contextWindowSize?: unknown;
  contextTimeoutMs?: unknown;
}

interface RawContext {
  sessionId?: string;
  runId?: string;
  sessionKey?: string;
  agentId?: string;
  messageProvider?: string;
  channelId?: string;
  trigger?: string;
  conversationId?: string;
  accountId?: string;
  [key: string]: unknown;
}

interface RawEvent {
  [key: string]: unknown;
}

interface MessageCapture {
  ts: string;
  role: "user" | "assistant";
  length: number;
  text?: string;
  metadata?: unknown;
  signalLabels?: string[];
}

type ImpliedOutcome = "positive" | "negative" | "unclear";

interface SkillExecutionState {
  id: number;
  finalized: boolean;
  ts: string;
  startAt: number;

  sessionId?: string;
  sessionKey?: string;
  runId?: string;

  scopeKeys: string[];
  skillName: string;
  skillPath: string;
  versionHash?: string | null;
  versionHashPromise?: Promise<string | null>;

  intentContext: MessageCapture[];
  followupMessages: MessageCapture[];

  toolReadCount: number;
  encounteredSkillPaths: Set<string>;
  sameSkillRetried: boolean;
  fallbackSkillRetried: boolean;

  inFlightToolCalls: Set<string>;
  hadToolCall: boolean;
  mechanicalSuccess: boolean;
  error?: string;

  inFollowup: boolean;
  followupTimer?: ReturnType<typeof setTimeout>;
  followupStartedAt?: number;
  impliedOutcome?: ImpliedOutcome;

  contextWindowSize: number;
  contextTimeoutMs: number;
}

interface SqliteBackend {
  kind: string;
  close: () => void;
  exec: (sql: string) => void;
  prepare: (sql: string) => {
    run: (params?: Record<string, unknown>) => unknown;
    get: (params?: Record<string, unknown>) => Record<string, unknown> | undefined;
  };
}

interface DbPrepared {
  insertEvent: (params: Record<string, unknown>) => void;
  insertVersion: (params: Record<string, unknown>) => void;
  upsertSkill: (params: Record<string, unknown>) => void;
  getLatestSkillVersion: (params: Record<string, unknown>) => { version_hash?: string } | undefined;
  insertExecution: (params: Record<string, unknown>) => void;
  insertFeedback: (params: Record<string, unknown>) => void;
}

interface DbState {
  backend: SqliteBackend | null;
  statements: DbPrepared | null;
  error?: string;
}

function resolveHomePath(pathLike: string): string {
  if (!pathLike.startsWith("~")) return resolve(pathLike);
  const home = process.env.HOME || process.env.USERPROFILE;
  if (!home) return resolve(pathLike);
  return resolve(home, pathLike.slice(2));
}


function resolveDbPath(pathLike: string | undefined): string {
  return resolveHomePath(pathLike && pathLike.trim().length > 0 ? pathLike : DEFAULT_DB_PATH);
}

function toStringLike(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const t = value.trim();
  return t.length ? t : undefined;
}

function parseIntConfig(value: unknown, fallback: number, min?: number): number {
  const parsed =
    typeof value === "number"
      ? Math.floor(value)
      : Number.parseInt(typeof value === "string" ? value.trim() : "", 10);

  if (!Number.isFinite(parsed)) return fallback;
  if (min !== undefined && parsed < min) return fallback;
  return parsed;
}

function normalizeText(value: unknown, redactKeys: Set<string>): string {
  const text = typeof value !== "string" ? String(value ?? "") : value;
  return sanitizeValue(redactParams(text, redactKeys), MAX_MESSAGE_LEN);
}

function sanitizeValue(value: unknown, maxLen = 400): string {
  if (value === null || value === undefined) return "";

  if (typeof value === "string") {
    const scrubbed = scrubSecrets(value);
    return scrubbed.length <= maxLen
      ? scrubbed
      : `${scrubbed.slice(0, maxLen)}…[truncated ${scrubbed.length - maxLen} chars]`;
  }

  if (Array.isArray(value)) {
    return JSON.stringify(value.map((entry) => sanitizeValue(entry, maxLen))).slice(0, maxLen);
  }

  if (typeof value === "object") {
    return JSON.stringify(redactParams(value, new Set())).slice(0, maxLen);
  }

  return String(value).slice(0, maxLen);
}

function redactUrlsWithQuery(value: string): string {
  return value.replace(URL_WITH_QUERY_PATTERN, (match) => {
    const idx = match.indexOf("?");
    if (idx < 0) return match;
    return `${match.slice(0, idx)}[query-removed]`;
  });
}

function scrubSecrets(value: string): string {
  const cleaned = sanitizePII(EMAIL_PATTERN, value);
  const scrubbedQuery = redactUrlsWithQuery(cleaned);
  const scrubbedPhone = sanitizePII(PHONE_PATTERN, scrubbedQuery);
  return SECRET_PATTERNS.reduce((next, pattern) => next.replace(pattern, "[REDACTED]"), scrubbedPhone);
}

function sanitizePII(pattern: RegExp, value: string): string {
  return value.replace(pattern, "[REDACTED]");
}
function shouldRedactKey(key: string, redactKeys: Set<string>): boolean {
  const candidate = key.toLowerCase();
  if (redactKeys.has(candidate)) return true;
  return candidate.includes("token") || candidate.includes("secret") || candidate.includes("auth") || candidate.includes("password");
}

function redactParams(value: unknown, redactKeys: Set<string>): unknown {
  if (value === null || value === undefined) return value;

  if (typeof value === "string") return scrubSecrets(value);
  if (Array.isArray(value)) return value.map((entry) => redactParams(entry, redactKeys));
  if (typeof value !== "object") return value;

  const entries = value as Record<string, unknown>;
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(entries)) {
    out[k] = shouldRedactKey(k, redactKeys) ? "[REDACTED]" : redactParams(v, redactKeys);
  }
  return out;
}

function buildToolParams(toolName: string, params: Record<string, unknown>, redactKeys: Set<string>) {
  if (toolName === "write" || toolName === "message") {
    const scrubbed = redactParams(params, redactKeys);
    if (typeof scrubbed === "object" && scrubbed && !Array.isArray(scrubbed)) {
      const copy = { ...(scrubbed as Record<string, unknown>) };
      if (Object.prototype.hasOwnProperty.call(copy, "text")) copy.text = "[REDACTED]";
      if (Object.prototype.hasOwnProperty.call(copy, "content")) copy.content = "[REDACTED]";
      if (Object.prototype.hasOwnProperty.call(copy, "body")) copy.body = "[REDACTED]";
      if (Object.prototype.hasOwnProperty.call(copy, "data")) copy.data = "[REDACTED]";
      if (Object.prototype.hasOwnProperty.call(copy, "message")) copy.message = "[REDACTED]";
      return copy;
    }
    return scrubbed;
  }

  if (toolName === "exec" && typeof params === "object" && params) {
    const copy = { ...(params as Record<string, unknown>), command: "[REDACTED]" };
    return copy;
  }

  return redactParams(params, redactKeys);
}

function detectMessageSignals(execution: SkillExecutionState | undefined, text: string): string[] {
  const lower = text.toLowerCase();
  const labels = new Set<string>();

  if (NEGATIVE_MESSAGE_PATTERNS.some((pattern) => pattern.test(lower))) {
    labels.add("negative_phrase_detected");
  }

  if (/(retry|redo|again|rerun|re-run)/i.test(lower)) {
    const token = execution?.skillName?.toLowerCase();
    if (!token || lower.includes(token)) {
      labels.add("skill_retry_detected");
    }
  }

  const mention = /(?:\/skill:|using\s+skill\s+|skill\s+name\s+['"]?)([a-z0-9._-]+)/i.exec(lower);
  if (mention?.[1] && execution && mention[1] !== execution.skillName.toLowerCase() && /fallback|instead|retry/.test(lower)) {
    labels.add("fallback_or_skill_switch_detected");
  }

  if (/(\/skill:|skill\s+name\s+['"]?[a-z0-9._-]+)/i.test(lower)) {
    labels.add("skill_reference_detected");
  }

  return [...labels];
}

function isNegativeSignal(signals: string[]): boolean {
  return signals.includes("negative_phrase_detected") || signals.includes("skill_retry_detected") || signals.includes("fallback_or_skill_switch_detected");
}

function makeMessageCapture(
  text: unknown,
  role: MessageCapture["role"],
  metadata: unknown,
  redactKeys: Set<string>,
  captureContent: boolean,
  signalLabels: string[] = [],
): MessageCapture | null {
  if (typeof text !== "string") return null;

  const safeText = normalizeText(text, redactKeys);
  const snap: MessageCapture = {
    ts: new Date().toISOString(),
    role,
    length: text.length,
    signalLabels: signalLabels.length ? [...new Set(signalLabels)] : undefined,
  };

  if (captureContent) {
    snap.text = safeText;
    if (metadata !== undefined) {
      snap.metadata = normalizeText(metadata, redactKeys);
    }
  }

  return snap;
}

function inferSkillName(skillPath: string): string {
  const resolved = resolve(skillPath);
  if (basename(resolved).toLowerCase() === "skill.md") {
    return basename(dirname(resolved));
  }
  return basename(resolved);
}

function inferSkillSource(skillPath: string): string {
  const abs = resolve(skillPath);
  const home = resolve(process.env.HOME || process.env.USERPROFILE || "");
  if (abs.includes(`${home}/.openclaw/extensions`) || abs.includes(`${home}/.openclaw/extensions/`)) return "extension";
  if (abs.includes(`${home}/.openclaw/skills`) || abs.includes(`${home}/.openclaw/skills/`)) return "bundled";
  if (abs.includes("/skills/") || abs.includes("\\skills\\")) return "workspace";
  return "unknown";
}

function extractSkillPathFromParams(params: Record<string, unknown>): string | undefined {
  const candidates = [
    params.path,
    params.file_path,
    params.filePath,
    params.target,
    params.targetPath,
  ];

  for (const candidate of candidates) {
    const p = toStringLike(candidate);
    if (!p) continue;
    if (basename(p).toLowerCase() === "skill.md") return p;
  }
  return undefined;
}

function buildBase(event: { sessionId?: string; runId?: string } | undefined, ctx: RawContext | undefined) {
  return {
    sessionId: toStringLike(event?.sessionId) || toStringLike(ctx?.sessionId) || undefined,
    runId: toStringLike(event?.runId) || toStringLike(ctx?.runId) || undefined,
    sessionKey: toStringLike(ctx?.sessionKey) || undefined,
    agentId: toStringLike(ctx?.agentId) || undefined,
    channelId: toStringLike(ctx?.channelId) || undefined,
    messageProvider: toStringLike(ctx?.messageProvider) || undefined,
    trigger: toStringLike(ctx?.trigger) || undefined,
  };
}

function buildScopeKeys(ctx: RawContext | undefined, event: RawEvent | undefined): string[] {
  const keys: string[] = [];
  const add = (key: string) => {
    if (key && !keys.includes(key)) keys.push(key);
  };

  const metadata = (event?.metadata as RawEvent) || {};
  const channel = toStringLike(ctx?.channelId) || toStringLike(metadata.channelId) || "unknown";
  const conv =
    toStringLike(ctx?.conversationId) ||
    toStringLike(metadata.conversationId) ||
    toStringLike((metadata as RawEvent).threadTs) ||
    toStringLike((metadata as RawEvent).threadId);
  const account = toStringLike(ctx?.accountId) || toStringLike(metadata.accountId);
  const sessionKey = toStringLike(ctx?.sessionKey) || toStringLike(metadata.sessionKey);
  const sessionId = toStringLike(ctx?.sessionId);
  const runId = toStringLike(ctx?.runId) || toStringLike(metadata.runId) || toStringLike(event?.runId);

  if (sessionKey) add(`sk:${sessionKey}`);
  if (runId) add(`run:${runId}`);
  if (sessionId) add(`sid:${sessionId}`);
  if (conv) {
    add(`conv:${channel}:${conv}`);
    if (account) add(`conv:${channel}:${account}:${conv}`);
  }
  if (account) add(`acct:${channel}:${account}`);
  if (channel) add(`ch:${channel}`);

  if (!keys.length) add("global");

  return keys;
}

function buildMessageScope(ctx: RawContext | undefined, event: RawEvent | undefined): string {
  const scopeCandidates = buildScopeKeys(ctx, event);
  return scopeCandidates.find((key) => key.startsWith("conv:")) || scopeCandidates.find((key) => key.startsWith("acct:")) || scopeCandidates[0];
}

function createPreparedStatements(db: SqliteBackend): DbPrepared {
  const insertEvent = db.prepare(`
    INSERT INTO skill_events (
      ts, type, session_id, session_key, run_id, agent_id, channel_id, message_provider,
      tool_name, tool_call_id, params, duration_ms, success, error,
      skill_name, skill_path, skill_source,
      skill_block_count, skill_block_names, skill_block_locations
    ) VALUES (
      @ts, @type, @session_id, @session_key, @run_id, @agent_id, @channel_id, @message_provider,
      @tool_name, @tool_call_id, @params, @duration_ms, @success, @error,
      @skill_name, @skill_path, @skill_source,
      @skill_block_count, @skill_block_names, @skill_block_locations
    )
  `);

  const insertVersion = db.prepare(`
    INSERT INTO skill_versions (skill_name, skill_path, version_hash, first_seen_at, notes)
    VALUES (@skill_name, @skill_path, @version_hash, @first_seen_at, @notes)
  `);

  const upsertSkill = db.prepare(`
    INSERT INTO skills (
      skill_name,
      skill_path,
      current_version_hash,
      status,
      last_modified_at,
      last_used_at,
      total_executions
    ) VALUES (
      @skill_name,
      @skill_path,
      @current_version_hash,
      COALESCE(@status, 'stable'),
      @last_modified_at,
      @last_used_at,
      1
    )
    ON CONFLICT(skill_name) DO UPDATE SET
      skill_path = excluded.skill_path,
      current_version_hash = excluded.current_version_hash,
      status = COALESCE(skills.status, excluded.status),
      last_modified_at = excluded.last_modified_at,
      last_used_at = excluded.last_used_at,
      total_executions = COALESCE(skills.total_executions, 0) + 1
  `);

  const getLatestSkillVersion = db.prepare(`
    SELECT version_hash
    FROM skill_versions
    WHERE skill_name = @skill_name AND skill_path = @skill_path
    ORDER BY id DESC
    LIMIT 1
  `);

  const getExactSkillVersion = db.prepare(`
    SELECT version_hash
    FROM skill_versions
    WHERE skill_name = @skill_name AND skill_path = @skill_path AND version_hash = @version_hash
    LIMIT 1
  `);

  const insertExecution = db.prepare(`
    INSERT INTO skill_executions (
      ts,
      session_key,
      run_id,
      skill_name,
      skill_path,
      version_hash,
      intent_context,
      mechanical_success,
      semantic_outcome,
      followup_messages,
      implied_outcome,
      error,
      duration_ms
    ) VALUES (
      @ts,
      @session_key,
      @run_id,
      @skill_name,
      @skill_path,
      @version_hash,
      @intent_context,
      @mechanical_success,
      @semantic_outcome,
      @followup_messages,
      @implied_outcome,
      @error,
      @duration_ms
    )
  `);

  const insertFeedback = db.prepare(`
    INSERT INTO skill_feedback (execution_id, source, label, notes)
    VALUES (@execution_id, @source, @label, @notes)
  `);

  return {
    insertEvent: (params) => {
      try {
        insertEvent.run(params);
      } catch {
        // ignore duplicate event shape issues; keep write path non-blocking
      }
    },
    insertVersion: (params) => {
      try {
        const existing = getExactSkillVersion.get(params) as { version_hash?: string } | undefined;
        if (!existing?.version_hash) {
          insertVersion.run(params);
        }
      } catch {
        // best effort
      }
    },
    upsertSkill: (params) => {
      try {
        upsertSkill.run(params);
      } catch {
        // best effort
      }
    },
    getLatestSkillVersion: (params) => getLatestSkillVersion.get(params) as { version_hash?: string } | undefined,
    insertExecution: (params) => insertExecution.run(params),
    insertFeedback: (params) => insertFeedback.run(params),
  };
}

async function initSqlite(path: string, log: { info: (msg: string) => void; error: (msg: string) => void; }) {
  await mkdir(dirname(path), { recursive: true });

  let backend: SqliteBackend | null = null;

  try {
    const sqlite3 = require("better-sqlite3");
    const BetterSqlite3 = sqlite3?.default || sqlite3;
    if (typeof BetterSqlite3 === "function") {
      const db = new BetterSqlite3(path);
      db.pragma("journal_mode = WAL");
      db.pragma("foreign_keys = ON");
      backend = {
        kind: "better-sqlite3",
        close: () => db.close(),
        exec: (sql) => db.exec(sql),
        prepare: (sql) => {
          const stmt = db.prepare(sql);
          return {
            run: (params) => stmt.run(params),
            get: (params) => stmt.get(params) as Record<string, unknown> | undefined,
          };
        },
      };
    }
  } catch {
    // fallback below
  }

  if (!backend) {
    try {
      const sqlite = require("node:sqlite");
      const DatabaseSync = sqlite.DatabaseSync;
      if (typeof DatabaseSync === "function") {
        const db = new DatabaseSync(path);
        db.exec("PRAGMA journal_mode = WAL;");
        db.exec("PRAGMA foreign_keys = ON;");
        backend = {
          kind: "node:sqlite",
          close: () => db.close(),
          exec: (sql) => db.exec(sql),
          prepare: (sql) => {
            const stmt = db.prepare(sql);
            return {
              run: (params) => stmt.run(params),
              get: (params) => stmt.get(params) as Record<string, unknown> | undefined,
            };
          },
        } as SqliteBackend;
      }
    } catch {
      // none
    }
  }

  if (!backend) {
    return {
      backend: null,
      statements: null,
      error: "No sqlite backend available; plugin will continue without sqlite writes",
    } as DbState;
  }

  backend.exec(`
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
  `);

  log.info(`skill-usage-audit: sqlite initialized at ${path} (${backend.kind})`);
  return {
    backend,
    statements: createPreparedStatements(backend),
  } as DbState;
}

async function computeSkillVersionHash(skillPath: string): Promise<string | null> {
  const dir = resolve(dirname(skillPath));
  const scriptDir = join(dir, "scripts");
  const files: string[] = [skillPath];
  try {
    const statDir = await stat(scriptDir);
    if (statDir.isDirectory()) {
      const scriptEntries = await readdir(scriptDir, { withFileTypes: true });
      for (const entry of scriptEntries.sort((a, b) => a.name.localeCompare(b.name))) {
        if (entry.isFile()) files.push(join(scriptDir, entry.name));
      }
    }
  } catch {
    // no scripts
  }

  if (!files.length) return null;

  const hash = createHash("sha256");
  let hasBytes = false;
  for (const p of files) {
    try {
      const data = await readFile(p);
      hash.update(relative(dir, p));
      hash.update("\0");
      hash.update(data);
      hash.update("\0");
      hasBytes = true;
    } catch {
      // skip unreadable
    }
  }

  if (!hasBytes) return null;
  return hash.digest("hex");
}

export default function register(api: OpenClawPluginApi) {
  const log = api.logger;
  const cfg = (api.pluginConfig as PluginConfig) || {};

  const includeToolParams = cfg.includeToolParams ?? DEFAULT_INCLUDE_TOOL_PARAMS;
  const captureMessageContent = cfg.captureMessageContent === undefined ? DEFAULT_CAPTURE_MESSAGE_CONTENT : cfg.captureMessageContent === true;
  const redactKeys = new Set((Array.isArray(cfg.redactKeys) ? cfg.redactKeys : DEFAULT_REDACT_KEYS).map((k) => String(k).toLowerCase()));
  const contextWindowSize = parseIntConfig(cfg.contextWindowSize, DEFAULT_CONTEXT_WINDOW_SIZE, 1);
  const contextTimeoutMs = parseIntConfig(cfg.contextTimeoutMs, DEFAULT_CONTEXT_TIMEOUT_MS, 0);
  const detectSkillBlocks = cfg.skillBlockDetection !== false;
  const dbPath = resolveDbPath(cfg.dbPath);

  const dbState: DbState = { backend: null, statements: null };
  const dbInitPromise = initSqlite(dbPath, log);
  let dbChain = Promise.resolve<void>(undefined);
  let hasLoggedDbIssue = false;

  let shutdownInProgress = false;
  let shutdownPromise: Promise<void> | null = null;

  const messageHistory = new Map<string, MessageCapture[]>();
  const executionsById = new Map<number, SkillExecutionState>();
  const execByScope = new Map<string, SkillExecutionState[]>();
  const execByTool = new Map<string, number>();
  let executionSeq = 0;

  async function ensureDbReady(): Promise<DbState> {
    if (dbState.backend || dbState.statements || dbState.error) {
      return dbState;
    }

    const state = await dbInitPromise;
    if (!dbState.statements) {
      dbState.backend = state.backend;
      dbState.statements = state.statements;
      dbState.error = state.error;
    }

    if (state.error && !hasLoggedDbIssue) {
      hasLoggedDbIssue = true;
      log.info(`skill-usage-audit: sqlite unavailable: ${state.error}`);
    }

    return dbState;
  }

  function queueDbInsert(rowType: RawEvent): void {
    if (rowType.type !== "session_start" && rowType.type !== "session_end" && rowType.type !== "tool_call_start" && rowType.type !== "tool_call_end" && rowType.type !== "skill_file_read" && rowType.type !== "skill_block_detected") {
      return;
    }

    dbChain = dbChain
      .then(async () => {
        const state = await ensureDbReady();
        if (!state.statements) return;

        const row = {
          ts: toStringLike(rowType.ts) || new Date().toISOString(),
          type: rowType.type,
          session_id: toStringLike(rowType.sessionId) || null,
          session_key: toStringLike(rowType.sessionKey) || null,
          run_id: toStringLike(rowType.runId) || null,
          agent_id: toStringLike(rowType.agentId) || null,
          channel_id: toStringLike(rowType.channelId) || null,
          message_provider: toStringLike(rowType.messageProvider) || null,
          tool_name: toStringLike(rowType.toolName) || null,
          tool_call_id: toStringLike(rowType.toolCallId) || null,
          params: rowType.params ? JSON.stringify(rowType.params) : null,
          duration_ms: typeof rowType.durationMs === "number" ? Math.max(0, Math.floor(rowType.durationMs)) : null,
          success: typeof rowType.success === "boolean" ? (rowType.success ? 1 : 0) : null,
          error: toStringLike(rowType.error) || null,
          skill_name: toStringLike(rowType.skillName) || null,
          skill_path: toStringLike(rowType.skillPath) || null,
          skill_source: toStringLike(rowType.skillSource) || null,
          skill_block_count: typeof rowType.skillBlockCount === "number" && Number.isFinite(rowType.skillBlockCount)
            ? Math.max(0, Math.floor(rowType.skillBlockCount))
            : null,
          skill_block_names: Array.isArray(rowType.skillBlockNames) ? JSON.stringify(rowType.skillBlockNames) : null,
          skill_block_locations: Array.isArray(rowType.skillBlockLocations) ? JSON.stringify(rowType.skillBlockLocations) : null,
        };

        state.statements?.insertEvent(row);
      })
      .catch((err) => {
        log.error(`skill-usage-audit: db write failed: ${String(err)}`);
      });
  }

  function queueSkillVersionWrite(skillName: string, skillPath: string, ts: string, versionHash: string | null | undefined): void {
    dbChain = dbChain
      .then(async () => {
        const state = await ensureDbReady();
        if (!state.statements) return;

        const hash = versionHash ?? (await computeSkillVersionHash(skillPath));
        if (!hash) return;

        state.statements.insertVersion({
          skill_name: skillName,
          skill_path: resolve(skillPath),
          version_hash: hash,
          first_seen_at: ts,
          notes: null,
        });

        state.statements.upsertSkill({
          skill_name: skillName,
          skill_path: resolve(skillPath),
          current_version_hash: hash,
          status: "stable",
          last_modified_at: ts,
          last_used_at: ts,
        });
      })
      .catch((err) => {
        log.error(`skill-usage-audit: failed writing skill version: ${String(err)}`);
      });
  }

  function enqueueEvent(event: RawEvent): void {
    queueDbInsert(event);
  }

  function addMessage(scope: string, message: MessageCapture): void {
    const list = messageHistory.get(scope) || [];
    list.push(message);
    if (list.length > MAX_HISTORY_PER_SCOPE) {
      list.splice(0, list.length - MAX_HISTORY_PER_SCOPE);
    }
    messageHistory.set(scope, list);
  }

  function getRecentMessages(scopeKeys: string[], windowSize: number): MessageCapture[] {
    const all: MessageCapture[] = [];
    for (const key of scopeKeys) {
      const list = messageHistory.get(key) || [];
      all.push(...list);
    }
    return all.sort((a, b) => (a.ts < b.ts ? -1 : a.ts > b.ts ? 1 : 0)).slice(-windowSize);
  }

  function trackExecution(execution: SkillExecutionState): void {
    for (const key of execution.scopeKeys) {
      const list = execByScope.get(key) || [];
      if (!list.includes(execution)) list.push(execution);
      execByScope.set(key, list);
    }
  }

  function untrackExecution(execution: SkillExecutionState): void {
    for (const key of execution.scopeKeys) {
      const list = execByScope.get(key);
      if (!list) continue;
      const next = list.filter((entry) => entry.id !== execution.id);
      if (next.length) execByScope.set(key, next);
      else execByScope.delete(key);
    }

    for (const [toolId, id] of execByTool.entries()) {
      if (id === execution.id) {
        execByTool.delete(toolId);
      }
    }

    executionsById.delete(execution.id);
  }

  function candidatesForScope(keys: string[]): SkillExecutionState[] {
    const set = new Map<number, SkillExecutionState>();
    for (const key of keys) {
      const list = execByScope.get(key) || [];
      for (const execution of list) {
        if (!execution.finalized) set.set(execution.id, execution);
      }
    }
    return [...set.values()].sort((a, b) => b.startAt - a.startAt);
  }

  function pickExecution(keys: string[], requireFollowup = false): SkillExecutionState | undefined {
    const list = candidatesForScope(keys);
    if (!list.length) return undefined;
    if (requireFollowup) {
      return list.find((execution) => execution.inFollowup) || list[0];
    }
    return list[0];
  }

  function markFollowup(execution: SkillExecutionState): void {
    if (execution.inFollowup) return;

    execution.inFollowup = true;
    execution.followupStartedAt = Date.now();

    if (contextTimeoutMs <= 0) {
      finalizeExecution(execution.id, "followup-timeout");
      return;
    }

    execution.followupTimer = setTimeout(() => {
      finalizeExecution(execution.id, "followup-timeout");
    }, contextTimeoutMs);
  }

  function stopFollowup(execution: SkillExecutionState): void {
    if (!execution.inFollowup) return;
    execution.inFollowup = false;
    execution.followupStartedAt = undefined;
    if (execution.followupTimer) clearTimeout(execution.followupTimer);
    execution.followupTimer = undefined;
    execution.followupMessages = [];
  }

  function isNegativeMessage(execution: SkillExecutionState, text: string | undefined): boolean {
    return isNegativeSignal(detectMessageSignals(execution, text || ""));
  }

  function determineOutcome(execution: SkillExecutionState): ImpliedOutcome {
    if (execution.fallbackSkillRetried || execution.sameSkillRetried) return "negative";
    if (execution.followupMessages.some((m) => isNegativeMessage(execution, m.text))) return "negative";
    if (execution.followupMessages.some((m) => isNegativeSignal(m.signalLabels || []))) return "negative";
    if (execution.hadToolCall && execution.mechanicalSuccess) return "positive";
    return "unclear";
  }

  function finalizeExecution(executionId: number, reason: string): void {
    const execution = executionsById.get(executionId);
    if (!execution || execution.finalized) return;

    execution.finalized = true;
    if (execution.followupTimer) clearTimeout(execution.followupTimer);
    execution.followupTimer = undefined;

    untrackExecution(execution);

    const durationMs = Math.max(0, Date.now() - execution.startAt);
    const outcome = determineOutcome(execution);

    dbChain = dbChain
      .then(async () => {
        const state = await ensureDbReady();
        if (!state.statements) return;

        let versionHash = execution.versionHash ?? null;
        if (!versionHash && execution.versionHashPromise) {
          try {
            versionHash = await execution.versionHashPromise;
          } catch {
            versionHash = null;
          }
        }

        if (!versionHash) {
          const row = state.statements.getLatestSkillVersion({
            skill_name: execution.skillName,
            skill_path: execution.skillPath,
          });
          if (row?.version_hash) versionHash = String(row.version_hash);
        }

        const mechanicalSuccess = execution.hadToolCall ? (execution.mechanicalSuccess ? 1 : 0) : null;
        state.statements.insertExecution({
          ts: execution.ts,
          session_key: execution.sessionKey || null,
          run_id: execution.runId || null,
          skill_name: execution.skillName,
          skill_path: execution.skillPath,
          version_hash: versionHash || null,
          intent_context: JSON.stringify(execution.intentContext),
          mechanical_success: mechanicalSuccess,
          semantic_outcome: "unclear",
          followup_messages: JSON.stringify(execution.followupMessages),
          implied_outcome: outcome,
          error: execution.error || null,
          duration_ms: durationMs,
        });
      })
      .catch((err) => {
        log.error(`skill-usage-audit: failed inserting execution ${executionId}: ${String(err)} (${reason})`);
      });
  }

  function normalizeSkillExecutionPath(rawPath: string | undefined): string | undefined {
    if (!rawPath) return undefined;
    const trimmed = rawPath.trim();
    if (!trimmed) return undefined;
    return trimmed.startsWith("~") ? resolveHomePath(trimmed) : resolve(trimmed);
  }

  function resolveSkillPathFromBlock(name: string, location: string | undefined): string | undefined {
    if (location) {
      const normalized = normalizeSkillExecutionPath(location);
      if (!normalized) return undefined;
      const lower = normalized.toLowerCase();
      if (lower.endsWith("skill.md")) return normalized;
      return `${normalized}/SKILL.md`;
    }

    if (!name) return undefined;
    const trimmed = name.trim();
    if (!trimmed) return undefined;
    if (trimmed.startsWith("~") || trimmed.includes("/") || trimmed.endsWith(".md")) {
      const normalized = normalizeSkillExecutionPath(trimmed);
      if (normalized && normalized.toLowerCase().endsWith(".md")) return normalized;
    }
    return undefined;
  }

  function findMatchingExecution(
    scopeKeys: string[],
    skillName: string,
    skillPath: string | undefined,
    runId: string | undefined,
  ): SkillExecutionState | undefined {
    const candidates = candidatesForScope(scopeKeys).filter((execution) => {
      if (execution.skillName.toLowerCase() === skillName.toLowerCase()) return true;
      return !!(skillPath && execution.encounteredSkillPaths.has(skillPath));
    });

    if (!candidates.length) return undefined;

    if (runId) {
      const sameRun = candidates.filter((e) => e.runId === runId);
      if (sameRun.length) {
        const exact = skillPath ? sameRun.filter((e) => e.encounteredSkillPaths.has(skillPath)) : [];
        if (exact.length) return exact[0];
        return sameRun[0];
      }
    }

    const exactPath = skillPath ? candidates.filter((e) => e.encounteredSkillPaths.has(skillPath)) : [];
    if (exactPath.length) return exactPath[0];

    return candidates[0];
  }

  function syncExecutionPath(execution: SkillExecutionState, skillName: string, skillPath: string | undefined): void {
    const normalized = normalizeSkillExecutionPath(skillPath);
    if (normalized) {
      execution.encounteredSkillPaths.add(normalized);
      if (execution.skillPath === execution.skillName || execution.skillPath === "") {
        execution.skillPath = normalized;
        execution.versionHashPromise = computeSkillVersionHash(normalized);
        execution.versionHash = null;
      }
    }

    if (skillName && execution.skillName !== skillName) {
      execution.skillName = skillName;
    }
  }

  function attachToolCall(keys: string[], event: RawEvent, execution?: SkillExecutionState): SkillExecutionState | undefined {
    const target = execution || pickExecution(keys);
    if (!target || target.finalized) return;

    if (target.inFollowup) stopFollowup(target);

    target.hadToolCall = true;
    target.toolReadCount += 1;

    const callId = toStringLike(event.toolCallId)
      ? `tool:${toStringLike(event.toolCallId)}`
      : `anon:${target.id}:${target.toolReadCount}`;
    target.inFlightToolCalls.add(callId);

    execByTool.set(callId, target.id);

    const toolName = toStringLike(event.toolName) || "";
    if (toolName === "read" && typeof event.params === "object") {
      const p = event.params as Record<string, unknown>;
      const rawPath = extractSkillPathFromParams(p);
      if (rawPath) {
        const inferredSkillName = inferSkillName(rawPath);
        syncExecutionPath(target, inferredSkillName, rawPath);

        const normalized = normalizeSkillExecutionPath(rawPath);
        if (normalized) {
          if (normalized === target.skillPath) {
            if (target.toolReadCount > 1) target.sameSkillRetried = true;
          } else {
            target.fallbackSkillRetried = true;
          }
          target.encounteredSkillPaths.add(normalized);
        }
      }
    }

    return target;
  }

  function detachToolCall(event: RawEvent, scopeKeys: string[]): SkillExecutionState | undefined {
    const toolId = toStringLike(event.toolCallId);
    const direct = toolId ? execByTool.get(`tool:${toolId}`) : undefined;
    const target = direct ? executionsById.get(direct) : pickExecution(scopeKeys);
    if (!target || target.finalized) return;

    const key = toolId ? `tool:${toolId}` : [...target.inFlightToolCalls].find((id) => id.startsWith(`anon:${target.id}:`));
    if (key) {
      target.inFlightToolCalls.delete(key);
      execByTool.delete(key);
    }

    if (target.inFlightToolCalls.size === 0) {
      markFollowup(target);
    }

    const toolName = toStringLike(event.toolName) || "";
    if (toolName === "read" && typeof event.params === "object" && event.error) {
      target.mechanicalSuccess = false;
      target.error = target.error || toStringLike(event.error) || "tool failure";
    }

    if (event.error) {
      target.mechanicalSuccess = false;
      target.error = target.error || toStringLike(event.error) || "tool failure";
    }

    return target;
  }

  function onFollowupMessage(execution: SkillExecutionState | undefined, message: MessageCapture): void {
    if (!execution) return;

    if (!execution.inFollowup) {
      execution.inFollowup = true;
      execution.followupStartedAt = Date.now();
    }
    execution.followupMessages.push(message);

    if (execution.followupMessages.length >= execution.contextWindowSize) {
      finalizeExecution(execution.id, "followup-limit");
      return;
    }

    if (isNegativeMessage(execution, message.text) || isNegativeSignal(message.signalLabels || [])) {
      finalizeExecution(execution.id, "negative-signal");
      return;
    }

    if (contextTimeoutMs <= 0 && execution.followupMessages.length > 0) {
      finalizeExecution(execution.id, "no-timeout");
    }
  }

  function startExecutionFromSkillRead(
    ctx: RawContext | undefined,
    event: RawEvent,
    skillPath: string,
    now: string,
    isFromSkillBlock = false,
  ): SkillExecutionState {
    const initialPath = normalizeSkillExecutionPath(skillPath) || skillPath;
    const skillName = inferSkillName(initialPath);
    const scopeKeys = buildScopeKeys(ctx, event);
    const runId = toStringLike(event.runId) || toStringLike(ctx?.runId) || toStringLike(ctx?.sessionId);

    const existing = findMatchingExecution(scopeKeys, skillName, initialPath, runId);
    if (existing) {
      syncExecutionPath(existing, skillName, initialPath);
      return existing;
    }

    const intentContext = getRecentMessages(scopeKeys, contextWindowSize);
    const execution: SkillExecutionState = {
      id: ++executionSeq,
      finalized: false,
      ts: now,
      startAt: Date.now(),
      sessionId: toStringLike(ctx?.sessionId),
      sessionKey: toStringLike(ctx?.sessionKey),
      runId,
      scopeKeys,
      skillName,
      skillPath: initialPath,
      versionHash: null,
      versionHashPromise: computeSkillVersionHash(initialPath),
      intentContext,
      followupMessages: [],
      toolReadCount: 0,
      encounteredSkillPaths: new Set(initialPath ? [initialPath] : []),
      sameSkillRetried: false,
      fallbackSkillRetried: false,
      inFlightToolCalls: new Set(),
      hadToolCall: false,
      mechanicalSuccess: true,
      inFollowup: false,
      contextWindowSize,
      contextTimeoutMs,
    };

    execution.versionHashPromise
      ?.then((h) => {
        execution.versionHash = h;
      })
      .catch(() => {
        // keep null; lookup later on finalize
      });

    trackExecution(execution);
    executionsById.set(execution.id, execution);

    queueSkillVersionWrite(skillName, initialPath, now, execution.versionHash);

    if (!isFromSkillBlock) {
      enqueueEvent({
        v: 1,
        ts: now,
        type: "skill_file_read",
        ...buildBase(event as { sessionId?: string; runId?: string }, ctx),
        skillName,
        skillPath: initialPath,
        skillSource: inferSkillSource(initialPath),
        toolName: toStringLike(event.toolName),
        toolCallId: toStringLike(event.toolCallId),
      });
    }

    return execution;
  }

  interface ParsedSkillBlock {
    name: string;
    location?: string;
  }

  function parseSkillBlock(prompt: string): { blocks: ParsedSkillBlock[]; names: string[]; locations: string[]; count: number } {
    const rgx = /<\s*skill\b([^>]*)>/gi;
    const blocks: ParsedSkillBlock[] = [];
    const names: string[] = [];
    const locations: string[] = [];
    let match = rgx.exec(prompt);

    while (match) {
      const attrs = match[1] || "";
      const nameMatch = /\bname="([^"]+)"/i.exec(attrs);
      const locMatch = /\blocation="([^"]+)"/i.exec(attrs);
      const name = nameMatch?.[1];
      if (!name) {
        match = rgx.exec(prompt);
        continue;
      }

      const location = locMatch?.[1];
      blocks.push({ name, location });
      names.push(name);
      if (location) locations.push(location);
      match = rgx.exec(prompt);
    }

    return { blocks, names, locations, count: names.length };
  }


  api.on("session_start", async (event, ctx: RawContext) => {
    enqueueEvent({
      v: 1,
      ts: new Date().toISOString(),
      type: "session_start",
      ...buildBase(event as { sessionId?: string }, ctx),
    });
  });

  api.on("session_end", async (event, ctx: RawContext) => {
    const keys = buildScopeKeys(ctx, event);
    const remaining = candidatesForScope(keys);
    for (const execution of remaining) {
      finalizeExecution(execution.id, "session-end");
    }

    enqueueEvent({
      v: 1,
      ts: new Date().toISOString(),
      type: "session_end",
      ...buildBase(event as { sessionId?: string }, ctx),
      durationMs: (event as RawEvent).durationMs,
      messageCount: (event as RawEvent).messageCount,
    });
  });

  api.on("before_tool_call", async (event, ctx: RawContext) => {
    const now = new Date().toISOString();
    const toolName = toStringLike((event as RawEvent).toolName) || "";
    const params = ((event as RawEvent).params as Record<string, unknown>) || {};

    enqueueEvent({
      v: 1,
      ts: now,
      type: "tool_call_start",
      ...buildBase(event as { sessionId?: string; runId?: string }, ctx),
      toolName,
      toolCallId: toStringLike((event as RawEvent).toolCallId),
      params: includeToolParams ? buildToolParams(toolName, params, redactKeys) : undefined,
    });

    const scopeKeys = buildScopeKeys(ctx, event);
    const skillPath = extractSkillPathFromParams(params);

    if (toolName === "read" && skillPath) {
      const normalized = resolve(skillPath);
      const execution = startExecutionFromSkillRead(ctx, event as RawEvent, normalized, now);
      attachToolCall(scopeKeys, event as RawEvent, execution);
      return;
    }

    attachToolCall(scopeKeys, event as RawEvent);
  });

  api.on("after_tool_call", async (event, ctx: RawContext) => {
    const now = new Date().toISOString();
    const toolName = toStringLike((event as RawEvent).toolName) || "";
    const params = ((event as RawEvent).params as Record<string, unknown>) || {};
    const scopeKeys = buildScopeKeys(ctx, event);
    const linked = detachToolCall(event as RawEvent, scopeKeys);

    enqueueEvent({
      v: 1,
      ts: now,
      type: "tool_call_end",
      ...buildBase(event as { sessionId?: string; runId?: string }, ctx),
      toolName,
      toolCallId: toStringLike((event as RawEvent).toolCallId),
      params: includeToolParams ? buildToolParams(toolName, params, redactKeys) : undefined,
      durationMs: typeof (event as RawEvent).durationMs === "number" ? Number((event as RawEvent).durationMs) : undefined,
      success: !(typeof (event as RawEvent).error === "string"),
      error: toStringLike((event as RawEvent).error),
      skillName: linked?.skillName,
      skillPath: linked?.skillPath,
    });
  });

  api.on("message_received", async (event, ctx: RawContext) => {
    const text = toStringLike((event as RawEvent).content);
    if (text === undefined) return;

    const scopeKeys = buildScopeKeys(ctx, event);
    const execution = pickExecution(scopeKeys, true);
    const msg = makeMessageCapture(text, "user", (event as RawEvent).metadata, redactKeys, captureMessageContent, detectMessageSignals(execution, text));
    if (!msg) return;

    const scope = buildMessageScope(ctx, event);
    addMessage(scope, msg);

    if (execution) {
      onFollowupMessage(execution, msg);
    }
  });

  api.on("message_sent", async (event, ctx: RawContext) => {
    const text = toStringLike((event as RawEvent).content);
    if (text === undefined) return;

    const scopeKeys = buildScopeKeys(ctx, event);
    const execution = pickExecution(scopeKeys, true);
    const msg = makeMessageCapture(text, "assistant", (event as RawEvent).metadata, redactKeys, captureMessageContent, detectMessageSignals(execution, text));
    if (!msg) return;

    const scope = buildMessageScope(ctx, event);
    addMessage(scope, msg);

    if (execution) {
      onFollowupMessage(execution, msg);
    }
  });

  api.on("before_prompt_build", async (event, ctx: RawContext) => {
    if (!detectSkillBlocks) return;
    const prompt = toStringLike((event as RawEvent).prompt);
    if (!prompt) return;

    const info = parseSkillBlock(prompt);
    if (!info.count) return;

    const timestamp = new Date().toISOString();

    enqueueEvent({
      v: 1,
      ts: timestamp,
      type: "skill_block_detected",
      ...buildBase(undefined, ctx),
      skillBlockCount: info.count,
      skillBlockNames: info.names,
      skillBlockLocations: info.locations,
    });

    for (const block of info.blocks) {
      const resolvedPath = resolveSkillPathFromBlock(block.name, block.location) || block.location || block.name;
      if (!resolvedPath) continue;
      startExecutionFromSkillRead(ctx, event as RawEvent, resolvedPath, timestamp, true);
    }
  });

  function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    return Promise.race([
      promise,
      new Promise<T>((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error("flush-timeout")), timeoutMs);
      }),
    ]).finally(() => {
      if (timer) clearTimeout(timer);
    });
  }

  function finalizeAllExecutions(reason: string): void {
    for (const execution of [...executionsById.values()]) {
      finalizeExecution(execution.id, reason);
    }
  }

  async function flushPendingWrites(reason: string): Promise<void> {
    finalizeAllExecutions(reason);

    try {
      await withTimeout(dbChain, 2500);
    } catch (err) {
      log.error(`skill-usage-audit: shutdown flush failed: ${String(err)}`);
    }

    if (dbState.backend) {
      try {
        dbState.backend.close();
      } catch {
        // ignore
      }
      dbState.backend = null;
      dbState.statements = null;
    }
  }

  function requestFlush(reason: string): Promise<void> {
    if (!shutdownPromise) {
      shutdownInProgress = true;
      shutdownPromise = flushPendingWrites(reason);
    }
    return shutdownPromise;
  }

  function handleSignal(signal: string): void {
    const timeout = setTimeout(() => {
      log.info(`skill-usage-audit: forced shutdown after ${signal}`);
      if (dbState.backend) {
        try {
          dbState.backend.close();
        } catch {
          // ignore
        }
      }
      process.exit(0);
    }, 3500);

    void requestFlush(`signal:${signal}`).finally(() => {
      clearTimeout(timeout);
      process.exit(0);
    });
  }

  process.on("SIGTERM", () => handleSignal("SIGTERM"));
  process.on("SIGINT", () => handleSignal("SIGINT"));
  process.on("beforeExit", () => {
    if (!shutdownInProgress) {
      void requestFlush("beforeExit");
    }
  });
  process.on("exit", () => {
    if (dbState.backend) {
      try {
        dbState.backend.close();
      } catch {
        // ignore
      }
    }
  });

  // eager init for logs
  void ensureDbReady();
  log.info("skill-usage-audit plugin registered");
}
