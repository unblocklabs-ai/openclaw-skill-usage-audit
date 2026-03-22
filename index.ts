/**
 * Skill usage audit plugin
 */

import { mkdir, readdir, readFile, stat } from "node:fs/promises";
import { dirname, basename, resolve, join, relative, sep } from "node:path";

import { createHash } from "node:crypto";

import type { OpenClawPluginApi } from "openclaw/plugin-sdk";


const DEFAULT_DB_PATH = "~/.openclaw/audits/skill-usage.db";
const DEFAULT_INCLUDE_TOOL_PARAMS = false;
const DEFAULT_CAPTURE_MESSAGE_CONTENT = false;
const DEFAULT_CONTEXT_WINDOW_SIZE = 5;
const DEFAULT_CONTEXT_TIMEOUT_MS = 60000;
const MAX_MESSAGE_LEN = 200;
const MAX_HISTORY_PER_SCOPE = 80;

const DEFAULT_ROUTER_MAX_SKILLS = 1;
const DEFAULT_ROUTER_MIN_SCORE = 6;
const DEFAULT_ROUTER_RECENCY_WINDOW = 10;
const ROUTER_CACHE_TTL_MS = 60_000;
const ROUTER_FRONTMATTER_MAX_LINES = 55;

const MESSAGE_HISTORY_STALE_MS = 30 * 60 * 1000;
const MESSAGE_HISTORY_CLEANUP_EVERY = 100;
const MESSAGE_HISTORY_MAX_SCOPES = 500;

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
  router?: RouterConfig;
}

interface RouterConfig {
  enabled?: unknown;
  targets?: {
    subagent?: unknown;
    cron?: unknown;
  };
  maxSkillsToNudge?: unknown;
  minScore?: unknown;
  recencyWindow?: unknown;
  overrides?: unknown;
  skillKeywords?: unknown;
  blocklist?: unknown;
}

interface RouterOverride {
  taskPattern: string;
  skills: string[];
  matcher?: RegExp;
}

interface SkillCandidate {
  name: string;
  description: string;
  filePath: string;
}

interface SkillCandidateCache {
  fetchedAt: number;
  candidates: SkillCandidate[];
  idf: Map<string, number>;
  avgDescLen: number;
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

interface MessageHistoryEntry {
  messages: MessageCapture[];
  lastSeenAt: number;
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
  skillReadCount: number;
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
  insertNudge: (params: Record<string, unknown>) => void;
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

function parseFloatConfig(value: unknown, fallback: number, min?: number): number {
  const parsed =
    typeof value === "number"
      ? value
      : Number.parseFloat(typeof value === "string" ? value.trim() : "");

  if (!Number.isFinite(parsed)) return fallback;
  if (min !== undefined && parsed < min) return fallback;
  return parsed;
}

function parseBooleanConfig(value: unknown, fallback: boolean): boolean {
  return typeof value === "boolean" ? value : fallback;
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

  const snap: MessageCapture = {
    ts: new Date().toISOString(),
    role,
    length: text.length,
    signalLabels: signalLabels.length ? [...new Set(signalLabels)] : undefined,
  };

  if (captureContent) {
    const safeText = normalizeText(text, redactKeys);
    snap.text = safeText;
    if (metadata !== undefined) {
      snap.metadata = normalizeText(metadata, redactKeys);
    }
  }

  return snap;
}

function inferSkillName(skillPath: string): string {
  const resolved = skillPath.startsWith("~") ? resolveHomePath(skillPath) : resolve(skillPath);
  if (basename(resolved).toLowerCase() === "skill.md") {
    return basename(dirname(resolved));
  }
  return basename(resolved);
}

function inferSkillSource(skillPath: string): string {
  const abs = skillPath.startsWith("~") ? resolveHomePath(skillPath) : resolve(skillPath);
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

const workspaceSkillCache = new Map<string, SkillCandidateCache>();

function resolveWorkspaceDir(): string {
  const extDir = resolve(__dirname);
  const marker = `${sep}.openclaw${sep}`;
  const markerIdx = extDir.indexOf(marker);
  if (markerIdx >= 0) {
    const candidate = extDir.slice(0, markerIdx);
    return candidate || process.cwd();
  }
  return process.cwd();
}

function parseRouterTextArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value
    .map((entry) => (typeof entry === "string" ? entry.trim() : ""))
    .filter((entry) => entry.length > 0);
}

function parseSkillKeywords(value: unknown): Record<string, string[]> {
  const out: Record<string, string[]> = {};
  if (!value || typeof value !== "object" || Array.isArray(value)) return out;

  const obj = value as Record<string, unknown>;
  for (const [key, val] of Object.entries(obj)) {
    const parsed = parseRouterTextArray(val);
    if (parsed.length) {
      out[key.toLowerCase()] = parsed;
    }
  }
  return out;
}

function parseOverrideRules(value: unknown): RouterOverride[] {
  if (!Array.isArray(value)) return [];

  const out: RouterOverride[] = [];
  for (const entry of value) {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) continue;
    const pattern = toStringLike((entry as Record<string, unknown>).taskPattern);
    const skills = parseRouterTextArray((entry as Record<string, unknown>).skills);
    if (pattern && skills.length) {
      out.push({ taskPattern: pattern, skills });
    }
  }
  return out;
}

function parseBlocklist(value: unknown): string[] {
  return parseRouterTextArray(value).map((item) => item.toLowerCase());
}

function normalizePathForDisplay(filePath: string): string {
  const home = process.env.HOME || process.env.USERPROFILE;
  if (home && filePath.startsWith(home)) {
    return `~${filePath.slice(home.length)}`;
  }
  return filePath;
}

function getTextFromMessage(message: unknown): string | undefined {
  if (typeof message === "string") return message;
  if (!message || typeof message !== "object") return undefined;

  const msg = message as Record<string, unknown>;
  if (typeof msg.content === "string") return msg.content;
  if (msg.content && typeof msg.content === "object") {
    const nested = msg.content as Record<string, unknown>;
    if (typeof nested.text === "string") return nested.text;
  }
  if (typeof msg.text === "string") return msg.text;
  if (typeof msg.body === "string") return msg.body;
  if (typeof msg.message === "string") return msg.message;
  return undefined;
}

function extractToolNameFromMessage(message: unknown): string | undefined {
  if (!message || typeof message !== "object" || Array.isArray(message)) return undefined;

  const msg = message as Record<string, unknown>;
  return (
    toStringLike(msg.toolName) ||
    toStringLike(msg.tool_name) ||
    toStringLike(msg.tool)
  )?.toLowerCase();
}

function normalizeCandidatePath(value: string): string {
  const raw = value
    .replace(/\\/g, "/")
    .replace(/["'`(){}\[\]]/g, "");

  const expanded = raw.startsWith("~") ? resolveHomePath(raw) : raw;
  return expanded
    .trim()
    .replace(/[#?].*$/, "")
    .replace(/\\/g, "/")
    .replace(/\/+/g, "/")
    .replace(/^\/+|\/+$/g, "")
    .toLowerCase();
}


function extractReadPathsFromToolMessage(message: unknown): string[] {
  if (!message || typeof message !== "object" || Array.isArray(message)) return [];

  const toolName = extractToolNameFromMessage(message);
  if (toolName !== "read") return [];

  const msg = message as Record<string, unknown>;
  const params =
    msg.params && typeof msg.params === "object" && !Array.isArray(msg.params)
      ? msg.params as Record<string, unknown>
      : undefined;

  const out: string[] = [];

  const collect = (value: unknown) => {
    const text = toStringLike(value);
    if (text) out.push(text);
  };

  if (params) {
    collect(params.path);
    collect(params.file_path);
    collect(params.filePath);
  }

  return out;
}

function wasSkillHandledRecently(messages: unknown[], skillName: string, skillFilePath: string, recencyWindow: number): boolean {
  if (!messages.length || recencyWindow <= 0) return false;

  const lookback = Math.max(1, Math.min(messages.length, recencyWindow));
  const start = messages.length - lookback;
  const normalizedSkillPath = normalizeCandidatePath(skillFilePath);
  const skillSuffix = `/${skillName.toLowerCase()}/skill.md`;

  for (let i = messages.length - 1; i >= start; i--) {
    const message = messages[i];
    const text = getTextFromMessage(message);

    if (text && text.includes("[skill-router]") && text.includes(`→ ${skillName}:`)) {
      return true;
    }

    const candidatePaths = extractReadPathsFromToolMessage(message);
    for (const candidate of candidatePaths) {
      const normalized = normalizeCandidatePath(candidate);
      if (!normalized) continue;

      if ((normalized === normalizedSkillPath) || normalized.endsWith(skillSuffix)) {
        return true;
      }
    }
  }

  return false;
}

function messageIndexForTask(messages: unknown[], prompt: string): number | null {
  if (!messages.length) return null;
  for (let i = messages.length - 1; i >= 0; i -= 1) {
    const text = getTextFromMessage(messages[i]);
    if (typeof text === "string" && text === prompt) {
      return i;
    }
  }
  return messages.length - 1;
}

function parseFrontmatterFromSkillMd(content: string): { name?: string; description?: string } | null {
  const lines = content.split(/\r?\n/).slice(0, ROUTER_FRONTMATTER_MAX_LINES);
  if (!lines.length || lines[0]?.trim() !== "---") return null;

  const out: { [k: string]: string } = {};
  for (let i = 1; i < lines.length; i += 1) {
    const line = lines[i].trim();
    if (line === "---") break;
    if (!line || line.startsWith("#")) continue;

    const colon = line.indexOf(":");
    if (colon < 0) continue;

    const key = line.slice(0, colon).trim().toLowerCase();
    let value = line.slice(colon + 1).trim();
    if (!key || !value) continue;

    if ((value.startsWith("\"") && value.endsWith("\"")) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }

    out[key] = value;
  }

  if (!Object.keys(out).length) return null;
  return {
    name: out.name,
    description: out.description,
  };
}

async function collectSkillCandidatesFromRoot(root: string): Promise<SkillCandidate[]> {
  const out: SkillCandidate[] = [];

  let entries: Awaited<ReturnType<typeof readdir>>;
  try {
    entries = await readdir(root, { withFileTypes: true });
  } catch {
    return out;
  }

  for (const entry of entries) {
    if (!entry.isDirectory()) continue;

    const dirPath = resolve(root, entry.name);
    const skillPath = resolve(dirPath, "SKILL.md");
    try {
      const statResult = await stat(skillPath);
      if (!statResult.isFile()) continue;
    } catch {
      continue;
    }

    let frontmatter: { name?: string; description?: string } | null = null;
    try {
      const raw = await readFile(skillPath, "utf8");
      frontmatter = parseFrontmatterFromSkillMd(raw);
    } catch {
      continue;
    }

    const name = (frontmatter?.name || entry.name).trim();
    if (!name) continue;

    out.push({
      name,
      description: frontmatter?.description || "",
      filePath: skillPath,
    });
  }

  return out;
}

async function loadSkillCandidatesForWorkspace(workspaceDir: string): Promise<SkillCandidateCache> {
  const now = Date.now();
  const cached = workspaceSkillCache.get(workspaceDir);
  if (cached && now - cached.fetchedAt < ROUTER_CACHE_TTL_MS) {
    return cached;
  }

  const roots = [
    resolve(workspaceDir, "skills"),
    resolveHomePath("~/.openclaw/skills"),
  ];

  const bundleRoot = (() => {
    try {
      const packagePath = require.resolve("openclaw/package.json");
      return resolve(dirname(packagePath), "skills");
    } catch {
      return undefined;
    }
  })();

  if (bundleRoot) {
    roots.push(bundleRoot);
  }

  const seen = new Map<string, { priority: number; candidate: SkillCandidate }>();

  for (let r = 0; r < roots.length; r += 1) {
    const root = roots[r];
    const candidates = await collectSkillCandidatesFromRoot(root);

    for (const candidate of candidates) {
      const key = candidate.name.toLowerCase();
      const existing = seen.get(key);
      if (!existing || existing.priority > r) {
        seen.set(key, { priority: r, candidate });
      }
    }
  }

  const candidates = [...seen.values()].map((entry) => entry.candidate);

  // Build BM25 IDF table from skill descriptions
  const { idf, avgDescLen } = buildIdfTable(candidates);

  const cacheEntry: SkillCandidateCache = { fetchedAt: now, candidates, idf, avgDescLen };
  workspaceSkillCache.set(workspaceDir, cacheEntry);
  return cacheEntry;
}

function tokenize(text: string): string[] {
  return text
    .toLowerCase()
    .replace(/[.,;:!?\'"()\[\]{}]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .split(/\s+/)
    .filter((t) => t.length > 2);
}

function buildIdfTable(candidates: SkillCandidate[]): { idf: Map<string, number>; avgDescLen: number } {
  const N = candidates.length;
  const docFreq = new Map<string, number>();
  let totalDescLen = 0;

  for (const candidate of candidates) {
    const descTokens = tokenize(candidate.description);
    totalDescLen += descTokens.length;
    const tokens = new Set(descTokens);
    for (const token of tokens) {
      docFreq.set(token, (docFreq.get(token) || 0) + 1);
    }
  }

  const idf = new Map<string, number>();
  for (const [term, df] of docFreq) {
    // Standard BM25 IDF: log((N - df + 0.5) / (df + 0.5) + 1)
    idf.set(term, Math.log((N - df + 0.5) / (df + 0.5) + 1));
  }

  return { idf, avgDescLen: N > 0 ? totalDescLen / N : 1 };
}

function scoreSkill(
  taskText: string,
  skill: { name: string; description: string },
  keywords: Record<string, string[]>,
  idf: Map<string, number>,
  avgDescLen: number,
) {
  const taskTokens = tokenize(taskText);
  const descTokens = tokenize(skill.description);

  let score = 0;
  let reason = "";

  // Name match bonus (still valuable — distinctive signal)
  if (taskText.toLowerCase().includes(skill.name.toLowerCase())) {
    score += 10;
    reason = "name_match";
  }

  // BM25 scoring: how well does the task text match this skill's description?
  // Parameters: k1 controls term frequency saturation, b controls length normalization
  const k1 = 1.5;
  const b = 0.75;
  const dl = descTokens.length || 1;
  const avgdl = avgDescLen || 1;

  // Build term frequency map for the description
  const descTf = new Map<string, number>();
  for (const token of descTokens) {
    descTf.set(token, (descTf.get(token) || 0) + 1);
  }

  // Score each query (task) term against the description document
  let bm25 = 0;
  for (const term of new Set(taskTokens)) {
    const tf = descTf.get(term) || 0;
    if (tf === 0) continue;

    const termIdf = idf.get(term) || 0;
    const numerator = tf * (k1 + 1);
    const denominator = tf + k1 * (1 - b + b * (dl / avgdl));
    bm25 += termIdf * (numerator / denominator);
  }

  score += bm25;
  if (bm25 > 0 && !reason) reason = "bm25";

  // Config keyword hits (+5 per keyword — manual boost for known associations)
  const skillKeywords = keywords[skill.name.toLowerCase()] || [];
  for (const kw of skillKeywords) {
    if (taskText.toLowerCase().includes(kw.toLowerCase())) {
      score += 5;
      if (!reason) reason = "keyword_match";
    }
  }

  return { score, reason: reason || "none" };
}

function formatNudge(skills: SkillCandidate[]): string {
  const noun = skills.length === 1 ? "this skill" : "these skills";
  const lines = [
    `[skill-router] Based on your current task, you likely need ${noun}:`,
  ];

  for (const skill of skills) {
    const desc = (skill.description || "").replace(/\s+/g, " ").trim();
    lines.push(`  → ${skill.name}: "${desc || "No description available."}"`);
    lines.push(`    Location: ${normalizePathForDisplay(skill.filePath)}`);
    lines.push("    Read it with the read tool before proceeding.");
  }

  return lines.join("\n");
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

  const insertNudge = db.prepare(`
    INSERT INTO skill_nudges (
      session_key,
      session_id,
      agent_id,
      skill_name,
      skill_path,
      score,
      match_reason,
      turn_number,
      task_excerpt
    ) VALUES (
      @session_key,
      @session_id,
      @agent_id,
      @skill_name,
      @skill_path,
      @score,
      @match_reason,
      @turn_number,
      @task_excerpt
    )
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
    insertNudge: (params) => {
      try {
        insertNudge.run(params);
      } catch {
        // best effort
      }
    },
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

    CREATE TABLE IF NOT EXISTS skill_nudges (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      session_key TEXT,
      session_id TEXT,
      agent_id TEXT,
      skill_name TEXT NOT NULL,
      skill_path TEXT,
      score REAL,
      match_reason TEXT,
      turn_number INTEGER,
      task_excerpt TEXT,
      timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_nudges_session ON skill_nudges(session_key);
    CREATE INDEX IF NOT EXISTS idx_nudges_skill ON skill_nudges(skill_name);
    CREATE INDEX IF NOT EXISTS idx_nudges_time ON skill_nudges(timestamp);
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

import type { OpenClawPluginDefinition } from "openclaw/plugin-sdk";

const plugin: OpenClawPluginDefinition = {
  id: "skill-usage-audit",
  name: "Skill Usage Audit",
  description: "Writes tool and skill usage telemetry to SQLite for audit and self-improving skill lifecycle.",
  register(api: OpenClawPluginApi) {
  const log = api.logger;
  const cfg = (api.pluginConfig as PluginConfig) || {};

  const includeToolParams = cfg.includeToolParams ?? DEFAULT_INCLUDE_TOOL_PARAMS;
  const captureMessageContent = cfg.captureMessageContent === undefined ? DEFAULT_CAPTURE_MESSAGE_CONTENT : cfg.captureMessageContent === true;
  const redactKeys = new Set((Array.isArray(cfg.redactKeys) ? cfg.redactKeys : DEFAULT_REDACT_KEYS).map((k) => String(k).toLowerCase()));
  const contextWindowSize = parseIntConfig(cfg.contextWindowSize, DEFAULT_CONTEXT_WINDOW_SIZE, 1);
  const contextTimeoutMs = parseIntConfig(cfg.contextTimeoutMs, DEFAULT_CONTEXT_TIMEOUT_MS, 0);
  const detectSkillBlocks = cfg.skillBlockDetection !== false;
  const dbPath = resolveDbPath(cfg.dbPath);

  const routerConfig = cfg.router || {} as RouterConfig;
  const routerEnabled = parseBooleanConfig(routerConfig.enabled, true);
  const routerTargets = routerConfig.targets || {};
  const routerTargetSubagent = parseBooleanConfig(routerTargets.subagent, true);
  const routerTargetCron = parseBooleanConfig(routerTargets.cron, true);
  const routerMaxSkillsToNudge = parseIntConfig(routerConfig.maxSkillsToNudge, DEFAULT_ROUTER_MAX_SKILLS, 1);
  const routerMinScore = parseFloatConfig(routerConfig.minScore, DEFAULT_ROUTER_MIN_SCORE, 0);
  const routerRecencyWindow = parseIntConfig(routerConfig.recencyWindow, DEFAULT_ROUTER_RECENCY_WINDOW, 1);
  const routerOverrides = parseOverrideRules(routerConfig.overrides).map((entry) => {
    try {
      return { ...entry, matcher: new RegExp(entry.taskPattern, "i") };
    } catch (error) {
      log.error(`skill-usage-audit: invalid override taskPattern: ${String(entry.taskPattern)} (${String(error)})`);
      return undefined;
    }
  }).filter((entry): entry is RouterOverride & { matcher: RegExp } => Boolean(entry));
  const routerSkillKeywords = parseSkillKeywords(routerConfig.skillKeywords);
  const routerBlocklist = new Set(parseBlocklist(routerConfig.blocklist));
  const pluginWorkspaceDir = resolveWorkspaceDir();

  const dbState: DbState = { backend: null, statements: null };
  const dbInitPromise = initSqlite(dbPath, log);
  let dbChain = Promise.resolve<void>(undefined);
  let hasLoggedDbIssue = false;
  const DB_QUEUE_DROP_THRESHOLD = 500;
  let dbQueueDepth = 0;
  let hasLoggedDbQueueDrop = false;

  let shutdownInProgress = false;
  let shutdownPromise: Promise<void> | null = null;

  const messageHistory = new Map<string, MessageHistoryEntry>();
  let messageHistoryCounter = 0;

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

  function scheduleDbWrite(label: string, critical: boolean, write: () => Promise<void> | void): void {
    if (shutdownInProgress) return;

    if (!critical && dbQueueDepth >= DB_QUEUE_DROP_THRESHOLD) {
      if (!hasLoggedDbQueueDrop) {
        hasLoggedDbQueueDrop = true;
        log.info(`skill-usage-audit: db queue backlog high (${dbQueueDepth}); dropping non-critical inserts`);
      }
      return;
    }

    dbQueueDepth += 1;
    dbChain = dbChain
      .then(async () => write())
      .catch((err) => {
        log.error(`skill-usage-audit: ${label}: ${String(err)}`);
      })
      .finally(() => {
        dbQueueDepth = Math.max(0, dbQueueDepth - 1);
        if (dbQueueDepth < DB_QUEUE_DROP_THRESHOLD) {
          hasLoggedDbQueueDrop = false;
        }
      });
  }

  function queueDbInsert(rowType: RawEvent): void {
    if (shutdownInProgress) return;
    if (rowType.type !== "session_start" && rowType.type !== "session_end" && rowType.type !== "tool_call_start" && rowType.type !== "tool_call_end" && rowType.type !== "skill_file_read" && rowType.type !== "skill_block_detected") {
      return;
    }

    scheduleDbWrite("event insert failed", false, async () => {
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
        success:
          typeof rowType.success === "boolean"
            ? (rowType.success ? 1 : 0)
            : typeof rowType.success === "number" && Number.isFinite(rowType.success)
              ? (rowType.success ? 1 : 0)
              : null,
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
    });
  }

  function queueSkillVersionWrite(skillName: string, skillPath: string, ts: string, versionHash: string | null | undefined): void {
    if (shutdownInProgress) return;

    scheduleDbWrite("failed writing skill version", true, async () => {
      const state = await ensureDbReady();
      if (!state.statements) return;

      const hash = versionHash ?? (await computeSkillVersionHash(skillPath));
      if (!hash) return;

      state.statements.insertVersion({
        skill_name: skillName,
        skill_path: normalizeSkillExecutionPath(skillPath) || resolve(skillPath),
        version_hash: hash,
        first_seen_at: ts,
        notes: null,
      });

      state.statements.upsertSkill({
        skill_name: skillName,
        skill_path: normalizeSkillExecutionPath(skillPath) || resolve(skillPath),
        current_version_hash: hash,
        status: "stable",
        last_modified_at: ts,
        last_used_at: ts,
      });
    });
  }

  function queueNudgeInsert(params: {
    event: RawEvent;
    ctx: RawContext | undefined;
    skillName: string;
    skillPath: string | undefined;
    score: number;
    matchReason: string;
    turnNumber: number | null;
    taskExcerpt: string;
  }): void {
    if (shutdownInProgress) return;

    scheduleDbWrite("failed writing nudge", true, async () => {
      const state = await ensureDbReady();
      if (!state.statements) return;

      const base = buildBase(params.event as { sessionId?: string; runId?: string }, params.ctx);
      state.statements.insertNudge({
        session_key: base.sessionKey || null,
        session_id: base.sessionId || null,
        agent_id: base.agentId || null,
        skill_name: params.skillName,
        skill_path: params.skillPath || null,
        score: params.score,
        match_reason: params.matchReason,
        turn_number: params.turnNumber,
        task_excerpt: params.taskExcerpt,
      });
    });
  }

  function cleanupMessageHistory(): void {
    const now = Date.now();

    for (const [scope, entry] of messageHistory.entries()) {
      if (now - entry.lastSeenAt > MESSAGE_HISTORY_STALE_MS) {
        messageHistory.delete(scope);
      }
    }

    if (messageHistory.size <= MESSAGE_HISTORY_MAX_SCOPES) return;

    const sortedScopes = [...messageHistory.entries()].sort((a, b) => a[1].lastSeenAt - b[1].lastSeenAt);
    while (messageHistory.size > MESSAGE_HISTORY_MAX_SCOPES) {
      const oldest = sortedScopes.shift();
      if (!oldest) break;
      messageHistory.delete(oldest[0]);
    }
  }

  function enqueueEvent(event: RawEvent): void {
    queueDbInsert(event);

    messageHistoryCounter += 1;
    if (messageHistoryCounter >= MESSAGE_HISTORY_CLEANUP_EVERY) {
      messageHistoryCounter = 0;
      cleanupMessageHistory();
    }
  }

  function addMessage(scope: string, message: MessageCapture): void {
    const slot = messageHistory.get(scope) || { messages: [], lastSeenAt: Date.now() };
    slot.messages.push(message);
    if (slot.messages.length > MAX_HISTORY_PER_SCOPE) {
      slot.messages.splice(0, slot.messages.length - MAX_HISTORY_PER_SCOPE);
    }
    slot.lastSeenAt = Date.now();
    messageHistory.set(scope, slot);

    messageHistoryCounter += 1;
    if (messageHistoryCounter >= MESSAGE_HISTORY_CLEANUP_EVERY) {
      messageHistoryCounter = 0;
      cleanupMessageHistory();
    }
  }

  function getRecentMessages(scopeKeys: string[], windowSize: number): MessageCapture[] {
    const all: MessageCapture[] = [];
    for (const key of scopeKeys) {
      const entry = messageHistory.get(key);
      if (entry) all.push(...entry.messages);
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

    scheduleDbWrite(`failed inserting execution ${executionId} (${reason})`, true, async () => {
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

    const toolName = toStringLike(event.toolName) || "";
    const params =
      typeof event.params === "object" && event.params !== null
        ? event.params as Record<string, unknown>
        : undefined;
    const isSkillRead = toolName === "read" && extractSkillPathFromParams(params || {}) !== undefined;

    if (!isSkillRead) {
      target.hadToolCall = true;
    }

    target.toolReadCount += 1;

    const callId = toStringLike(event.toolCallId)
      ? `tool:${toStringLike(event.toolCallId)}`
      : `anon:${target.id}:${target.toolReadCount}`;
    target.inFlightToolCalls.add(callId);

    execByTool.set(callId, target.id);

    if (toolName === "read" && params) {
      const rawPath = extractSkillPathFromParams(params);
      if (rawPath) {
        const inferredSkillName = inferSkillName(rawPath);
        syncExecutionPath(target, inferredSkillName, rawPath);

        const normalized = normalizeSkillExecutionPath(rawPath);
        if (normalized) {
          target.skillReadCount = (target.skillReadCount || 0) + 1;
          if (normalized === target.skillPath) {
            if (target.skillReadCount > 1) target.sameSkillRetried = true;
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
      skillReadCount: 0,
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

  async function maybeNudgeSkills(event: RawEvent, ctx: RawContext): Promise<{ prependContext: string } | undefined> {
    if (!routerEnabled) return;

    const sessionKey = toStringLike(ctx?.sessionKey) || "";
    const isSubagent = sessionKey.includes(":subagent:");
    const isCron = toStringLike(ctx?.trigger) === "cron";

    const shouldRoute =
      (isSubagent && routerTargetSubagent) ||
      (isCron && routerTargetCron);

    if (!shouldRoute) return;

    const prompt = toStringLike((event as RawEvent).prompt) || "";
    const messageList = Array.isArray((event as RawEvent).messages) ? (event as RawEvent).messages : [];
    const turnNumber = messageList.length ? messageIndexForTask(messageList, prompt) : null;

    const taskText = prompt || messageList.map((m) => getTextFromMessage(m)).join("\n").trim();
    if (!taskText) return;

    const parsedBlocks = parseSkillBlock(prompt);
    if (parsedBlocks.count > 0) return;

    const configuredWorkspaceDir = toStringLike(ctx?.workspaceDir);
    const workspaceDir = configuredWorkspaceDir && configuredWorkspaceDir.trim().length > 0 ? configuredWorkspaceDir : pluginWorkspaceDir;
    const skillCache = await loadSkillCandidatesForWorkspace(workspaceDir);
    const candidates = skillCache.candidates;
    const { idf, avgDescLen } = skillCache;
    if (!candidates.length) return;

    const blocklist = new Set(routerBlocklist);

    const candidateByName = new Map<string, SkillCandidate>();
    for (const candidate of candidates) {
      candidateByName.set(candidate.name.toLowerCase(), candidate);
    }

    const availableCandidates = candidates.filter((skill) => !blocklist.has(skill.name.toLowerCase()));
    if (!availableCandidates.length) return;

    const selected: SkillCandidate[] = [];
    const seen = new Set<string>();
    const hasOverride = [] as SkillCandidate[];

    for (const override of routerOverrides) {
      if (!override.matcher.test(taskText)) continue;

      for (const skillName of override.skills || []) {
        const skill = candidateByName.get(String(skillName).toLowerCase());
        if (!skill) continue;
        if (blocklist.has(skill.name.toLowerCase())) continue;
        if (seen.has(skill.name.toLowerCase())) continue;
        if (wasSkillHandledRecently(messageList, skill.name, skill.filePath, routerRecencyWindow)) continue;

        hasOverride.push(skill);
        seen.add(skill.name.toLowerCase());
        if (hasOverride.length >= routerMaxSkillsToNudge) break;
      }

      if (hasOverride.length >= routerMaxSkillsToNudge) break;
    }

    if (hasOverride.length) {
      selected.push(...hasOverride);
    } else {
      const scored = availableCandidates
        .map((skill) => ({
          skill,
          ...scoreSkill(taskText, skill, routerSkillKeywords, idf, avgDescLen),
        }))
        .filter((row) => row.score >= routerMinScore)
        .sort((a, b) => {
          if (b.score !== a.score) return b.score - a.score;
          return a.skill.name.localeCompare(b.skill.name);
        });

      for (const row of scored) {
        if (wasSkillHandledRecently(messageList, row.skill.name, row.skill.filePath, routerRecencyWindow)) continue;
        const key = row.skill.name.toLowerCase();
        if (seen.has(key)) continue;

        selected.push(row.skill);
        seen.add(key);
        if (selected.length >= routerMaxSkillsToNudge) break;
      }
    }

    if (!selected.length) return;

    const taskExcerpt = sanitizeValue(scrubSecrets(taskText), 200);
    for (const skill of selected) {
      const isOverride = hasOverride.some((entry) => entry.name.toLowerCase() === skill.name.toLowerCase());
      const row = isOverride ? { score: 0, reason: "override" } : scoreSkill(taskText, skill, routerSkillKeywords, idf, avgDescLen);

      queueNudgeInsert({
        event,
        ctx,
        skillName: skill.name,
        skillPath: skill.filePath,
        score: row.score,
        matchReason: row.reason,
        turnNumber,
        taskExcerpt,
      });
    }

    return { prependContext: formatNudge(selected) };
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
      const normalized = normalizeSkillExecutionPath(skillPath) || resolve(skillPath);
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
      success: (event as RawEvent).error ? 0 : 1,
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
    const routerContext = await maybeNudgeSkills(event, ctx);

    if (detectSkillBlocks) {
      const prompt = toStringLike((event as RawEvent).prompt);
      if (prompt) {
        const info = parseSkillBlock(prompt);
        if (info.count) {
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
        }
      }
    }

    if (routerContext) {
      return routerContext;
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

  // Use gateway_stop lifecycle hook instead of process signal handlers.
  // Calling process.exit() from an in-process plugin can short-circuit
  // the gateway's own shutdown ordering and destabilize other plugins.
  api.on("gateway_stop", async () => {
    await requestFlush("gateway_stop");
  });

  // eager init for logs
  void ensureDbReady();
  log.info("skill-usage-audit plugin registered");
  },
};

export default plugin;
