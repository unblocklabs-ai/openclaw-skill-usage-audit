/**
 * Skill usage audit plugin
 */
import { mkdir, readdir, readFile, stat } from "node:fs/promises";
import { dirname, basename, resolve, join, relative, sep, isAbsolute } from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import { createHash } from "node:crypto";
import { definePluginEntry } from "openclaw/plugin-sdk/plugin-entry";
import { buildSkillIdentityMap, canonicalSkillId, isCandidateAllowedForAgent, isCandidateDisabledByEntries, isSkillBlocked, normalizedSkillListIncludes, parseSkillKeyFromFrontmatter, resolveAgentSkillAllowlist, resolveEffectiveAgentId, resolveRouterTargetConfig, selectRouterTaskText, shouldSuppressRecentNudgeRecord, } from "./skill-router-helpers.mjs";
import { buildSkillRootSpecs, normalizeStringList, readOpenClawConfig, resolveBundledSkillsRoot, resolveCodexHome, resolveHomePath as resolveConfigHomePath, } from "./skill-roots.mjs";
const require = createRequire(import.meta.url);
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const DEFAULT_DB_PATH = "~/.openclaw/audits/skill-usage.db";
const DEFAULT_INCLUDE_TOOL_PARAMS = false;
const DEFAULT_CAPTURE_MESSAGE_CONTENT = false;
const DEFAULT_CONTEXT_WINDOW_SIZE = 5;
const DEFAULT_CONTEXT_TIMEOUT_MS = 60000;
const MAX_MESSAGE_LEN = 200;
const MAX_HISTORY_PER_SCOPE = 80;
const DEFAULT_ROUTER_MAX_SKILLS = 1;
const DEFAULT_ROUTER_MIN_SCORE = 6;
const DEFAULT_ROUTER_AGENT_MIN_SCORE = 8;
const DEFAULT_ROUTER_RECENCY_WINDOW = 10;
const DEFAULT_ROUTER_RECENCY_FALLBACK_MINUTES = 30;
const DEFAULT_ROUTER_LOOKBACK_MESSAGES = 8;
const DEFAULT_ROUTER_DISCOVERY_GROUP_DEPTH = 1;
const DEFAULT_ROUTER_OBSERVABILITY_TOP_CANDIDATES = 5;
const DEFAULT_ROUTER_OBSERVABILITY_RETENTION_DAYS = 30;
const MAX_ROUTER_DISCOVERY_GROUP_DEPTH = 3;
const MAX_ROUTER_OBSERVABILITY_TOP_CANDIDATES = 20;
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
const SECRET_PATTERNS = [
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
    /that didn['']?t work/i,
    /no that['']?s not/i,
    /\bredo\b/i,
    /\bincorrect\b/i,
    /did not/i,
];
// Patterns that only match when message role === "user" (too noisy on assistant messages)
const USER_ONLY_NEGATIVE_PATTERNS = [
    /\bfix\b/i,
    /\bbroken\b/i,
    /\bbad\b/i,
];
const skillUsageAuditGlobalAccessor = globalThis;
skillUsageAuditGlobalAccessor.__skillUsageAuditState ??= {
    db: { backend: null, statements: null },
    dbInitPromise: null,
    dbPath: null,
    dbChain: Promise.resolve(undefined),
    dbQueueDepth: 0,
    hasLoggedDbQueueDrop: false,
    acceptingWrites: true,
    shutdownPromise: null,
    shutdownParticipants: new Set(),
    registeredFullApis: new WeakSet(),
    skipModeLog: new Set(),
    warnedDbPaths: new Set(),
};
function resolveHomePath(pathLike) {
    return resolveConfigHomePath(pathLike);
}
function isHomeRelativePath(pathLike) {
    return pathLike === "~" || pathLike.startsWith("~/") || pathLike.startsWith("~\\");
}
function canonicalSkillPath(rawPath) {
    if (!rawPath)
        return undefined;
    const trimmed = rawPath.trim();
    if (!trimmed)
        return undefined;
    if (isHomeRelativePath(trimmed))
        return resolveHomePath(trimmed);
    if (isAbsolute(trimmed))
        return resolve(trimmed);
    return undefined;
}
function getSkillNameFromReference(skillRef) {
    const normalized = skillRef.replace(/\\/g, "/");
    const trimmed = normalized.trim();
    if (!trimmed)
        return "unknown";
    if (trimmed.toLowerCase().endsWith("skill.md")) {
        const parts = trimmed.split("/").filter(Boolean);
        if (parts.length >= 2) {
            return parts[parts.length - 2] || "unknown";
        }
    }
    const parts = trimmed.split("/").filter(Boolean);
    return parts[parts.length - 1] || "unknown";
}
function resolveDbPath(pathLike) {
    return resolveHomePath(pathLike && pathLike.trim().length > 0 ? pathLike : DEFAULT_DB_PATH);
}
function toStringLike(value) {
    if (typeof value !== "string")
        return undefined;
    const t = value.trim();
    return t.length ? t : undefined;
}
function parseIntConfig(value, fallback, min) {
    const parsed = typeof value === "number"
        ? Math.floor(value)
        : Number.parseInt(typeof value === "string" ? value.trim() : "", 10);
    if (!Number.isFinite(parsed))
        return fallback;
    if (min !== undefined && parsed < min)
        return fallback;
    return parsed;
}
function clampIntConfig(value, fallback, min, max) {
    const parsed = parseIntConfig(value, fallback, min);
    return Math.min(parsed, max);
}
function parseBooleanConfig(value, fallback) {
    return typeof value === "boolean" ? value : fallback;
}
function parseStringConfig(value, fallback, allowed) {
    const parsed = typeof value === "string" ? value.trim() : "";
    return allowed.has(parsed) ? parsed : fallback;
}
function normalizeText(value, redactKeys, piiEnabled = true) {
    const text = typeof value !== "string" ? String(value ?? "") : value;
    return sanitizeValue(redactParams(text, redactKeys, piiEnabled), MAX_MESSAGE_LEN, piiEnabled);
}
function sanitizeValue(value, maxLen = 400, piiEnabled = true) {
    if (value === null || value === undefined)
        return "";
    if (typeof value === "string") {
        const scrubbed = scrubSecrets(value, piiEnabled);
        return scrubbed.length <= maxLen
            ? scrubbed
            : `${scrubbed.slice(0, maxLen)}…[truncated ${scrubbed.length - maxLen} chars]`;
    }
    if (Array.isArray(value)) {
        return JSON.stringify(value.map((entry) => sanitizeValue(entry, maxLen, piiEnabled))).slice(0, maxLen);
    }
    if (typeof value === "object") {
        return JSON.stringify(redactParams(value, new Set(), piiEnabled)).slice(0, maxLen);
    }
    return String(value).slice(0, maxLen);
}
function redactUrlsWithQuery(value) {
    return value.replace(URL_WITH_QUERY_PATTERN, (match) => {
        const idx = match.indexOf("?");
        if (idx < 0)
            return match;
        return `${match.slice(0, idx)}[query-removed]`;
    });
}
function scrubTokensOnly(value) {
    return SECRET_PATTERNS.reduce((next, pattern) => next.replace(pattern, "[REDACTED]"), value);
}
function scrubPIIFromValue(value) {
    const cleaned = sanitizePII(EMAIL_PATTERN, value);
    const scrubbedQuery = redactUrlsWithQuery(cleaned);
    return sanitizePII(PHONE_PATTERN, scrubbedQuery);
}
function scrubSecrets(value, piiEnabled = true) {
    const tokenScrubbed = scrubTokensOnly(value);
    return piiEnabled ? scrubPIIFromValue(tokenScrubbed) : tokenScrubbed;
}
function sanitizePII(pattern, value) {
    return value.replace(pattern, "[REDACTED]");
}
function shouldRedactKey(key, redactKeys) {
    const candidate = key.toLowerCase();
    if (redactKeys.has(candidate))
        return true;
    return candidate.includes("token") || candidate.includes("secret") || candidate.includes("auth") || candidate.includes("password");
}
function redactParams(value, redactKeys, piiEnabled = true) {
    if (value === null || value === undefined)
        return value;
    if (typeof value === "string")
        return scrubSecrets(value, piiEnabled);
    if (Array.isArray(value))
        return value.map((entry) => redactParams(entry, redactKeys, piiEnabled));
    if (typeof value !== "object")
        return value;
    const entries = value;
    const out = {};
    for (const [k, v] of Object.entries(entries)) {
        out[k] = shouldRedactKey(k, redactKeys) ? "[REDACTED]" : redactParams(v, redactKeys, piiEnabled);
    }
    return out;
}
function buildToolParams(toolName, params, redactKeys, piiEnabled = true) {
    if (toolName === "write" || toolName === "message") {
        const scrubbed = redactParams(params, redactKeys, piiEnabled);
        if (typeof scrubbed === "object" && scrubbed && !Array.isArray(scrubbed)) {
            const copy = { ...scrubbed };
            if (Object.prototype.hasOwnProperty.call(copy, "text"))
                copy.text = "[REDACTED]";
            if (Object.prototype.hasOwnProperty.call(copy, "content"))
                copy.content = "[REDACTED]";
            if (Object.prototype.hasOwnProperty.call(copy, "body"))
                copy.body = "[REDACTED]";
            if (Object.prototype.hasOwnProperty.call(copy, "data"))
                copy.data = "[REDACTED]";
            if (Object.prototype.hasOwnProperty.call(copy, "message"))
                copy.message = "[REDACTED]";
            return copy;
        }
        return scrubbed;
    }
    if (toolName === "exec" && typeof params === "object" && params) {
        const copy = { ...params, command: "[REDACTED]" };
        return copy;
    }
    return redactParams(params, redactKeys, piiEnabled);
}
function detectMessageSignals(execution, text, role) {
    const lower = text.toLowerCase();
    const labels = new Set();
    if (NEGATIVE_MESSAGE_PATTERNS.some((pattern) => pattern.test(lower))) {
        labels.add("negative_phrase_detected");
    }
    if (role === "user" && USER_ONLY_NEGATIVE_PATTERNS.some((pattern) => pattern.test(lower))) {
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
function isNegativeSignal(signals) {
    return signals.includes("negative_phrase_detected") || signals.includes("skill_retry_detected") || signals.includes("fallback_or_skill_switch_detected");
}
function makeMessageCapture(text, role, metadata, redactKeys, captureContent, signalLabels = [], piiEnabled = true) {
    if (typeof text !== "string")
        return null;
    const snap = {
        ts: new Date().toISOString(),
        role,
        length: text.length,
        signalLabels: signalLabels.length ? [...new Set(signalLabels)] : undefined,
    };
    if (captureContent) {
        const safeText = normalizeText(text, redactKeys, piiEnabled);
        snap.text = safeText;
        if (metadata !== undefined) {
            snap.metadata = normalizeText(metadata, redactKeys, piiEnabled);
        }
    }
    return snap;
}
function inferSkillName(skillPath) {
    const canonical = canonicalSkillPath(skillPath);
    if (canonical) {
        if (basename(canonical).toLowerCase() === "skill.md") {
            return basename(dirname(canonical));
        }
        return basename(canonical);
    }
    return getSkillNameFromReference(skillPath);
}
function inferSkillSource(skillPath) {
    const abs = canonicalSkillPath(skillPath);
    if (!abs)
        return "unknown";
    const home = resolve(process.env.HOME || process.env.USERPROFILE || "");
    const codexHome = resolveCodexHome();
    if (abs.includes(`${codexHome}/plugins/cache`) || abs.includes(`${codexHome}/plugins/cache/`))
        return "codex-plugin";
    if (abs.includes(`${codexHome}/skills`) || abs.includes(`${codexHome}/skills/`))
        return "codex";
    if (abs.includes(`${home}/.openclaw/extensions`) || abs.includes(`${home}/.openclaw/extensions/`))
        return "extension";
    if (abs.includes(`${home}/.openclaw/plugin-skills`) || abs.includes(`${home}/.openclaw/plugin-skills/`))
        return "extension";
    if (abs.includes(`${home}/.openclaw/skills`) || abs.includes(`${home}/.openclaw/skills/`))
        return "managed";
    if (abs.includes("/skills/") || abs.includes("\\skills\\"))
        return "workspace";
    return "unknown";
}
function extractSkillPathFromParams(params) {
    const candidates = [
        params.path,
        params.file_path,
        params.filePath,
        params.target,
        params.targetPath,
    ];
    for (const candidate of candidates) {
        const p = toStringLike(candidate);
        if (!p)
            continue;
        if (basename(p).toLowerCase() === "skill.md")
            return p;
    }
    return undefined;
}
function buildBase(event, ctx) {
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
function buildScopeKeys(ctx, event) {
    const keys = [];
    const add = (key) => {
        if (key && !keys.includes(key))
            keys.push(key);
    };
    const metadata = event?.metadata || {};
    const channel = toStringLike(ctx?.channelId) || toStringLike(metadata.channelId) || "unknown";
    const conv = toStringLike(ctx?.conversationId) ||
        toStringLike(metadata.conversationId) ||
        toStringLike(metadata.threadTs) ||
        toStringLike(metadata.threadId);
    const account = toStringLike(ctx?.accountId) || toStringLike(metadata.accountId);
    const sessionKey = toStringLike(ctx?.sessionKey) || toStringLike(metadata.sessionKey);
    const sessionId = toStringLike(ctx?.sessionId);
    const runId = toStringLike(ctx?.runId) || toStringLike(metadata.runId) || toStringLike(event?.runId);
    if (sessionKey)
        add(`sk:${sessionKey}`);
    if (runId)
        add(`run:${runId}`);
    if (sessionId)
        add(`sid:${sessionId}`);
    if (conv) {
        add(`conv:${channel}:${conv}`);
        if (account)
            add(`conv:${channel}:${account}:${conv}`);
    }
    if (account)
        add(`acct:${channel}:${account}`);
    if (channel)
        add(`ch:${channel}`);
    if (!keys.length)
        add("global");
    return keys;
}
function buildMessageScope(ctx, event) {
    const scopeCandidates = buildScopeKeys(ctx, event);
    return scopeCandidates.find((key) => key.startsWith("conv:")) || scopeCandidates.find((key) => key.startsWith("acct:")) || scopeCandidates[0];
}
const workspaceSkillCache = new Map();
function resolveWorkspaceDir(config) {
    // 1. Try reading workspace from config (most reliable)
    if (config?.agents) {
        // Check agent-specific workspace first (main), then defaults
        const mainWorkspace = config.agents.main?.workspace;
        if (mainWorkspace && mainWorkspace.trim())
            return resolveHomePath(mainWorkspace);
        const defaultWorkspace = config.agents.defaults?.workspace;
        if (defaultWorkspace && defaultWorkspace.trim())
            return resolveHomePath(defaultWorkspace);
    }
    // 2. Fallback: derive from __dirname (works when plugin is inside workspace/.openclaw/extensions/)
    const extDir = resolve(__dirname);
    const marker = `${sep}.openclaw${sep}`;
    const markerIdx = extDir.indexOf(marker);
    if (markerIdx >= 0) {
        const candidate = extDir.slice(0, markerIdx);
        return candidate || process.cwd();
    }
    return process.cwd();
}
function routerDiscoveryCacheKey(workspaceDir, config, bundledRoot, discovery) {
    return JSON.stringify({
        workspaceDir: resolve(workspaceDir),
        bundledRoot: bundledRoot || null,
        codexHome: resolveCodexHome(),
        discovery,
        allowBundled: config?.skills?.allowBundled ?? null,
        entries: config?.skills?.entries ?? null,
        extraDirs: config?.skills?.load?.extraDirs ?? null,
        pluginSkills: resolveHomePath("~/.openclaw/plugin-skills"),
    });
}
function buildRouterDiscoveryConfig(discovery) {
    return {
        workspace: parseBooleanConfig(discovery?.workspace, true),
        agentsProject: parseBooleanConfig(discovery?.agentsProject, true),
        agentsPersonal: parseBooleanConfig(discovery?.agentsPersonal, true),
        openclawManaged: parseBooleanConfig(discovery?.openclawManaged, true),
        openclawBundled: parseBooleanConfig(discovery?.openclawBundled, true),
        extraDirs: parseBooleanConfig(discovery?.extraDirs, true),
        openclawPluginSkills: parseBooleanConfig(discovery?.openclawPluginSkills, true),
        codex: parseBooleanConfig(discovery?.codex, true),
        codexPlugin: parseBooleanConfig(discovery?.codexPlugin, true),
        groupDepth: clampIntConfig(discovery?.groupDepth, DEFAULT_ROUTER_DISCOVERY_GROUP_DEPTH, 0, MAX_ROUTER_DISCOVERY_GROUP_DEPTH),
    };
}
function parseRouterTextArray(value) {
    if (!Array.isArray(value))
        return [];
    return value
        .map((entry) => (typeof entry === "string" ? entry.trim() : ""))
        .filter((entry) => entry.length > 0);
}
function parseSkillKeywords(value) {
    const out = {};
    if (!value || typeof value !== "object" || Array.isArray(value))
        return out;
    const obj = value;
    for (const [key, val] of Object.entries(obj)) {
        const parsed = parseRouterTextArray(val);
        if (parsed.length) {
            out[key.toLowerCase()] = parsed;
        }
    }
    return out;
}
function parseOverrideRules(value) {
    if (!Array.isArray(value))
        return [];
    const out = [];
    for (const entry of value) {
        if (!entry || typeof entry !== "object" || Array.isArray(entry))
            continue;
        const pattern = toStringLike(entry.taskPattern);
        const skills = parseRouterTextArray(entry.skills);
        if (pattern && skills.length) {
            out.push({ taskPattern: pattern, skills });
        }
    }
    return out;
}
function parseBlocklist(value) {
    return parseRouterTextArray(value).map((item) => item.toLowerCase());
}
function normalizePathForDisplay(filePath) {
    const home = process.env.HOME || process.env.USERPROFILE;
    if (home && filePath.startsWith(home)) {
        return `~${filePath.slice(home.length)}`;
    }
    return filePath;
}
function getTextFromMessage(message) {
    if (typeof message === "string")
        return message;
    if (!message || typeof message !== "object")
        return undefined;
    const msg = message;
    if (typeof msg.content === "string")
        return msg.content;
    if (msg.content && typeof msg.content === "object") {
        const nested = msg.content;
        if (typeof nested.text === "string")
            return nested.text;
    }
    if (typeof msg.text === "string")
        return msg.text;
    if (typeof msg.body === "string")
        return msg.body;
    if (typeof msg.message === "string")
        return msg.message;
    return undefined;
}
function getRoleFromMessage(message) {
    if (!message || typeof message !== "object" || Array.isArray(message))
        return undefined;
    const msg = message;
    return toStringLike(msg.role)?.toLowerCase();
}
function extractToolNameFromMessage(message) {
    if (!message || typeof message !== "object" || Array.isArray(message))
        return undefined;
    const msg = message;
    return (toStringLike(msg.toolName) ||
        toStringLike(msg.tool_name) ||
        toStringLike(msg.tool))?.toLowerCase();
}
function normalizeCandidatePath(value) {
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
function extractReadPathsFromToolMessage(message) {
    if (!message || typeof message !== "object" || Array.isArray(message))
        return [];
    const toolName = extractToolNameFromMessage(message);
    if (toolName !== "read")
        return [];
    const msg = message;
    const params = msg.params && typeof msg.params === "object" && !Array.isArray(msg.params)
        ? msg.params
        : undefined;
    const out = [];
    const collect = (value) => {
        const text = toStringLike(value);
        if (text)
            out.push(text);
    };
    if (params) {
        collect(params.path);
        collect(params.file_path);
        collect(params.filePath);
    }
    return out;
}
function wasSkillHandledRecently(messages, skill, recencyWindow) {
    if (!messages.length || recencyWindow <= 0)
        return false;
    const lookback = Math.max(1, Math.min(messages.length, recencyWindow));
    const start = messages.length - lookback;
    const skillId = canonicalSkillId(skill);
    const skillName = skill.name;
    const skillFilePath = skill.filePath;
    const normalizedSkillPath = normalizeCandidatePath(skillFilePath);
    const displaySkillPath = normalizePathForDisplay(skillFilePath);
    const skillSuffix = `/${skillName.toLowerCase()}/skill.md`;
    for (let i = messages.length - 1; i >= start; i--) {
        const message = messages[i];
        const text = getTextFromMessage(message);
        if (text && skillId && text.includes(`[skill-router:id ${skillId}]`)) {
            return true;
        }
        if (text &&
            text.includes("[skill-router]") &&
            text.includes(`→ ${skillName}:`) &&
            (text.includes(displaySkillPath) || text.includes(skillFilePath))) {
            return true;
        }
        const candidatePaths = extractReadPathsFromToolMessage(message);
        for (const candidate of candidatePaths) {
            const normalized = normalizeCandidatePath(candidate);
            if (!normalized)
                continue;
            if ((normalized === normalizedSkillPath) || normalized.endsWith(skillSuffix)) {
                return true;
            }
        }
    }
    return false;
}
function messageIndexForTask(messages, prompt) {
    if (!messages.length)
        return null;
    for (let i = messages.length - 1; i >= 0; i -= 1) {
        const text = getTextFromMessage(messages[i]);
        if (typeof text === "string" && text === prompt) {
            return i;
        }
    }
    return messages.length - 1;
}
function parseFrontmatterFromSkillMd(content) {
    const lines = content.split(/\r?\n/).slice(0, ROUTER_FRONTMATTER_MAX_LINES);
    if (!lines.length || lines[0]?.trim() !== "---")
        return null;
    const out = {};
    for (let i = 1; i < lines.length; i += 1) {
        const line = lines[i].trim();
        if (line === "---")
            break;
        if (!line || line.startsWith("#"))
            continue;
        const colon = line.indexOf(":");
        if (colon < 0)
            continue;
        const key = line.slice(0, colon).trim().toLowerCase();
        let value = line.slice(colon + 1).trim();
        if (!key || !value)
            continue;
        if ((value.startsWith("\"") && value.endsWith("\"")) || (value.startsWith("'") && value.endsWith("'"))) {
            value = value.slice(1, -1);
        }
        out[key] = value;
    }
    if (!Object.keys(out).length)
        return null;
    return {
        name: out.name,
        description: out.description,
    };
}
async function collectSkillCandidateFromDir(dirPath, source) {
    const skillPath = resolve(dirPath, "SKILL.md");
    try {
        const statResult = await stat(skillPath);
        if (!statResult.isFile())
            return undefined;
    }
    catch {
        return undefined;
    }
    let frontmatter = null;
    let skillKey;
    try {
        const raw = await readFile(skillPath, "utf8");
        frontmatter = parseFrontmatterFromSkillMd(raw);
        skillKey = parseSkillKeyFromFrontmatter(raw, ROUTER_FRONTMATTER_MAX_LINES);
    }
    catch {
        // Skip skills with unreadable SKILL.md files
        return undefined;
    }
    const name = (frontmatter?.name || basename(dirPath)).trim();
    if (!name)
        return undefined;
    return {
        name,
        key: skillKey || name,
        description: frontmatter?.description || "",
        filePath: skillPath,
        source,
    };
}
async function collectSkillCandidatesFromRoot(root, source, groupDepth) {
    const out = [];
    const rootSkill = await collectSkillCandidateFromDir(root, source);
    if (rootSkill) {
        out.push(rootSkill);
        return out;
    }
    let entries;
    try {
        entries = await readdir(root, { withFileTypes: true });
    }
    catch {
        return out;
    }
    const walk = async (dirPath, remainingGroupDepth) => {
        const directSkill = await collectSkillCandidateFromDir(dirPath, source);
        if (directSkill) {
            out.push(directSkill);
            return;
        }
        if (remainingGroupDepth <= 0)
            return;
        let childEntries;
        try {
            childEntries = await readdir(dirPath, { withFileTypes: true });
        }
        catch {
            return;
        }
        for (const childEntry of childEntries) {
            if (!childEntry.isDirectory())
                continue;
            await walk(resolve(dirPath, childEntry.name), remainingGroupDepth - 1);
        }
    };
    for (const entry of entries) {
        if (!entry.isDirectory())
            continue;
        await walk(resolve(root, entry.name), groupDepth);
    }
    return out;
}
async function readOpenClawConfigSnapshot() {
    try {
        const parsed = readOpenClawConfig();
        if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
            return undefined;
        return parsed;
    }
    catch {
        return undefined;
    }
}
function shouldIncludeRouterCandidate(candidate, config) {
    if (isCandidateDisabledByEntries(candidate, config?.skills?.entries))
        return false;
    if (candidate.source === "openclaw-bundled") {
        const allowBundled = normalizeStringList(config?.skills?.allowBundled);
        if (allowBundled.length > 0 && !normalizedSkillListIncludes(allowBundled, candidate)) {
            return false;
        }
    }
    return true;
}
async function loadSkillCandidatesForWorkspace(workspaceDir, discovery) {
    const now = Date.now();
    const config = await readOpenClawConfigSnapshot();
    const bundleRoot = resolveBundledSkillsRoot({ moduleDir: __dirname });
    const cacheKey = routerDiscoveryCacheKey(workspaceDir, config, bundleRoot, discovery);
    const cached = workspaceSkillCache.get(cacheKey);
    if (cached && now - cached.fetchedAt < ROUTER_CACHE_TTL_MS) {
        return cached;
    }
    const roots = await buildSkillRootSpecs({
        workspaceDir,
        bundledRoot: bundleRoot,
        includeBundled: Boolean(bundleRoot),
        discovery,
    });
    const groupDepth = typeof discovery.groupDepth === "number" ? discovery.groupDepth : DEFAULT_ROUTER_DISCOVERY_GROUP_DEPTH;
    const seen = new Map();
    for (let r = 0; r < roots.length; r += 1) {
        const { root, source } = roots[r];
        const candidates = await collectSkillCandidatesFromRoot(root, source, groupDepth);
        for (const candidate of candidates) {
            if (!shouldIncludeRouterCandidate(candidate, config))
                continue;
            const key = canonicalSkillId(candidate);
            if (!key)
                continue;
            const existing = seen.get(key);
            if (!existing || existing.priority > r) {
                seen.set(key, { priority: r, candidate });
            }
        }
    }
    const candidates = [...seen.values()].map((entry) => entry.candidate);
    const cacheEntry = { fetchedAt: now, candidates };
    workspaceSkillCache.set(cacheKey, cacheEntry);
    return cacheEntry;
}
function tokenize(text) {
    return text
        .toLowerCase()
        .replace(/[.,;:!?\'"()\[\]{}]/g, " ")
        .replace(/\s+/g, " ")
        .trim()
        .split(/\s+/)
        .filter((t) => t.length > 2);
}
function buildIdfTable(candidates) {
    const N = candidates.length;
    const docFreq = new Map();
    let totalDescLen = 0;
    for (const candidate of candidates) {
        const descTokens = tokenize(candidate.description);
        totalDescLen += descTokens.length;
        const tokens = new Set(descTokens);
        for (const token of tokens) {
            docFreq.set(token, (docFreq.get(token) || 0) + 1);
        }
    }
    const idf = new Map();
    for (const [term, df] of docFreq) {
        // Standard BM25 IDF: log((N - df + 0.5) / (df + 0.5) + 1)
        idf.set(term, Math.log((N - df + 0.5) / (df + 0.5) + 1));
    }
    return { idf, avgDescLen: N > 0 ? totalDescLen / N : 1 };
}
function scoreSkill(taskText, skill, keywords, idf, avgDescLen) {
    const taskTokens = tokenize(taskText);
    const descTokens = tokenize(skill.description);
    let score = 0;
    let reason = "";
    // Name match bonus (still valuable - distinctive signal)
    const escapedName = skill.name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const namePattern = new RegExp(`(^|[^a-z0-9])${escapedName}([^a-z0-9]|$)`, "i");
    if (namePattern.test(taskText)) {
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
    const descTf = new Map();
    for (const token of descTokens) {
        descTf.set(token, (descTf.get(token) || 0) + 1);
    }
    // Score each query (task) term against the description document
    let bm25 = 0;
    for (const term of new Set(taskTokens)) {
        const tf = descTf.get(term) || 0;
        if (tf === 0)
            continue;
        const termIdf = idf.get(term) || 0;
        const numerator = tf * (k1 + 1);
        const denominator = tf + k1 * (1 - b + b * (dl / avgdl));
        bm25 += termIdf * (numerator / denominator);
    }
    score += bm25;
    if (bm25 > 0 && !reason)
        reason = "bm25";
    // Config keyword hits (+5 per keyword - manual boost for known associations)
    const skillKeywordAliases = new Set([
        canonicalSkillId(skill),
        skill.name.toLowerCase(),
        String(skill.key || "").trim().toLowerCase(),
    ].filter(Boolean));
    const skillKeywords = [...skillKeywordAliases].flatMap((alias) => keywords[alias] || []);
    for (const kw of skillKeywords) {
        if (taskText.toLowerCase().includes(kw.toLowerCase())) {
            score += 5;
            if (!reason)
                reason = "keyword_match";
        }
    }
    return { score, reason: reason || "none" };
}
function formatNudge(skills) {
    const noun = skills.length === 1 ? "this skill" : "these skills";
    const lines = [
        `[skill-router] Based on your current task, you likely need ${noun}:`,
    ];
    for (const skill of skills) {
        const desc = (skill.description || "").replace(/\s+/g, " ").trim();
        lines.push(`  → ${skill.name}: "${desc || "No description available."}"`);
        lines.push(`    [skill-router:id ${canonicalSkillId(skill)}]`);
        lines.push(`    Location: ${normalizePathForDisplay(skill.filePath)}`);
        lines.push("    Read it with the read tool before proceeding.");
    }
    return lines.join("\n");
}
function createPreparedStatements(db, log) {
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
      run_id,
      agent_id,
      skill_name,
      skill_key,
      skill_path,
      score,
      match_reason,
      turn_number,
      task_excerpt
    ) VALUES (
      @session_key,
      @session_id,
      @run_id,
      @agent_id,
      @skill_name,
      @skill_key,
      @skill_path,
      @score,
      @match_reason,
      @turn_number,
      @task_excerpt
    )
  `);
    const insertRouterDecision = db.prepare(`
    INSERT INTO skill_router_decisions (
      ts,
      session_key,
      session_id,
      run_id,
      agent_id,
      target_type,
      decision,
      reason,
      task_window_mode,
      min_score,
      max_skills,
      recency_window,
      lookback_messages,
      candidate_count,
      available_count,
      scored_count,
      selected_count,
      selected_skill_keys,
      top_candidates,
      task_length,
      task_excerpt
    ) VALUES (
      @ts,
      @session_key,
      @session_id,
      @run_id,
      @agent_id,
      @target_type,
      @decision,
      @reason,
      @task_window_mode,
      @min_score,
      @max_skills,
      @recency_window,
      @lookback_messages,
      @candidate_count,
      @available_count,
      @scored_count,
      @selected_count,
      @selected_skill_keys,
      @top_candidates,
      @task_length,
      @task_excerpt
    )
  `);
    const cleanupRouterDecisions = db.prepare(`
    DELETE FROM skill_router_decisions
    WHERE timestamp < datetime('now', @retention_window)
  `);
    const getRecentNudge = db.prepare(`
    SELECT id
    FROM skill_nudges n
    WHERE (
        (@run_id IS NOT NULL AND n.run_id = @run_id)
        OR (@run_id IS NULL AND @session_key IS NOT NULL AND n.session_key = @session_key)
        OR (@run_id IS NULL AND @session_key IS NULL AND @session_id IS NOT NULL AND n.session_id = @session_id)
      )
      AND (
        LOWER(COALESCE(n.skill_key, n.skill_name)) = @skill_key_lower
        OR LOWER(n.skill_name) = @skill_name_lower
        OR (@skill_path IS NOT NULL AND n.skill_path = @skill_path)
      )
      AND (
        (
          @turn_number IS NOT NULL
          AND n.turn_number IS NOT NULL
          AND @turn_number >= n.turn_number
          AND (@turn_number - n.turn_number) < @recency_window
        )
        OR (
          (@turn_number IS NULL OR n.turn_number IS NULL)
          AND datetime(n.timestamp) >= datetime('now', @fallback_window)
        )
      )
    ORDER BY n.id DESC
    LIMIT 1
  `);
    return {
        insertEvent: (params) => {
            try {
                insertEvent.run(params);
            }
            catch (err) {
                log.debug?.(`skill-usage-audit: insertEvent failed: ${String(err)}`);
            }
        },
        insertVersion: (params) => {
            try {
                const existing = getExactSkillVersion.get(params);
                if (!existing?.version_hash) {
                    insertVersion.run(params);
                }
            }
            catch (err) {
                log.debug?.(`skill-usage-audit: insertVersion failed: ${String(err)}`);
            }
        },
        upsertSkill: (params) => {
            try {
                upsertSkill.run(params);
            }
            catch (err) {
                log.debug?.(`skill-usage-audit: upsertSkill failed: ${String(err)}`);
            }
        },
        getLatestSkillVersion: (params) => getLatestSkillVersion.get(params),
        insertExecution: (params) => insertExecution.run(params),
        insertFeedback: (params) => insertFeedback.run(params),
        insertNudge: (params) => {
            try {
                insertNudge.run(params);
            }
            catch (err) {
                log.debug?.(`skill-usage-audit: insertNudge failed: ${String(err)}`);
            }
        },
        insertRouterDecision: (params) => {
            try {
                insertRouterDecision.run(params);
            }
            catch (err) {
                log.debug?.(`skill-usage-audit: insertRouterDecision failed: ${String(err)}`);
            }
        },
        cleanupRouterDecisions: (params) => {
            try {
                cleanupRouterDecisions.run(params);
            }
            catch (err) {
                log.debug?.(`skill-usage-audit: cleanupRouterDecisions failed: ${String(err)}`);
            }
        },
        getRecentNudge: (params) => {
            try {
                return getRecentNudge.get(params);
            }
            catch (err) {
                log.debug?.(`skill-usage-audit: getRecentNudge failed: ${String(err)}`);
                return undefined;
            }
        },
    };
}
async function initSqlite(path, log) {
    await mkdir(dirname(path), { recursive: true });
    let backend = null;
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
                        get: (params) => stmt.get(params),
                    };
                },
            };
        }
    }
    catch {
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
                            get: (params) => stmt.get(params),
                        };
                    },
                };
            }
        }
        catch {
            // none
        }
    }
    if (!backend) {
        return {
            backend: null,
            statements: null,
            error: "No sqlite backend available; plugin will continue without sqlite writes",
        };
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
      run_id TEXT,
      agent_id TEXT,
      skill_name TEXT NOT NULL,
      skill_key TEXT,
      skill_path TEXT,
      score REAL,
      match_reason TEXT,
      turn_number INTEGER,
      task_excerpt TEXT,
      timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS skill_router_decisions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts TEXT NOT NULL,
      session_key TEXT,
      session_id TEXT,
      run_id TEXT,
      agent_id TEXT,
      target_type TEXT,
      decision TEXT NOT NULL,
      reason TEXT NOT NULL,
      task_window_mode TEXT,
      min_score REAL,
      max_skills INTEGER,
      recency_window INTEGER,
      lookback_messages INTEGER,
      candidate_count INTEGER DEFAULT 0,
      available_count INTEGER DEFAULT 0,
      scored_count INTEGER DEFAULT 0,
      selected_count INTEGER DEFAULT 0,
      selected_skill_keys TEXT,
      top_candidates TEXT,
      task_length INTEGER,
      task_excerpt TEXT,
      timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_nudges_session ON skill_nudges(session_key);
    CREATE INDEX IF NOT EXISTS idx_nudges_skill ON skill_nudges(skill_name);
    CREATE INDEX IF NOT EXISTS idx_nudges_time ON skill_nudges(timestamp);
    CREATE INDEX IF NOT EXISTS idx_router_decisions_time ON skill_router_decisions(timestamp);
    CREATE INDEX IF NOT EXISTS idx_router_decisions_reason ON skill_router_decisions(reason);
    CREATE INDEX IF NOT EXISTS idx_router_decisions_run ON skill_router_decisions(run_id);
    CREATE INDEX IF NOT EXISTS idx_router_decisions_session ON skill_router_decisions(session_key);
  `);
    try {
        backend.exec(`ALTER TABLE skill_nudges ADD COLUMN run_id TEXT;`);
    }
    catch {
        // Existing databases may already have the column.
    }
    try {
        backend.exec(`ALTER TABLE skill_nudges ADD COLUMN skill_key TEXT;`);
    }
    catch {
        // Existing databases may already have the column.
    }
    backend.exec(`
    CREATE INDEX IF NOT EXISTS idx_nudges_run ON skill_nudges(run_id);
    CREATE INDEX IF NOT EXISTS idx_nudges_skill_key ON skill_nudges(skill_key);
  `);
    const statements = createPreparedStatements(backend, log);
    log.info(`skill-usage-audit: sqlite initialized at ${path} (${backend.kind})`);
    return {
        backend,
        statements,
    };
}
async function computeSkillVersionHash(skillPath) {
    const canonicalPath = canonicalSkillPath(skillPath);
    if (!canonicalPath)
        return null;
    const dir = dirname(canonicalPath);
    const scriptDir = join(dir, "scripts");
    const files = [canonicalPath];
    try {
        const statDir = await stat(scriptDir);
        if (statDir.isDirectory()) {
            const scriptEntries = await readdir(scriptDir, { withFileTypes: true });
            for (const entry of scriptEntries.sort((a, b) => a.name.localeCompare(b.name))) {
                if (entry.isFile())
                    files.push(join(scriptDir, entry.name));
            }
        }
    }
    catch {
        // no scripts
    }
    if (!files.length)
        return null;
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
        }
        catch {
            // skip unreadable
        }
    }
    if (!hasBytes)
        return null;
    return hash.digest("hex");
}
export default definePluginEntry({
    id: "skill-usage-audit",
    name: "Skill Usage Audit",
    description: "Skill usage telemetry, execution tracking, and intelligent skill routing for OpenClaw. Writes audit data to SQLite.",
    register(api) {
        const log = api.logger;
        const registrationMode = api.registrationMode || "full";
        const sharedState = skillUsageAuditGlobalAccessor.__skillUsageAuditState;
        const apiRegistrationToken = api;
        if (registrationMode !== "full") {
            if (!sharedState.skipModeLog.has(registrationMode)) {
                log.debug?.(`skill-usage-audit: skipping init (registrationMode=${registrationMode})`);
                sharedState.skipModeLog.add(registrationMode);
            }
            return;
        }
        if (sharedState.registeredFullApis.has(apiRegistrationToken)) {
            return;
        }
        const cfg = api.pluginConfig || {};
        const includeToolParams = cfg.includeToolParams ?? DEFAULT_INCLUDE_TOOL_PARAMS;
        const captureMessageContent = cfg.captureMessageContent === undefined ? DEFAULT_CAPTURE_MESSAGE_CONTENT : cfg.captureMessageContent === true;
        const piiEnabled = cfg.scrubPII !== false; // default true
        const redactKeys = new Set((Array.isArray(cfg.redactKeys) ? cfg.redactKeys : DEFAULT_REDACT_KEYS).map((k) => String(k).toLowerCase()));
        const contextWindowSize = parseIntConfig(cfg.contextWindowSize, DEFAULT_CONTEXT_WINDOW_SIZE, 1);
        const contextTimeoutMs = parseIntConfig(cfg.contextTimeoutMs, DEFAULT_CONTEXT_TIMEOUT_MS, 0);
        const detectSkillBlocks = cfg.skillBlockDetection !== false;
        const dbPath = resolveDbPath(cfg.dbPath);
        if (!sharedState.dbPath) {
            sharedState.dbPath = dbPath;
        }
        else if (sharedState.dbPath !== dbPath && !sharedState.warnedDbPaths.has(dbPath)) {
            log.warn?.(`skill-usage-audit: ignoring dbPath=${dbPath} for additional full registration; using shared dbPath=${sharedState.dbPath}`);
            sharedState.warnedDbPaths.add(dbPath);
        }
        const dbState = sharedState.db;
        let hasLoggedDbIssue = false;
        const DB_QUEUE_DROP_THRESHOLD = 500;
        const routerConfig = cfg.router || {};
        const routerEnabled = parseBooleanConfig(routerConfig.enabled, true);
        const routerTargets = routerConfig.targets || {};
        const routerTargetAgent = parseBooleanConfig(routerTargets.agent, true);
        const routerTargetSubagent = parseBooleanConfig(routerTargets.subagent, true);
        const routerTargetCron = parseBooleanConfig(routerTargets.cron, true);
        const routerTargetConfig = {
            agent: resolveRouterTargetConfig(routerConfig, "agent", {
                defaultMaxSkills: DEFAULT_ROUTER_MAX_SKILLS,
                defaultMinScore: DEFAULT_ROUTER_MIN_SCORE,
                defaultAgentMinScore: DEFAULT_ROUTER_AGENT_MIN_SCORE,
            }),
            subagent: resolveRouterTargetConfig(routerConfig, "subagent", {
                defaultMaxSkills: DEFAULT_ROUTER_MAX_SKILLS,
                defaultMinScore: DEFAULT_ROUTER_MIN_SCORE,
                defaultAgentMinScore: DEFAULT_ROUTER_AGENT_MIN_SCORE,
            }),
            cron: resolveRouterTargetConfig(routerConfig, "cron", {
                defaultMaxSkills: DEFAULT_ROUTER_MAX_SKILLS,
                defaultMinScore: DEFAULT_ROUTER_MIN_SCORE,
                defaultAgentMinScore: DEFAULT_ROUTER_AGENT_MIN_SCORE,
            }),
        };
        const routerRecencyWindow = parseIntConfig(routerConfig.recencyWindow, DEFAULT_ROUTER_RECENCY_WINDOW, 1);
        const routerRecencyFallbackMinutes = parseIntConfig(routerConfig.recencyFallbackMinutes, DEFAULT_ROUTER_RECENCY_FALLBACK_MINUTES, 0);
        const routerLookbackMessages = parseIntConfig(routerConfig.lookbackMessages, DEFAULT_ROUTER_LOOKBACK_MESSAGES, 1);
        const routerTaskWindowMode = parseStringConfig(routerConfig.taskWindowMode, "recentMessages", new Set(["recentMessages", "latestUser", "promptOnly"]));
        const routerDiscovery = buildRouterDiscoveryConfig(routerConfig.discovery);
        const routerObservability = routerConfig.observability || {};
        const routerObservabilityEnabled = parseBooleanConfig(routerObservability.enabled, true);
        const routerObservabilityTopCandidates = clampIntConfig(routerObservability.topCandidates, DEFAULT_ROUTER_OBSERVABILITY_TOP_CANDIDATES, 0, MAX_ROUTER_OBSERVABILITY_TOP_CANDIDATES);
        const routerObservabilityRetentionDays = parseIntConfig(routerObservability.retentionDays, DEFAULT_ROUTER_OBSERVABILITY_RETENTION_DAYS, 0);
        const routerObservabilityIncludeTaskExcerpt = parseBooleanConfig(routerObservability.includeTaskExcerpt, false);
        const routerOverrides = parseOverrideRules(routerConfig.overrides).map((entry) => {
            try {
                return { ...entry, matcher: new RegExp(entry.taskPattern, "i") };
            }
            catch (error) {
                log.error(`skill-usage-audit: invalid override taskPattern: ${String(entry.taskPattern)} (${String(error)})`);
                return undefined;
            }
        }).filter((entry) => Boolean(entry));
        const routerSkillKeywords = parseSkillKeywords(routerConfig.skillKeywords);
        const routerBlocklist = new Set(parseBlocklist(routerConfig.blocklist));
        const pluginWorkspaceDir = resolveWorkspaceDir(api.config);
        const messageHistory = new Map();
        const recentRouterNudges = new Map();
        let messageHistoryCounter = 0;
        const executionsById = new Map();
        const execByScope = new Map();
        const execByTool = new Map();
        let executionSeq = 0;
        const EXECUTION_STALE_MS = 10 * 60 * 1000; // 10 minutes
        const EXECUTION_CLEANUP_EVERY = 50;
        let executionCleanupCounter = 0;
        async function ensureDbReady() {
            if (dbState.backend || dbState.statements || dbState.error) {
                return dbState;
            }
            if (!sharedState.dbInitPromise) {
                const initPath = sharedState.dbPath || dbPath;
                sharedState.dbInitPromise = initSqlite(initPath, log).then((state) => {
                    if (!dbState.backend && !dbState.statements && dbState.error === undefined) {
                        dbState.backend = state.backend;
                        dbState.statements = state.statements;
                        dbState.error = state.error;
                    }
                    if (state.statements && routerObservabilityRetentionDays > 0) {
                        state.statements.cleanupRouterDecisions({
                            retention_window: `-${routerObservabilityRetentionDays} days`,
                        });
                    }
                    return state;
                });
            }
            const state = await sharedState.dbInitPromise;
            if (state.error && !hasLoggedDbIssue) {
                hasLoggedDbIssue = true;
                log.info(`skill-usage-audit: sqlite unavailable: ${state.error}`);
            }
            return dbState;
        }
        function scheduleDbWrite(label, critical, write) {
            if (!sharedState.acceptingWrites)
                return;
            if (!critical && sharedState.dbQueueDepth >= DB_QUEUE_DROP_THRESHOLD) {
                if (!sharedState.hasLoggedDbQueueDrop) {
                    sharedState.hasLoggedDbQueueDrop = true;
                    log.info(`skill-usage-audit: db queue backlog high (${sharedState.dbQueueDepth}); dropping non-critical inserts`);
                }
                return;
            }
            sharedState.dbQueueDepth += 1;
            sharedState.dbChain = sharedState.dbChain
                .then(async () => write())
                .catch((err) => {
                log.error(`skill-usage-audit: ${label}: ${String(err)}`);
            })
                .finally(() => {
                sharedState.dbQueueDepth = Math.max(0, sharedState.dbQueueDepth - 1);
                if (sharedState.dbQueueDepth < DB_QUEUE_DROP_THRESHOLD) {
                    sharedState.hasLoggedDbQueueDrop = false;
                }
            });
        }
        function queueDbInsert(rowType) {
            if (rowType.type !== "session_start" && rowType.type !== "session_end" && rowType.type !== "tool_call_start" && rowType.type !== "tool_call_end" && rowType.type !== "skill_file_read" && rowType.type !== "skill_block_detected") {
                return;
            }
            scheduleDbWrite("event insert failed", false, async () => {
                const state = await ensureDbReady();
                if (!state.statements)
                    return;
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
                    success: typeof rowType.success === "boolean"
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
        function queueSkillVersionWrite(skillName, skillPath, ts, versionHash) {
            scheduleDbWrite("failed writing skill version", true, async () => {
                const state = await ensureDbReady();
                if (!state.statements)
                    return;
                const canonicalPath = normalizeSkillExecutionPath(skillPath);
                if (!canonicalPath)
                    return;
                const hash = versionHash ?? (await computeSkillVersionHash(canonicalPath));
                if (!hash)
                    return;
                state.statements.insertVersion({
                    skill_name: skillName,
                    skill_path: canonicalPath,
                    version_hash: hash,
                    first_seen_at: ts,
                    notes: null,
                });
                state.statements.upsertSkill({
                    skill_name: skillName,
                    skill_path: canonicalPath,
                    current_version_hash: hash,
                    status: "stable",
                    last_modified_at: ts,
                    last_used_at: ts,
                });
            });
        }
        function queueNudgeInsert(params) {
            scheduleDbWrite("failed writing nudge", true, async () => {
                const state = await ensureDbReady();
                if (!state.statements)
                    return;
                const base = buildBase(params.event, params.ctx);
                state.statements.insertNudge({
                    session_key: base.sessionKey || null,
                    session_id: base.sessionId || null,
                    run_id: base.runId || null,
                    agent_id: base.agentId || null,
                    skill_name: params.skillName,
                    skill_key: params.skillKey,
                    skill_path: params.skillPath || null,
                    score: params.score,
                    match_reason: params.matchReason,
                    turn_number: params.turnNumber,
                    task_excerpt: params.taskExcerpt,
                });
            });
        }
        function queueRouterDecisionInsert(params) {
            if (!routerObservabilityEnabled)
                return;
            const boundedTop = (params.topCandidates || []).slice(0, routerObservabilityTopCandidates).map((row) => ({
                skill_key: row.skill.key,
                skill_name: row.skill.name,
                source: row.skill.source,
                score: Number(row.score.toFixed(3)),
                match_reason: row.reason,
                status: row.status || undefined,
            }));
            const selectedKeys = (params.selectedSkills || []).map((skill) => canonicalSkillId(skill)).filter(Boolean);
            const taskLength = params.taskText ? params.taskText.length : 0;
            const taskExcerpt = routerObservabilityIncludeTaskExcerpt && params.taskText
                ? sanitizeValue(scrubSecrets(params.taskText, piiEnabled), 200, piiEnabled)
                : null;
            scheduleDbWrite("failed writing router decision", false, async () => {
                const state = await ensureDbReady();
                if (!state.statements)
                    return;
                const base = buildBase(params.event, params.ctx);
                state.statements.insertRouterDecision({
                    ts: new Date().toISOString(),
                    session_key: base.sessionKey || null,
                    session_id: base.sessionId || null,
                    run_id: base.runId || null,
                    agent_id: base.agentId || null,
                    target_type: params.targetType,
                    decision: params.decision,
                    reason: params.reason,
                    task_window_mode: routerTaskWindowMode,
                    min_score: params.targetConfig?.minScore ?? null,
                    max_skills: params.targetConfig?.maxSkillsToNudge ?? null,
                    recency_window: routerRecencyWindow,
                    lookback_messages: routerLookbackMessages,
                    candidate_count: params.counts?.candidateCount ?? 0,
                    available_count: params.counts?.availableCount ?? 0,
                    scored_count: params.counts?.scoredCount ?? 0,
                    selected_count: params.counts?.selectedCount ?? selectedKeys.length,
                    selected_skill_keys: selectedKeys.length ? JSON.stringify(selectedKeys) : null,
                    top_candidates: boundedTop.length ? JSON.stringify(boundedTop) : null,
                    task_length: taskLength,
                    task_excerpt: taskExcerpt,
                });
            });
        }
        function cleanupMessageHistory() {
            const now = Date.now();
            for (const [scope, entry] of messageHistory.entries()) {
                if (now - entry.lastSeenAt > MESSAGE_HISTORY_STALE_MS) {
                    messageHistory.delete(scope);
                }
            }
            for (const [key, entry] of recentRouterNudges.entries()) {
                if (now - entry.seenAt > MESSAGE_HISTORY_STALE_MS) {
                    recentRouterNudges.delete(key);
                }
            }
            if (messageHistory.size <= MESSAGE_HISTORY_MAX_SCOPES)
                return;
            const sortedScopes = [...messageHistory.entries()].sort((a, b) => a[1].lastSeenAt - b[1].lastSeenAt);
            while (messageHistory.size > MESSAGE_HISTORY_MAX_SCOPES) {
                const oldest = sortedScopes.shift();
                if (!oldest)
                    break;
                messageHistory.delete(oldest[0]);
            }
        }
        function cleanupStaleExecutions() {
            const now = Date.now();
            for (const [id, execution] of executionsById.entries()) {
                if (execution.finalized)
                    continue;
                if (now - execution.startAt > EXECUTION_STALE_MS && execution.inFlightToolCalls.size === 0) {
                    finalizeExecution(id, "stale-cleanup");
                }
            }
        }
        function enqueueEvent(event) {
            queueDbInsert(event);
            messageHistoryCounter += 1;
            if (messageHistoryCounter >= MESSAGE_HISTORY_CLEANUP_EVERY) {
                messageHistoryCounter = 0;
                cleanupMessageHistory();
            }
            executionCleanupCounter += 1;
            if (executionCleanupCounter >= EXECUTION_CLEANUP_EVERY) {
                executionCleanupCounter = 0;
                cleanupStaleExecutions();
            }
        }
        function addMessage(scope, message) {
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
        function getRecentMessages(scopeKeys, windowSize) {
            const all = [];
            for (const key of scopeKeys) {
                const entry = messageHistory.get(key);
                if (entry)
                    all.push(...entry.messages);
            }
            return all.sort((a, b) => (a.ts < b.ts ? -1 : a.ts > b.ts ? 1 : 0)).slice(-windowSize);
        }
        function trackExecution(execution) {
            for (const key of execution.scopeKeys) {
                const list = execByScope.get(key) || [];
                if (!list.includes(execution))
                    list.push(execution);
                execByScope.set(key, list);
            }
        }
        function untrackExecution(execution) {
            for (const key of execution.scopeKeys) {
                const list = execByScope.get(key);
                if (!list)
                    continue;
                const next = list.filter((entry) => entry.id !== execution.id);
                if (next.length)
                    execByScope.set(key, next);
                else
                    execByScope.delete(key);
            }
            for (const [toolId, id] of execByTool.entries()) {
                if (id === execution.id) {
                    execByTool.delete(toolId);
                }
            }
            executionsById.delete(execution.id);
        }
        function candidatesForScope(keys) {
            const set = new Map();
            for (const key of keys) {
                const list = execByScope.get(key) || [];
                for (const execution of list) {
                    if (!execution.finalized)
                        set.set(execution.id, execution);
                }
            }
            return [...set.values()].sort((a, b) => b.startAt - a.startAt);
        }
        function pickExecution(keys, requireFollowup = false) {
            const list = candidatesForScope(keys);
            if (!list.length)
                return undefined;
            if (requireFollowup) {
                return list.find((execution) => execution.inFollowup) || list[0];
            }
            return list[0];
        }
        function markFollowup(execution) {
            if (execution.inFollowup)
                return;
            execution.inFollowup = true;
            execution.followupStartedAt = Date.now();
            if (contextTimeoutMs <= 0) {
                return;
            }
            execution.followupTimer = setTimeout(() => {
                finalizeExecution(execution.id, "followup-timeout");
            }, contextTimeoutMs);
        }
        function stopFollowup(execution) {
            if (!execution.inFollowup)
                return;
            execution.inFollowup = false;
            execution.followupStartedAt = undefined;
            if (execution.followupTimer)
                clearTimeout(execution.followupTimer);
            execution.followupTimer = undefined;
            execution.followupMessages = [];
        }
        function isNegativeMessage(execution, text) {
            return isNegativeSignal(detectMessageSignals(execution, text || ""));
        }
        function determineOutcome(execution) {
            if (execution.fallbackSkillRetried || execution.sameSkillRetried)
                return "negative";
            if (execution.followupMessages.some((m) => isNegativeMessage(execution, m.text)))
                return "negative";
            if (execution.followupMessages.some((m) => isNegativeSignal(m.signalLabels || [])))
                return "negative";
            if (execution.hadToolCall && execution.mechanicalSuccess)
                return "positive";
            return "unclear";
        }
        function finalizeExecution(executionId, reason) {
            const execution = executionsById.get(executionId);
            if (!execution || execution.finalized)
                return;
            execution.finalized = true;
            if (execution.followupTimer)
                clearTimeout(execution.followupTimer);
            execution.followupTimer = undefined;
            untrackExecution(execution);
            const durationMs = Math.max(0, Date.now() - execution.startAt);
            const outcome = determineOutcome(execution);
            scheduleDbWrite(`failed inserting execution ${executionId} (${reason})`, true, async () => {
                const state = await ensureDbReady();
                if (!state.statements)
                    return;
                let versionHash = execution.versionHash ?? null;
                if (!versionHash && execution.versionHashPromise) {
                    try {
                        versionHash = await execution.versionHashPromise;
                    }
                    catch {
                        versionHash = null;
                    }
                }
                if (!versionHash) {
                    const row = state.statements.getLatestSkillVersion({
                        skill_name: execution.skillName,
                        skill_path: execution.skillPath,
                    });
                    if (row?.version_hash)
                        versionHash = String(row.version_hash);
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
        function normalizeSkillExecutionPath(rawPath) {
            return canonicalSkillPath(rawPath);
        }
        function resolveSkillPathFromBlock(name, location) {
            if (location) {
                const normalized = normalizeSkillExecutionPath(location);
                if (!normalized)
                    return undefined;
                const lower = normalized.toLowerCase();
                if (lower.endsWith("skill.md"))
                    return normalized;
                return `${normalized}/SKILL.md`;
            }
            if (!name)
                return undefined;
            const trimmed = name.trim();
            if (!trimmed)
                return undefined;
            if (trimmed.startsWith("~") || trimmed.includes("/") || trimmed.endsWith(".md")) {
                const normalized = normalizeSkillExecutionPath(trimmed);
                if (normalized && normalized.toLowerCase().endsWith(".md"))
                    return normalized;
            }
            return undefined;
        }
        function findMatchingExecution(scopeKeys, skillName, skillPath, runId) {
            const candidates = candidatesForScope(scopeKeys).filter((execution) => {
                if (execution.skillName.toLowerCase() === skillName.toLowerCase())
                    return true;
                return !!(skillPath && execution.encounteredSkillPaths.has(skillPath));
            });
            if (!candidates.length)
                return undefined;
            if (runId) {
                const sameRun = candidates.filter((e) => e.runId === runId);
                if (sameRun.length) {
                    const exact = skillPath ? sameRun.filter((e) => e.encounteredSkillPaths.has(skillPath)) : [];
                    if (exact.length)
                        return exact[0];
                    return sameRun[0];
                }
            }
            const exactPath = skillPath ? candidates.filter((e) => e.encounteredSkillPaths.has(skillPath)) : [];
            if (exactPath.length)
                return exactPath[0];
            return candidates[0];
        }
        function syncExecutionPath(execution, skillName, skillPath) {
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
        function attachToolCall(keys, event, execution) {
            const target = execution || pickExecution(keys);
            if (!target || target.finalized)
                return;
            if (target.inFollowup)
                stopFollowup(target);
            const toolName = toStringLike(event.toolName) || "";
            const params = typeof event.params === "object" && event.params !== null
                ? event.params
                : undefined;
            const isSkillRead = toolName === "read" && extractSkillPathFromParams(params || {}) !== undefined;
            if (!isSkillRead) {
                target.hadToolCall = true;
            }
            target.attachedToolCallCount += 1;
            const callId = toStringLike(event.toolCallId)
                ? `tool:${toStringLike(event.toolCallId)}`
                : `anon:${target.id}:${target.attachedToolCallCount}`;
            target.inFlightToolCalls.add(callId);
            execByTool.set(callId, target.id);
            if (toolName === "read" && params) {
                const rawPath = extractSkillPathFromParams(params);
                if (rawPath) {
                    const inferredSkillName = inferSkillName(rawPath);
                    syncExecutionPath(target, inferredSkillName, rawPath);
                    const normalized = normalizeSkillExecutionPath(rawPath);
                    if (normalized) {
                        target.skillFileReadCount = (target.skillFileReadCount || 0) + 1;
                        if (normalized === target.skillPath) {
                            if (target.skillFileReadCount > 1)
                                target.sameSkillRetried = true;
                        }
                        else {
                            target.fallbackSkillRetried = true;
                        }
                        target.encounteredSkillPaths.add(normalized);
                    }
                }
            }
            return target;
        }
        function detachToolCall(event, scopeKeys) {
            const toolId = toStringLike(event.toolCallId);
            const direct = toolId ? execByTool.get(`tool:${toolId}`) : undefined;
            const target = direct ? executionsById.get(direct) : pickExecution(scopeKeys);
            if (!target || target.finalized)
                return;
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
        function onFollowupMessage(execution, message) {
            if (!execution)
                return;
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
        function routerNudgeScope(ctx, event) {
            return buildScopeKeys(ctx, event).join("|");
        }
        function routerNudgeKey(scope, skill) {
            return `${scope}:${skill.key.toLowerCase()}:${skill.name.toLowerCase()}`;
        }
        function wasNudgedRecentlyInMemory(scope, skill, turnNumber, recencyWindow, fallbackMinutes) {
            const entry = recentRouterNudges.get(routerNudgeKey(scope, skill));
            return shouldSuppressRecentNudgeRecord(entry, turnNumber, recencyWindow, Date.now(), fallbackMinutes * 60_000);
        }
        async function wasNudgedRecentlyInDb(event, ctx, skill, turnNumber, recencyWindow, fallbackMinutes) {
            const state = await ensureDbReady();
            if (!state.statements)
                return false;
            const base = buildBase(event, ctx);
            if (!base.runId && !base.sessionKey && !base.sessionId)
                return false;
            const row = state.statements.getRecentNudge({
                run_id: base.runId || null,
                session_key: base.sessionKey || null,
                session_id: base.sessionId || null,
                skill_key_lower: skill.key.toLowerCase(),
                skill_name_lower: skill.name.toLowerCase(),
                skill_path: skill.filePath || null,
                turn_number: turnNumber,
                recency_window: recencyWindow,
                fallback_window: `-${Math.max(0, Math.floor(fallbackMinutes))} minutes`,
            });
            return Boolean(row?.id);
        }
        function rememberRouterNudge(scope, skill, turnNumber) {
            recentRouterNudges.set(routerNudgeKey(scope, skill), {
                turnNumber,
                seenAt: Date.now(),
            });
        }
        function startExecutionFromSkillRead(ctx, event, skillPath, now, isFromSkillBlock = false) {
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
            const execution = {
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
                attachedToolCallCount: 0,
                skillFileReadCount: 0,
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
                    ...buildBase(event, ctx),
                    skillName,
                    skillPath: initialPath,
                    skillSource: inferSkillSource(initialPath),
                    toolName: toStringLike(event.toolName),
                    toolCallId: toStringLike(event.toolCallId),
                });
            }
            return execution;
        }
        function parseSkillBlock(prompt) {
            const rgx = /<\s*skill\b([^>]*)>/gi;
            const blocks = [];
            const names = [];
            const locations = [];
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
                if (location)
                    locations.push(location);
                match = rgx.exec(prompt);
            }
            return { blocks, names, locations, count: names.length };
        }
        async function maybeNudgeSkills(event, ctx) {
            if (!routerEnabled)
                return;
            const sessionKey = toStringLike(ctx?.sessionKey) || "";
            const isSubagent = sessionKey.includes(":subagent:");
            const isCron = toStringLike(ctx?.trigger) === "cron";
            const isRegularAgent = !isSubagent && !isCron;
            const routerTargetType = isCron ? "cron" : isSubagent ? "subagent" : "agent";
            const targetConfig = routerTargetConfig[routerTargetType];
            const targetMinScore = targetConfig.minScore;
            const targetMaxSkillsToNudge = targetConfig.maxSkillsToNudge;
            const shouldRoute = (isRegularAgent && routerTargetAgent) ||
                (isSubagent && routerTargetSubagent) ||
                (isCron && routerTargetCron);
            if (!shouldRoute) {
                queueRouterDecisionInsert({
                    event,
                    ctx,
                    targetType: routerTargetType,
                    decision: "skipped",
                    reason: "target_disabled",
                    targetConfig,
                });
                return;
            }
            const prompt = toStringLike(event.prompt) || "";
            const eventMessages = event.messages;
            const messageList = Array.isArray(eventMessages) ? eventMessages : [];
            const turnNumber = messageList.length ? messageIndexForTask(messageList, prompt) : null;
            const recentMessages = isRegularAgent ? messageList.slice(-routerLookbackMessages) : messageList;
            if (isRegularAgent && recentMessages.length > 0) {
                const latestMessage = [...recentMessages].reverse().find((message) => getTextFromMessage(message));
                const latestRole = getRoleFromMessage(latestMessage);
                if (latestRole && latestRole !== "user") {
                    queueRouterDecisionInsert({
                        event,
                        ctx,
                        targetType: routerTargetType,
                        decision: "skipped",
                        reason: "latest_message_not_user",
                        targetConfig,
                        counts: { candidateCount: 0, availableCount: 0, scoredCount: 0, selectedCount: 0 },
                    });
                    return;
                }
            }
            const taskText = selectRouterTaskText({
                prompt,
                messages: isRegularAgent ? messageList : recentMessages,
                mode: routerTaskWindowMode,
                lookbackMessages: isRegularAgent ? routerLookbackMessages : recentMessages.length || routerLookbackMessages,
            });
            if (!taskText) {
                queueRouterDecisionInsert({
                    event,
                    ctx,
                    targetType: routerTargetType,
                    decision: "skipped",
                    reason: "empty_task",
                    targetConfig,
                });
                return;
            }
            const parsedBlocks = parseSkillBlock(prompt);
            if (parsedBlocks.count > 0) {
                queueRouterDecisionInsert({
                    event,
                    ctx,
                    targetType: routerTargetType,
                    decision: "skipped",
                    reason: "skill_block_present",
                    targetConfig,
                    taskText,
                });
                return;
            }
            const configuredWorkspaceDir = toStringLike(ctx?.workspaceDir);
            const workspaceDir = configuredWorkspaceDir && configuredWorkspaceDir.trim().length > 0 ? configuredWorkspaceDir : pluginWorkspaceDir;
            const skillCache = await loadSkillCandidatesForWorkspace(workspaceDir, routerDiscovery);
            const config = await readOpenClawConfigSnapshot();
            const effectiveAgentId = resolveEffectiveAgentId(config, ctx, event, isRegularAgent);
            const agentAllowlist = resolveAgentSkillAllowlist(config, effectiveAgentId);
            const discoveredCandidateCount = skillCache.candidates.length;
            if (!discoveredCandidateCount) {
                queueRouterDecisionInsert({
                    event,
                    ctx,
                    targetType: routerTargetType,
                    decision: "skipped",
                    reason: "no_candidates_discovered",
                    targetConfig,
                    taskText,
                });
                return;
            }
            const candidates = skillCache.candidates.filter((candidate) => isCandidateAllowedForAgent(candidate, agentAllowlist));
            if (!candidates.length) {
                queueRouterDecisionInsert({
                    event,
                    ctx,
                    targetType: routerTargetType,
                    decision: "skipped",
                    reason: "no_candidates_after_allowlist",
                    targetConfig,
                    taskText,
                    counts: { candidateCount: discoveredCandidateCount },
                });
                return;
            }
            const { idf, avgDescLen } = buildIdfTable(candidates);
            const blocklist = new Set(routerBlocklist);
            const candidateByName = buildSkillIdentityMap(candidates);
            const availableCandidates = candidates.filter((skill) => !isSkillBlocked(skill, blocklist));
            if (!availableCandidates.length) {
                queueRouterDecisionInsert({
                    event,
                    ctx,
                    targetType: routerTargetType,
                    decision: "skipped",
                    reason: "all_candidates_blocked",
                    targetConfig,
                    taskText,
                    counts: { candidateCount: candidates.length, availableCount: 0 },
                });
                return;
            }
            const selected = [];
            const seen = new Set();
            const hasOverride = [];
            const nudgeScope = routerNudgeScope(ctx, event);
            const wasSuppressedRecently = async (skill) => {
                if (wasSkillHandledRecently(messageList, skill, routerRecencyWindow))
                    return true;
                if (wasNudgedRecentlyInMemory(nudgeScope, skill, turnNumber, routerRecencyWindow, routerRecencyFallbackMinutes))
                    return true;
                return wasNudgedRecentlyInDb(event, ctx, skill, turnNumber, routerRecencyWindow, routerRecencyFallbackMinutes);
            };
            let scoredForDecision = [];
            for (const override of routerOverrides) {
                if (!override.matcher.test(taskText))
                    continue;
                for (const skillName of override.skills || []) {
                    const skill = candidateByName.get(String(skillName).toLowerCase());
                    if (!skill)
                        continue;
                    if (isSkillBlocked(skill, blocklist))
                        continue;
                    const selectedKey = canonicalSkillId(skill);
                    if (seen.has(selectedKey))
                        continue;
                    if (await wasSuppressedRecently(skill))
                        continue;
                    hasOverride.push(skill);
                    seen.add(selectedKey);
                    if (hasOverride.length >= targetMaxSkillsToNudge)
                        break;
                }
                if (hasOverride.length >= targetMaxSkillsToNudge)
                    break;
            }
            if (hasOverride.length) {
                selected.push(...hasOverride);
            }
            else {
                const scored = availableCandidates
                    .map((skill) => ({
                    skill,
                    ...scoreSkill(taskText, skill, routerSkillKeywords, idf, avgDescLen),
                }))
                    .sort((a, b) => {
                    if (b.score !== a.score)
                        return b.score - a.score;
                    return a.skill.name.localeCompare(b.skill.name);
                });
                scoredForDecision = scored.map((row) => ({
                    ...row,
                    status: row.score >= targetMinScore ? "eligible" : "below_threshold",
                }));
                const eligibleScored = scored.filter((row) => row.score >= targetMinScore);
                for (const row of eligibleScored) {
                    const suppressed = await wasSuppressedRecently(row.skill);
                    if (suppressed) {
                        const decisionRow = scoredForDecision.find((entry) => canonicalSkillId(entry.skill) === canonicalSkillId(row.skill));
                        if (decisionRow)
                            decisionRow.status = "suppressed_recently";
                        continue;
                    }
                    const key = canonicalSkillId(row.skill);
                    if (seen.has(key))
                        continue;
                    selected.push(row.skill);
                    const decisionRow = scoredForDecision.find((entry) => canonicalSkillId(entry.skill) === key);
                    if (decisionRow)
                        decisionRow.status = "selected";
                    seen.add(key);
                    if (selected.length >= targetMaxSkillsToNudge)
                        break;
                }
            }
            if (!selected.length) {
                const reason = scoredForDecision.some((row) => row.score >= targetMinScore)
                    ? "all_suppressed_recently"
                    : "all_below_threshold";
                queueRouterDecisionInsert({
                    event,
                    ctx,
                    targetType: routerTargetType,
                    decision: "skipped",
                    reason,
                    targetConfig,
                    taskText,
                    counts: {
                        candidateCount: candidates.length,
                        availableCount: availableCandidates.length,
                        scoredCount: scoredForDecision.length,
                        selectedCount: 0,
                    },
                    topCandidates: scoredForDecision,
                });
                return;
            }
            const taskExcerpt = sanitizeValue(scrubSecrets(taskText, piiEnabled), 200, piiEnabled);
            for (const skill of selected) {
                const isOverride = hasOverride.some((entry) => canonicalSkillId(entry) === canonicalSkillId(skill));
                const row = isOverride ? { score: 0, reason: "override" } : scoreSkill(taskText, skill, routerSkillKeywords, idf, avgDescLen);
                queueNudgeInsert({
                    event,
                    ctx,
                    skillName: skill.name,
                    skillKey: skill.key,
                    skillPath: skill.filePath,
                    score: row.score,
                    matchReason: row.reason,
                    turnNumber,
                    taskExcerpt,
                });
                rememberRouterNudge(nudgeScope, skill, turnNumber);
            }
            const selectedKeys = new Set(selected.map((skill) => canonicalSkillId(skill)));
            const topCandidates = hasOverride.length
                ? selected.map((skill) => ({ skill, score: 0, reason: "override", status: "selected" }))
                : scoredForDecision.map((row) => selectedKeys.has(canonicalSkillId(row.skill)) ? { ...row, status: "selected" } : row);
            queueRouterDecisionInsert({
                event,
                ctx,
                targetType: routerTargetType,
                decision: "nudged",
                reason: hasOverride.length ? "override" : "score_threshold",
                targetConfig,
                taskText,
                counts: {
                    candidateCount: candidates.length,
                    availableCount: availableCandidates.length,
                    scoredCount: scoredForDecision.length,
                    selectedCount: selected.length,
                },
                selectedSkills: selected,
                topCandidates,
            });
            return { prependContext: formatNudge(selected) };
        }
        api.on("session_start", async (event, ctx) => {
            enqueueEvent({
                v: 1,
                ts: new Date().toISOString(),
                type: "session_start",
                ...buildBase(event, ctx),
            });
        });
        api.on("session_end", async (event, ctx) => {
            const keys = buildScopeKeys(ctx, event);
            const remaining = candidatesForScope(keys);
            for (const execution of remaining) {
                finalizeExecution(execution.id, "session-end");
            }
            enqueueEvent({
                v: 1,
                ts: new Date().toISOString(),
                type: "session_end",
                ...buildBase(event, ctx),
                durationMs: event.durationMs,
                messageCount: event.messageCount,
            });
        });
        api.on("before_tool_call", async (event, ctx) => {
            const now = new Date().toISOString();
            const toolName = toStringLike(event.toolName) || "";
            const params = event.params || {};
            enqueueEvent({
                v: 1,
                ts: now,
                type: "tool_call_start",
                ...buildBase(event, ctx),
                toolName,
                toolCallId: toStringLike(event.toolCallId),
                params: includeToolParams ? buildToolParams(toolName, params, redactKeys, piiEnabled) : undefined,
            });
            const scopeKeys = buildScopeKeys(ctx, event);
            const skillPath = extractSkillPathFromParams(params);
            if (toolName === "read" && skillPath) {
                const execution = startExecutionFromSkillRead(ctx, event, skillPath, now);
                attachToolCall(scopeKeys, event, execution);
                return;
            }
            attachToolCall(scopeKeys, event);
        });
        api.on("after_tool_call", async (event, ctx) => {
            const now = new Date().toISOString();
            const toolName = toStringLike(event.toolName) || "";
            const params = event.params || {};
            const scopeKeys = buildScopeKeys(ctx, event);
            const linked = detachToolCall(event, scopeKeys);
            enqueueEvent({
                v: 1,
                ts: now,
                type: "tool_call_end",
                ...buildBase(event, ctx),
                toolName,
                toolCallId: toStringLike(event.toolCallId),
                params: includeToolParams ? buildToolParams(toolName, params, redactKeys, piiEnabled) : undefined,
                durationMs: typeof event.durationMs === "number" ? Number(event.durationMs) : undefined,
                success: event.error ? 0 : 1,
                error: toStringLike(event.error),
                skillName: linked?.skillName,
                skillPath: linked?.skillPath,
            });
        });
        api.on("message_received", async (event, ctx) => {
            const text = toStringLike(event.content);
            if (text === undefined)
                return;
            const scopeKeys = buildScopeKeys(ctx, event);
            const execution = pickExecution(scopeKeys, true);
            const msg = makeMessageCapture(text, "user", event.metadata, redactKeys, captureMessageContent, detectMessageSignals(execution, text, "user"), piiEnabled);
            if (!msg)
                return;
            const scope = buildMessageScope(ctx, event);
            addMessage(scope, msg);
            if (execution) {
                onFollowupMessage(execution, msg);
            }
        });
        api.on("message_sent", async (event, ctx) => {
            const text = toStringLike(event.content);
            if (text === undefined)
                return;
            const scopeKeys = buildScopeKeys(ctx, event);
            const execution = pickExecution(scopeKeys, true);
            const msg = makeMessageCapture(text, "assistant", event.metadata, redactKeys, captureMessageContent, detectMessageSignals(execution, text, "assistant"), piiEnabled);
            if (!msg)
                return;
            const scope = buildMessageScope(ctx, event);
            addMessage(scope, msg);
            if (execution) {
                onFollowupMessage(execution, msg);
            }
        });
        api.on("before_prompt_build", async (event, ctx) => {
            const routerContext = await maybeNudgeSkills(event, ctx);
            if (detectSkillBlocks) {
                const prompt = toStringLike(event.prompt);
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
                            const resolvedPath = resolveSkillPathFromBlock(block.name, block.location);
                            if (!resolvedPath)
                                continue;
                            startExecutionFromSkillRead(ctx, event, resolvedPath, timestamp, true);
                        }
                    }
                }
            }
            if (routerContext) {
                return routerContext;
            }
        });
        function withTimeout(promise, timeoutMs) {
            let timer;
            return Promise.race([
                promise,
                new Promise((_resolve, reject) => {
                    timer = setTimeout(() => reject(new Error("flush-timeout")), timeoutMs);
                }),
            ]).finally(() => {
                if (timer)
                    clearTimeout(timer);
            });
        }
        function finalizeAllExecutions(reason) {
            for (const execution of [...executionsById.values()]) {
                finalizeExecution(execution.id, reason);
            }
        }
        const shutdownParticipant = {
            flush: (reason) => {
                finalizeAllExecutions(reason);
            },
        };
        sharedState.shutdownParticipants.add(shutdownParticipant);
        async function flushPendingWrites(reason) {
            for (const participant of [...sharedState.shutdownParticipants]) {
                try {
                    participant.flush(reason);
                }
                catch (err) {
                    log.error(`skill-usage-audit: shutdown finalize failed: ${String(err)}`);
                }
            }
            sharedState.acceptingWrites = false;
            try {
                await withTimeout(sharedState.dbChain, 2500);
            }
            catch (err) {
                log.error(`skill-usage-audit: shutdown flush failed: ${String(err)}`);
            }
            if (dbState.backend) {
                try {
                    dbState.backend.close();
                }
                catch {
                    // ignore
                }
                dbState.backend = null;
                dbState.statements = null;
            }
            sharedState.dbInitPromise = null;
        }
        function requestFlush(reason) {
            if (!sharedState.shutdownPromise) {
                sharedState.shutdownPromise = flushPendingWrites(reason);
            }
            return sharedState.shutdownPromise;
        }
        // Use gateway_stop lifecycle hook instead of process signal handlers.
        api.on("gateway_stop", async () => {
            await requestFlush("gateway_stop");
        });
        sharedState.registeredFullApis.add(apiRegistrationToken);
        log.info(`skill-usage-audit plugin registered (registrationMode=${registrationMode})`);
    },
});
