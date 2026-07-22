import { normalizeStringList } from "./skill-roots.mjs";

const DEFAULT_AGENT_ID = "main";

function toStringLike(value) {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length ? trimmed : undefined;
}

function parseSkillMetadataObject(value) {
  if (!value) return undefined;
  let raw = value.trim();
  if (!raw) return undefined;
  if ((raw.startsWith("\"") && raw.endsWith("\"")) || (raw.startsWith("'") && raw.endsWith("'"))) {
    raw = raw.slice(1, -1);
  }
  try {
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return undefined;
    return parsed;
  } catch {
    return undefined;
  }
}

function unquoteFrontmatterScalar(value) {
  let raw = value.trim();
  if (!raw) return undefined;
  const commentIdx = raw.indexOf(" #");
  if (commentIdx >= 0) raw = raw.slice(0, commentIdx).trim();
  if ((raw.startsWith("\"") && raw.endsWith("\"")) || (raw.startsWith("'") && raw.endsWith("'"))) {
    raw = raw.slice(1, -1);
  }
  return raw.trim() || undefined;
}

function frontmatterIndent(line) {
  const match = /^ */.exec(line);
  return match?.[0].length || 0;
}

export function parseSkillKeyFromFrontmatter(content, maxLines = 55) {
  const lines = content.split(/\r?\n/).slice(0, maxLines);
  if (!lines.length || lines[0]?.trim() !== "---") return undefined;

  for (let i = 1; i < lines.length; i += 1) {
    const rawLine = lines[i];
    const line = rawLine.trim();
    if (line === "---") break;
    if (!line || line.startsWith("#")) continue;

    const colon = line.indexOf(":");
    if (colon < 0) continue;
    const key = line.slice(0, colon).trim().toLowerCase();
    if (key !== "metadata") continue;

    const metadata = parseSkillMetadataObject(line.slice(colon + 1));
    if (metadata) {
      const openclaw = metadata.openclaw;
      if (!openclaw || typeof openclaw !== "object" || Array.isArray(openclaw)) return undefined;
      return toStringLike(openclaw.skillKey);
    }

    const metadataIndent = frontmatterIndent(rawLine);
    let inOpenClaw = false;
    let openClawIndent = -1;
    for (let j = i + 1; j < lines.length; j += 1) {
      const nestedRaw = lines[j];
      const nested = nestedRaw.trim();
      if (nested === "---") break;
      if (!nested || nested.startsWith("#")) continue;

      const indent = frontmatterIndent(nestedRaw);
      if (indent <= metadataIndent) break;

      if (!inOpenClaw) {
        if (/^openclaw\s*:\s*$/i.test(nested)) {
          inOpenClaw = true;
          openClawIndent = indent;
        }
        continue;
      }

      if (indent <= openClawIndent) break;
      const skillKeyMatch = /^skillKey\s*:\s*(.+)$/i.exec(nested);
      if (skillKeyMatch) return unquoteFrontmatterScalar(skillKeyMatch[1]);
    }
  }

  return undefined;
}

export function canonicalSkillId(candidate) {
  return String(candidate?.key || candidate?.name || "").trim().toLowerCase();
}

function skillAliases(candidate) {
  return [candidate?.key, candidate?.name]
    .map((entry) => String(entry || "").trim().toLowerCase())
    .filter(Boolean);
}

function buildNormalizedSkillEntryMap(entries) {
  const out = new Map();
  if (!entries || typeof entries !== "object" || Array.isArray(entries)) return out;

  for (const [key, entry] of Object.entries(entries)) {
    const normalized = String(key || "").trim().toLowerCase();
    if (normalized) out.set(normalized, entry);
  }
  return out;
}

export function isCandidateDisabledByEntries(candidate, entries) {
  const normalizedEntries = buildNormalizedSkillEntryMap(entries);
  if (!normalizedEntries.size) return false;

  return skillAliases(candidate).some((alias) => normalizedEntries.get(alias)?.enabled === false);
}

export function buildSkillIdentityMap(candidates) {
  const byAlias = new Map();
  const nameCounts = new Map();
  for (const candidate of candidates) {
    const name = String(candidate.name || "").trim().toLowerCase();
    if (name) nameCounts.set(name, (nameCounts.get(name) || 0) + 1);
  }

  for (const candidate of candidates) {
    const key = String(candidate.key || "").trim().toLowerCase();
    const name = String(candidate.name || "").trim().toLowerCase();
    if (key && !byAlias.has(key)) byAlias.set(key, candidate);
    if (name && nameCounts.get(name) === 1 && !byAlias.has(name)) byAlias.set(name, candidate);
  }
  return byAlias;
}

export function isSkillBlocked(candidate, blocklist) {
  return skillAliases(candidate).some((alias) => blocklist.has(alias));
}

export function normalizedSkillListIncludes(list, candidate) {
  const normalized = new Set(normalizeStringList(list).map((entry) => entry.toLowerCase()));
  return skillAliases(candidate).some((alias) => normalized.has(alias));
}

export function resolveAgentSkillAllowlist(config, agentId) {
  const agents = config?.agents;
  if (!agents) return undefined;

  const matchingAgent = Array.isArray(agents.list)
    ? agents.list.find((entry) => toStringLike(entry?.id) === agentId)
    : undefined;

  if (matchingAgent && Object.prototype.hasOwnProperty.call(matchingAgent, "skills")) {
    return normalizeStringList(matchingAgent.skills);
  }
  if (agentId) {
    const keyedAgent = agents[agentId];
    if (keyedAgent && typeof keyedAgent === "object" && !Array.isArray(keyedAgent)) {
      if (Object.prototype.hasOwnProperty.call(keyedAgent, "skills")) {
        return normalizeStringList(keyedAgent.skills);
      }
    }
  }
  if (agents.defaults && Object.prototype.hasOwnProperty.call(agents.defaults, "skills")) {
    return normalizeStringList(agents.defaults.skills);
  }
  return undefined;
}

function resolveDefaultAgentId(config) {
  const agents = config?.agents;
  const direct =
    toStringLike(agents?.defaultAgentId) ||
    toStringLike(agents?.defaultAgent) ||
    toStringLike(agents?.defaults?.id);
  if (direct) return direct;

  if (Array.isArray(agents?.list)) {
    const first = agents.list.map((entry) => toStringLike(entry?.id)).find(Boolean);
    if (first) return first;
  }

  if (agents && typeof agents === "object") {
    const firstConfiguredAgent = Object.keys(agents).find((key) => key !== "defaults" && key !== "list");
    if (firstConfiguredAgent) return firstConfiguredAgent;
  }

  return DEFAULT_AGENT_ID;
}

export function resolveEffectiveAgentId(config, ctx, event, isRegularAgent) {
  const metadata = event?.metadata && typeof event.metadata === "object" && !Array.isArray(event.metadata)
    ? event.metadata
    : undefined;

  return (
    toStringLike(ctx?.agentId) ||
    toStringLike(event?.agentId) ||
    toStringLike(metadata?.agentId) ||
    (isRegularAgent ? resolveDefaultAgentId(config) : undefined)
  );
}

export function isCandidateAllowedForAgent(candidate, allowlist) {
  if (!allowlist) return true;
  return normalizedSkillListIncludes(allowlist, candidate);
}

export function shouldSuppressRecentNudgeRecord(record, turnNumber, recencyWindow, nowMs, fallbackWindowMs) {
  if (!record) return false;
  if (turnNumber !== null && turnNumber !== undefined && record.turnNumber !== null && record.turnNumber !== undefined) {
    return turnNumber >= record.turnNumber && turnNumber - record.turnNumber < recencyWindow;
  }
  return nowMs - record.seenAt < fallbackWindowMs;
}

function parseNumber(value, fallback, min, integer = false) {
  const parsed =
    typeof value === "number"
      ? value
      : Number.parseFloat(typeof value === "string" ? value.trim() : "");
  const normalized = integer ? Math.floor(parsed) : parsed;

  if (!Number.isFinite(normalized)) return fallback;
  if (min !== undefined && normalized < min) return fallback;
  return normalized;
}

export function resolveRouterTargetConfig(routerConfig = {}, target, defaults = {}) {
  const defaultMaxSkills = defaults.defaultMaxSkills ?? 1;
  const defaultMinScore = defaults.defaultMinScore ?? 6;
  const defaultAgentMinScore = defaults.defaultAgentMinScore ?? 8;

  const globalMaxSkillsToNudge = parseNumber(routerConfig.maxSkillsToNudge, defaultMaxSkills, 1, true);
  const globalMinScore = parseNumber(routerConfig.minScore, defaultMinScore, 0, false);

  const maxKey = `${target}MaxSkillsToNudge`;
  const minKey = `${target}MinScore`;
  const fallbackMinScore = target === "agent" ? defaultAgentMinScore : globalMinScore;

  return {
    maxSkillsToNudge: parseNumber(routerConfig[maxKey], globalMaxSkillsToNudge, 1, true),
    minScore: parseNumber(routerConfig[minKey], fallbackMinScore, 0, false),
  };
}

function textFromMessage(message) {
  if (typeof message === "string") return message;
  if (!message || typeof message !== "object" || Array.isArray(message)) return undefined;

  if (typeof message.content === "string") return message.content;
  if (message.content && typeof message.content === "object" && !Array.isArray(message.content)) {
    if (typeof message.content.text === "string") return message.content.text;
  }
  if (typeof message.text === "string") return message.text;
  if (typeof message.body === "string") return message.body;
  if (typeof message.message === "string") return message.message;
  return undefined;
}

function roleFromMessage(message) {
  if (!message || typeof message !== "object" || Array.isArray(message)) return undefined;
  return toStringLike(message.role)?.toLowerCase();
}

export function selectRouterTaskText({ prompt = "", messages = [], mode = "recentMessages", lookbackMessages = 8 } = {}) {
  const messageList = Array.isArray(messages) ? messages : [];
  const recentMessages = messageList.slice(-Math.max(1, Math.floor(lookbackMessages)));
  const latestUserMessage = [...messageList].reverse().find((message) => roleFromMessage(message) === "user" && textFromMessage(message));
  const latestUserText = textFromMessage(latestUserMessage);

  if (mode === "promptOnly") return prompt;
  if (mode === "latestUser") return latestUserText || prompt;

  return recentMessages.map((message) => textFromMessage(message)).filter(Boolean).join("\n").trim() || prompt;
}
