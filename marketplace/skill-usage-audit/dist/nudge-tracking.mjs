import { realpath } from "node:fs/promises";
import { dirname, isAbsolute, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const DIRECT_READ_TOOL_NAMES = new Set([
  "read",
  "read_file",
  "readfile",
  "read_text_file",
  "filesystem_read_file",
  "fs_read_file",
  "get_file_contents",
  "open_file",
  "view_file",
]);

const SHELL_TOOL_NAMES = new Set([
  "bash",
  "exec",
  "exec_command",
  "run_command",
  "shell",
]);

const SIMPLE_READ_COMMANDS = new Set([
  "bat",
  "cat",
  "less",
  "more",
]);

const SCRIPT_RUNNERS = new Set([
  "bash",
  "deno",
  "node",
  "perl",
  "python",
  "python3",
  "ruby",
  "sh",
  "zsh",
]);

const PATH_PARAM_KEYS = [
  "path",
  "file_path",
  "filePath",
  "filename",
  "file",
  "uri",
  "resourceUri",
  "resource_uri",
];

function text(value) {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed || undefined;
}

function normalizeToolName(value) {
  return String(value || "").trim().toLowerCase().replace(/[.:/-]+/g, "_");
}

function basenameOfCommand(value) {
  const normalized = String(value || "").replace(/\\/g, "/");
  return normalized.slice(normalized.lastIndexOf("/") + 1).toLowerCase();
}

function isDirectReadTool(toolName) {
  const normalized = normalizeToolName(toolName);
  if (DIRECT_READ_TOOL_NAMES.has(normalized)) return true;
  return /(?:^|_)(?:readfile|read(?:_text)?_file|read_resource|get_file_contents|open_file|view_file)$/.test(normalized);
}

function isShellTool(toolName) {
  return SHELL_TOOL_NAMES.has(normalizeToolName(toolName));
}

function pathFromFileUri(value) {
  try {
    return fileURLToPath(value);
  } catch {
    return undefined;
  }
}

function resolveAccessPath(rawPath, cwd) {
  const candidate = text(rawPath);
  if (!candidate || candidate.includes("\0") || candidate.includes("\n")) return undefined;

  let pathValue = candidate;
  if (/^file:\/\//i.test(pathValue)) {
    pathValue = pathFromFileUri(pathValue);
    if (!pathValue) return undefined;
  }

  if (pathValue === "~" || pathValue.startsWith("~/")) {
    const home = process.env.HOME || process.env.USERPROFILE;
    if (!home) return undefined;
    return resolve(home, pathValue === "~" ? "." : pathValue.slice(2));
  }

  if (isAbsolute(pathValue)) return resolve(pathValue);
  const base = text(cwd);
  if (!base || !isAbsolute(base)) return undefined;
  return resolve(base, pathValue);
}

async function realpathOrUndefined(pathValue) {
  try {
    return await realpath(pathValue);
  } catch {
    return undefined;
  }
}

export async function resolvePathIdentity(rawPath, cwd) {
  const lexicalPath = resolveAccessPath(rawPath, cwd);
  if (!lexicalPath) return undefined;
  return {
    lexicalPath,
    realPath: await realpathOrUndefined(lexicalPath),
  };
}

function isWithin(candidate, parent) {
  const rel = relative(parent, candidate);
  return rel !== "" && rel !== ".." && !rel.startsWith(`..${process.platform === "win32" ? "\\" : "/"}`) && !isAbsolute(rel);
}

export async function classifyAccessForSkill(accessPath, skillPath, skillRealPath, cwd) {
  const access = await resolvePathIdentity(accessPath, cwd);
  const skill = await resolvePathIdentity(skillPath, cwd);
  if (!access || !skill) return undefined;

  const exactLexical = access.lexicalPath === skill.lexicalPath;
  const exactReal = Boolean(access.realPath && (skillRealPath || skill.realPath) && access.realPath === (skillRealPath || skill.realPath));
  if (exactLexical || exactReal) return "skill";

  const lexicalDescendant = isWithin(access.lexicalPath, dirname(skill.lexicalPath));
  const resolvedSkillPath = skillRealPath || skill.realPath;
  const realDescendant = Boolean(access.realPath && resolvedSkillPath && isWithin(access.realPath, dirname(resolvedSkillPath)));
  return lexicalDescendant || realDescendant ? "resource" : undefined;
}

function collectStructuredPaths(params) {
  const paths = [];
  for (const key of PATH_PARAM_KEYS) {
    const value = params?.[key];
    if (Array.isArray(value)) {
      for (const entry of value) {
        const candidate = text(entry);
        if (candidate) paths.push(candidate);
      }
      continue;
    }
    const candidate = text(value);
    if (candidate) paths.push(candidate);
  }
  return paths;
}

function shellTokenize(command) {
  const tokens = [];
  let value = "";
  let quote;
  let escaped = false;
  let ambiguous = false;

  const push = () => {
    if (value) tokens.push({ value, operator: false });
    value = "";
  };

  for (let i = 0; i < command.length; i += 1) {
    const char = command[i];
    const next = command[i + 1];

    if (escaped) {
      value += char;
      escaped = false;
      continue;
    }
    if (char === "\\" && quote !== "'") {
      escaped = true;
      continue;
    }
    if (quote) {
      if (char === quote) {
        quote = undefined;
      } else {
        if (quote === '"' && (char === "$" || char === "`")) ambiguous = true;
        value += char;
      }
      continue;
    }
    if (char === "'" || char === '"') {
      quote = char;
      continue;
    }
    if (char === "$" || char === "`" || (char === "(" && next === "(")) {
      ambiguous = true;
      value += char;
      continue;
    }
    if (char === "#" && !value) {
      while (i < command.length && command[i] !== "\n") i += 1;
      push();
      continue;
    }
    if (/\s/.test(char)) {
      push();
      continue;
    }
    if (char === ";" || char === "|" || char === "<" || char === ">" || char === "&") {
      push();
      let operator = char;
      if (char === "<" && next === "<" && command[i + 2] === "<") {
        operator = "<<<";
        i += 2;
      } else if (char === "&" && next === ">" && command[i + 2] === ">") {
        operator = "&>>";
        i += 2;
      } else if (next === char || (char === "&" && next === ">")) {
        operator += next;
        i += 1;
      }
      tokens.push({ value: operator, operator: true });
      continue;
    }
    value += char;
  }
  push();
  if (quote || escaped) ambiguous = true;
  return { tokens, ambiguous };
}

function optionlessOperands(args, optionsWithValues = new Set()) {
  const out = [];
  let skipValue = false;
  let afterOptions = false;
  for (const arg of args) {
    if (skipValue) {
      skipValue = false;
      continue;
    }
    if (!afterOptions && arg === "--") {
      afterOptions = true;
      continue;
    }
    if (!afterOptions && arg.startsWith("-")) {
      if (optionsWithValues.has(arg)) skipValue = true;
      continue;
    }
    out.push(arg);
  }
  return out;
}

function extractRedirections(words) {
  const commandWords = [];
  const inputPaths = [];
  let ambiguous = false;

  for (let index = 0; index < words.length; index += 1) {
    const word = words[index];
    if (!["<", "<<", "<<<", ">", ">>", "&>", "&>>"].includes(word)) {
      commandWords.push(word);
      continue;
    }

    if ([">", ">>", "&>", "&>>"].includes(word) && /^\d+$/.test(commandWords.at(-1) || "")) {
      commandWords.pop();
    }

    const target = words[index + 1];
    if (word === "<" && target) inputPaths.push(target);
    if (target) index += 1;
    if (word === "<<" || word === "<<<") ambiguous = true;
    if (word === "<<") break;
  }

  return { commandWords, inputPaths, ambiguous };
}

function shellSegmentAccesses(rawWords, cwd) {
  const accesses = [];
  if (!rawWords.length) return { accesses, cwd, ambiguous: false };

  const { commandWords: words, inputPaths, ambiguous: redirectionAmbiguous } = extractRedirections(rawWords);
  for (const path of inputPaths) accesses.push({ path, kind: "read" });
  if (!words.length) return { accesses, cwd, ambiguous: inputPaths.length === 0 };
  const result = (ambiguous = false, nextCwd = cwd) => ({
    accesses,
    cwd: nextCwd,
    ambiguous: redirectionAmbiguous || ambiguous,
  });

  let cursor = 0;
  while (cursor < words.length && /^[A-Za-z_][A-Za-z0-9_]*=/.test(words[cursor])) cursor += 1;
  while (["command", "env", "nohup", "sudo"].includes(basenameOfCommand(words[cursor]))) {
    cursor += 1;
    while (cursor < words.length && (words[cursor].startsWith("-") || /^[A-Za-z_][A-Za-z0-9_]*=/.test(words[cursor]))) cursor += 1;
  }
  if (cursor >= words.length) return { accesses, cwd, ambiguous: false };

  const commandToken = words[cursor];
  const commandName = basenameOfCommand(commandToken);
  const args = words.slice(cursor + 1);

  if (commandName === "cd") {
    const next = args.find((arg) => !arg.startsWith("-"));
    const resolved = resolveAccessPath(next, cwd);
    return result(!resolved, resolved || cwd);
  }

  if (SIMPLE_READ_COMMANDS.has(commandName)) {
    for (const operand of optionlessOperands(args)) accesses.push({ path: operand, kind: "read" });
    return result();
  }

  if (commandName === "head" || commandName === "tail" || commandName === "wc") {
    const optionsWithValues = new Set(["-c", "-m", "-n", "--bytes", "--lines", "--max-unchanged-stats"]);
    for (const operand of optionlessOperands(args, optionsWithValues)) accesses.push({ path: operand, kind: "read" });
    return result();
  }

  if (commandName === "sed") {
    const operands = optionlessOperands(args, new Set(["-e", "-f", "--expression", "--file"]));
    const hasExplicitPattern = args.some((arg) => ["-e", "-f", "--file", "--regexp"].includes(arg));
    const fileOperands = hasExplicitPattern ? operands : operands.length > 1 ? operands.slice(1) : [];
    for (const operand of fileOperands) accesses.push({ path: operand, kind: "read" });
    return result(operands.length === 1);
  }

  if (commandName === "grep" || commandName === "rg" || commandName === "awk") {
    const operands = optionlessOperands(args, new Set([
      "-A", "-B", "-C", "-e", "-f", "-g", "-m", "--after-context", "--before-context",
      "--context", "--file", "--glob", "--max-count", "--regexp", "--type", "-t",
    ]));
    const hasExplicitPattern = args.some((arg) => ["-e", "-f", "--file", "--regexp"].includes(arg));
    const fileOperands = hasExplicitPattern ? operands : operands.length > 1 ? operands.slice(1) : [];
    for (const operand of fileOperands) accesses.push({ path: operand, kind: "read" });
    return result(fileOperands.length === 0);
  }

  if (commandName === "source" || commandName === ".") {
    const operand = args.find((arg) => !arg.startsWith("-"));
    if (operand) accesses.push({ path: operand, kind: "execute" });
    return result(!operand);
  }

  if (SCRIPT_RUNNERS.has(commandName)) {
    const operands = optionlessOperands(args, new Set(["-c", "-e", "--eval", "--execute"]));
    const script = operands[0];
    if (script) accesses.push({ path: script, kind: "execute" });
    return result(!script && args.some((arg) => arg.includes("SKILL.md")));
  }

  if (commandToken.includes("/") || commandToken.startsWith(".")) {
    accesses.push({ path: commandToken, kind: "execute" });
    return result();
  }

  return result(words.some((word) => /(?:^|[\\/])SKILL\.md$/i.test(word)));
}

function extractShellAccesses(command, cwd) {
  if (Array.isArray(command) && command.every((entry) => typeof entry === "string")) {
    const [commandToken, ...args] = command;
    return shellSegmentAccesses([commandToken, ...args], cwd);
  }
  const commandText = text(command);
  if (!commandText) return { accesses: [], ambiguous: true };

  const parsed = shellTokenize(commandText);
  const accesses = [];
  let segment = [];
  let activeCwd = cwd;
  let ambiguous = parsed.ambiguous;

  const flush = () => {
    if (!segment.length) return;
    const result = shellSegmentAccesses(segment, activeCwd);
    for (const access of result.accesses) accesses.push({ ...access, cwd: activeCwd });
    activeCwd = result.cwd;
    ambiguous ||= result.ambiguous;
    segment = [];
  };

  for (const token of parsed.tokens) {
    if (token.operator && [";", "&&", "||", "|"].includes(token.value)) {
      const cwdBeforePipeline = activeCwd;
      flush();
      if (token.value === "|") activeCwd = cwdBeforePipeline;
      continue;
    }
    segment.push(token.value);
  }
  flush();
  return { accesses, ambiguous };
}

export function extractToolAccesses(toolName, params = {}, contextCwd) {
  const cwd = text(params.cwd) || text(params.workdir) || text(contextCwd);
  if (isDirectReadTool(toolName)) {
    const paths = collectStructuredPaths(params);
    return {
      accesses: paths.map((path) => ({ path, cwd, kind: "read" })),
      ambiguous: paths.length === 0,
      supported: true,
    };
  }

  if (isShellTool(toolName)) {
    const command = params.command ?? params.cmd ?? params.script;
    const parsed = extractShellAccesses(command, cwd);
    return { ...parsed, supported: true };
  }

  return { accesses: [], ambiguous: false, supported: false };
}

function failureToken(value) {
  return typeof value === "string" && ["blocked", "error", "failed", "failure", "timeout"].includes(value.trim().toLowerCase());
}

export function isToolCallFailure(event) {
  if (text(event?.error)) return true;
  const result = event?.result;
  if (!result || typeof result !== "object" || Array.isArray(result)) return false;
  if (result.isError === true || result.success === false || result.ok === false) return true;
  if (failureToken(result.status) || failureToken(result.outcome)) return true;
  for (const key of ["exitCode", "exit_code", "code"]) {
    if (typeof result[key] === "number" && result[key] !== 0) return true;
  }
  const details = result.details;
  if (details && typeof details === "object" && !Array.isArray(details)) {
    if (details.isError === true || details.success === false || failureToken(details.status)) return true;
  }
  return false;
}

export function stableToolEventKey({ nudgeId, toolCallId, toolName, eventType, accessPath }) {
  const callId = text(toolCallId);
  if (!callId) return undefined;
  return [nudgeId, callId, normalizeToolName(toolName), eventType, String(accessPath || "")].join("\0");
}
