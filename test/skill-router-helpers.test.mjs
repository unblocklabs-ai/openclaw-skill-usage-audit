import test from "node:test";
import assert from "node:assert/strict";

import {
  buildSkillIdentityMap,
  canonicalSkillId,
  isCandidateAllowedForAgent,
  isCandidateDisabledByEntries,
  isSkillBlocked,
  parseSkillKeyFromFrontmatter,
  resolveAgentSkillAllowlist,
  resolveEffectiveAgentId,
  resolveRouterTargetConfig,
  selectRouterTaskText,
  shouldSuppressRecentNudgeRecord,
} from "../skill-router-helpers.mjs";

test("parseSkillKeyFromFrontmatter supports inline JSON and nested metadata", () => {
  assert.equal(parseSkillKeyFromFrontmatter(`---
name: Alpha
metadata: {"openclaw":{"skillKey":"alpha-key"}}
---
`), "alpha-key");

  assert.equal(parseSkillKeyFromFrontmatter(`---
name: Beta
metadata:
  openclaw:
    skillKey: beta-key # comment
---
`), "beta-key");
});

test("skill identity keeps duplicate names with different keys addressable", () => {
  const first = { key: "one", name: "Shared", filePath: "/one/SKILL.md" };
  const second = { key: "two", name: "Shared", filePath: "/two/SKILL.md" };
  const byAlias = buildSkillIdentityMap([first, second]);

  assert.equal(canonicalSkillId(first), "one");
  assert.equal(canonicalSkillId(second), "two");
  assert.equal(byAlias.get("one"), first);
  assert.equal(byAlias.get("two"), second);
  assert.equal(byAlias.has("shared"), false);
});

test("blocklist and allowlists honor both skill key and display name", () => {
  const candidate = { key: "Alpha-Key", name: "Alpha Skill" };

  assert.equal(isSkillBlocked(candidate, new Set(["alpha-key"])), true);
  assert.equal(isSkillBlocked(candidate, new Set(["alpha skill"])), true);
  assert.equal(isSkillBlocked(candidate, new Set(["other"])), false);

  assert.equal(isCandidateAllowedForAgent(candidate, ["alpha-key"]), true);
  assert.equal(isCandidateAllowedForAgent(candidate, ["alpha skill"]), true);
  assert.equal(isCandidateAllowedForAgent(candidate, ["other"]), false);
});

test("disabled skill entries are matched case-insensitively by key or display name", () => {
  assert.equal(
    isCandidateDisabledByEntries(
      { key: "Alpha-Key", name: "Alpha Skill" },
      { "alpha-key": { enabled: false } },
    ),
    true,
  );
  assert.equal(
    isCandidateDisabledByEntries(
      { key: "Beta-Key", name: "Beta Skill" },
      { "beta skill": { enabled: false } },
    ),
    true,
  );
  assert.equal(
    isCandidateDisabledByEntries(
      { key: "Gamma-Key", name: "Gamma Skill" },
      { other: { enabled: false } },
    ),
    false,
  );
});

test("agent allowlist resolves explicit and default regular agents", () => {
  const config = {
    agents: {
      defaultAgentId: "main",
      defaults: { skills: ["default-skill"] },
      list: [
        { id: "main", skills: ["main-skill"] },
        { id: "research", skills: ["research-skill"] },
      ],
    },
  };

  assert.equal(resolveEffectiveAgentId(config, {}, {}, true), "main");
  assert.equal(resolveEffectiveAgentId(config, { agentId: "research" }, {}, true), "research");
  assert.deepEqual(resolveAgentSkillAllowlist(config, "main"), ["main-skill"]);
  assert.deepEqual(resolveAgentSkillAllowlist(config, "research"), ["research-skill"]);
});

test("recent nudge suppression respects turn windows before time fallback", () => {
  const now = 1_000_000;
  const fallbackWindow = 30 * 60 * 1000;

  assert.equal(
    shouldSuppressRecentNudgeRecord({ turnNumber: 2, seenAt: now - 10_000 }, 20, 3, now, fallbackWindow),
    false,
  );
  assert.equal(
    shouldSuppressRecentNudgeRecord({ turnNumber: 18, seenAt: now - 10_000 }, 20, 3, now, fallbackWindow),
    true,
  );
  assert.equal(
    shouldSuppressRecentNudgeRecord({ turnNumber: null, seenAt: now - 10_000 }, 20, 3, now, fallbackWindow),
    true,
  );
});

test("router target config keeps stricter agent default and inherits globals for other targets", () => {
  const routerConfig = {
    minScore: 7,
    maxSkillsToNudge: 2,
  };
  const defaults = {
    defaultMaxSkills: 1,
    defaultMinScore: 6,
    defaultAgentMinScore: 8,
  };

  assert.deepEqual(resolveRouterTargetConfig(routerConfig, "agent", defaults), {
    maxSkillsToNudge: 2,
    minScore: 8,
  });
  assert.deepEqual(resolveRouterTargetConfig(routerConfig, "subagent", defaults), {
    maxSkillsToNudge: 2,
    minScore: 7,
  });
  assert.deepEqual(resolveRouterTargetConfig({ ...routerConfig, cronMinScore: 9, cronMaxSkillsToNudge: 3 }, "cron", defaults), {
    maxSkillsToNudge: 3,
    minScore: 9,
  });
});

test("selectRouterTaskText supports prompt, latest user, and recent-message windows", () => {
  const messages = [
    { role: "user", content: "older user" },
    { role: "assistant", content: "older assistant" },
    { role: "user", content: "latest user" },
  ];

  assert.equal(
    selectRouterTaskText({ prompt: "prompt text", messages, mode: "promptOnly", lookbackMessages: 2 }),
    "prompt text",
  );
  assert.equal(
    selectRouterTaskText({ prompt: "prompt text", messages, mode: "latestUser", lookbackMessages: 2 }),
    "latest user",
  );
  assert.equal(
    selectRouterTaskText({ prompt: "prompt text", messages, mode: "recentMessages", lookbackMessages: 2 }),
    "older assistant\nlatest user",
  );
});
