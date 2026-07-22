import test from "node:test";
import assert from "node:assert/strict";
import { mkdir, mkdtemp, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

import {
  classifyAccessForSkill,
  extractToolAccesses,
  isToolCallFailure,
  resolvePathIdentity,
  stableToolEventKey,
} from "../nudge-tracking.mjs";

test("extractToolAccesses recognizes structured and quoted shell reads without substring matching", () => {
  const cwd = "/tmp/work space";
  assert.deepEqual(extractToolAccesses("read", { path: "/skills/alpha/SKILL.md" }, cwd), {
    accesses: [{ path: "/skills/alpha/SKILL.md", cwd, kind: "read" }],
    ambiguous: false,
    supported: true,
  });

  const shell = extractToolAccesses("exec", {
    command: `sed -n '1,120p' "skills/alpha skill/SKILL.md" && cat "skills/alpha skill/references/guide.md"`,
    workdir: cwd,
  });
  assert.equal(shell.ambiguous, false);
  assert.deepEqual(shell.accesses, [
    { path: "skills/alpha skill/SKILL.md", cwd, kind: "read" },
    { path: "skills/alpha skill/references/guide.md", cwd, kind: "read" },
  ]);

  const grep = extractToolAccesses("exec", {
    command: `rg -n heading "skills/alpha skill/SKILL.md"`,
    workdir: cwd,
  });
  assert.deepEqual(grep.accesses, [{ path: "skills/alpha skill/SKILL.md", cwd, kind: "read" }]);

  const unrelated = extractToolAccesses("exec", {
    command: `printf '%s' "/skills/alpha/SKILL.md"`,
    workdir: cwd,
  });
  assert.deepEqual(unrelated.accesses, []);
  assert.equal(unrelated.ambiguous, true);

  for (const command of [
    `cat unrelated.txt > "skills/alpha skill/SKILL.md"`,
    `cat unrelated.txt &>> "skills/alpha skill/SKILL.md"`,
    `head -n 2 unrelated.txt > "skills/alpha skill/SKILL.md"`,
    `sed -n '1p' unrelated.txt > "skills/alpha skill/SKILL.md"`,
  ]) {
    const redirected = extractToolAccesses("exec", { command, workdir: cwd });
    assert.deepEqual(redirected.accesses, [{ path: "unrelated.txt", cwd, kind: "read" }]);
    assert.equal(redirected.ambiguous, false);
  }

  const redirectedInput = extractToolAccesses("exec", {
    command: `cat < "skills/alpha skill/SKILL.md" > output.txt`,
    workdir: cwd,
  });
  assert.deepEqual(redirectedInput.accesses, [
    { path: "skills/alpha skill/SKILL.md", cwd, kind: "read" },
  ]);

  for (const command of [
    `cat <<< "/skills/alpha/SKILL.md"`,
    `cat <<EOF\n/skills/alpha/SKILL.md\nEOF`,
  ]) {
    const inlineInput = extractToolAccesses("exec", { command, workdir: cwd });
    assert.deepEqual(inlineInput.accesses, []);
    assert.equal(inlineInput.ambiguous, true);
  }
});

test("classifyAccessForSkill resolves spaces, descendants, and symlink aliases", async () => {
  const root = await mkdtemp(join(tmpdir(), "nudge-paths-"));
  const realSkillDir = resolve(root, "real skills", "alpha");
  const aliasDir = resolve(root, "alias-alpha");
  const skillPath = resolve(realSkillDir, "SKILL.md");
  const resourcePath = resolve(realSkillDir, "references", "guide.md");
  await mkdir(resolve(realSkillDir, "references"), { recursive: true });
  await writeFile(skillPath, "# Alpha\n");
  await writeFile(resourcePath, "guide\n");
  await symlink(realSkillDir, aliasDir);

  const identity = await resolvePathIdentity(skillPath, root);
  assert.equal(await classifyAccessForSkill(resolve(aliasDir, "SKILL.md"), skillPath, identity.realPath, root), "skill");
  assert.equal(await classifyAccessForSkill(resolve(aliasDir, "references", "guide.md"), skillPath, identity.realPath, root), "resource");
  assert.equal(await classifyAccessForSkill(resolve(root, "other", "SKILL.md"), skillPath, identity.realPath, root), undefined);
});

test("tool failures and event keys are deterministic", () => {
  assert.equal(isToolCallFailure({ error: "not found" }), true);
  assert.equal(isToolCallFailure({ result: { exit_code: 1 } }), true);
  assert.equal(isToolCallFailure({ result: { success: true } }), false);

  const params = {
    nudgeId: "nudge-1",
    toolCallId: "tool-1",
    toolName: "exec_command",
    eventType: "opened",
    accessPath: "/skills/alpha/SKILL.md",
  };
  assert.equal(stableToolEventKey(params), stableToolEventKey(params));
  assert.equal(stableToolEventKey({ ...params, toolCallId: undefined }), undefined);
});
