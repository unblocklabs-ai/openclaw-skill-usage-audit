import test from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

test("entrypoint smoke gives OPENCLAW_CHECKOUT precedence over node_modules", async () => {
  const tempRoot = await mkdtemp(join(tmpdir(), "openclaw-checkout-priority-"));
  try {
    const checkout = resolve(tempRoot, "openclaw");
    const entryDir = resolve(checkout, "dist", "plugin-sdk");
    const sentinel = resolve(tempRoot, "selected.txt");
    await mkdir(entryDir, { recursive: true });
    await writeFile(resolve(checkout, "package.json"), JSON.stringify({
      name: "openclaw",
      type: "module",
      exports: { "./plugin-sdk/plugin-entry": "./dist/plugin-sdk/plugin-entry.js" },
    }));
    await writeFile(resolve(entryDir, "plugin-entry.js"), `
      import { writeFileSync } from "node:fs";
      if (process.env.OPENCLAW_SELECTION_SENTINEL) {
        writeFileSync(process.env.OPENCLAW_SELECTION_SENTINEL, "override");
      }
      export function definePluginEntry(entry) { return entry; }
    `);

    const { stdout } = await execFileAsync(process.execPath, [
      "scripts/smoke-openclaw-entrypoint.mjs",
    ], {
      cwd: resolve(new URL("..", import.meta.url).pathname),
      env: {
        ...process.env,
        OPENCLAW_CHECKOUT: checkout,
        OPENCLAW_SELECTION_SENTINEL: sentinel,
      },
    });

    assert.match(stdout, /entrypoint smoke ok/);
    assert.equal(await readFile(sentinel, "utf8"), "override");
  } finally {
    await rm(tempRoot, { recursive: true, force: true });
  }
});
