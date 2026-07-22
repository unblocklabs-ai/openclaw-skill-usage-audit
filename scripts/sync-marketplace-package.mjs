#!/usr/bin/env node

import fs from "node:fs";
import os from "node:os";
import path from "node:path";

const ROOT = process.cwd();
const MARKETPLACE_PACKAGE_DIR = path.join(ROOT, "marketplace", "skill-usage-audit");
const CHECK_MODE = process.argv.includes("--check");
const RUNTIME_FILES = [
  "dist/index.js",
  "dist/skill-roots.mjs",
  "dist/skill-router-helpers.mjs",
  "dist/nudge-tracking.mjs",
  "evaluate-skill-health.mjs",
  "evaluate-nudge-health.mjs",
];
const ASSET_FILES = [
  "docs/skill-usage-audit-banner.webp",
];

function fail(message) {
  console.error(message);
  process.exit(1);
}

function readJson(relativePath) {
  return JSON.parse(fs.readFileSync(path.join(ROOT, relativePath), "utf8"));
}

function writeJson(filePath, value) {
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

function assertPathExists(filePath, label) {
  if (!fs.existsSync(filePath)) fail(`Missing ${label}: ${path.relative(ROOT, filePath)}`);
}

function buildMarketplacePackageJson() {
  const packageJson = readJson("package.json");
  const allowedFields = [
    "name",
    "version",
    "description",
    "type",
    "repository",
    "homepage",
    "bugs",
    "keywords",
    "license",
    "author",
    "engines",
    "main",
    "exports",
    "dependencies",
    "optionalDependencies",
    "openclaw",
  ];
  const output = {};
  for (const field of allowedFields) {
    if (packageJson[field] !== undefined) output[field] = packageJson[field];
  }
  output.files = [
    "dist",
    "evaluate-skill-health.mjs",
    "evaluate-nudge-health.mjs",
    ...ASSET_FILES,
    "README.md",
    "openclaw.plugin.json",
  ];
  return output;
}

function syncInto(targetDir) {
  assertPathExists(path.join(ROOT, "README.md"), "README.md");
  assertPathExists(path.join(ROOT, "openclaw.plugin.json"), "openclaw.plugin.json");
  for (const file of RUNTIME_FILES) assertPathExists(path.join(ROOT, file), file);
  for (const file of ASSET_FILES) assertPathExists(path.join(ROOT, file), file);

  fs.rmSync(targetDir, { recursive: true, force: true });
  fs.mkdirSync(targetDir, { recursive: true });

  writeJson(path.join(targetDir, "package.json"), buildMarketplacePackageJson());
  fs.copyFileSync(path.join(ROOT, "README.md"), path.join(targetDir, "README.md"));
  fs.copyFileSync(path.join(ROOT, "openclaw.plugin.json"), path.join(targetDir, "openclaw.plugin.json"));
  for (const file of RUNTIME_FILES) {
    const targetPath = path.join(targetDir, file);
    fs.mkdirSync(path.dirname(targetPath), { recursive: true });
    fs.copyFileSync(path.join(ROOT, file), targetPath);
  }
  for (const file of ASSET_FILES) {
    const targetPath = path.join(targetDir, file);
    fs.mkdirSync(path.dirname(targetPath), { recursive: true });
    fs.copyFileSync(path.join(ROOT, file), targetPath);
  }
}

function walkFiles(rootDir) {
  if (!fs.existsSync(rootDir)) return [];
  const out = [];
  const stack = [rootDir];
  while (stack.length > 0) {
    const current = stack.pop();
    for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
      const fullPath = path.join(current, entry.name);
      if (entry.isDirectory()) stack.push(fullPath);
      else if (entry.isFile()) out.push(path.relative(rootDir, fullPath).split(path.sep).join("/"));
    }
  }
  return out.sort();
}

function assertDirectoriesMatch(actualDir, expectedDir) {
  const actualFiles = walkFiles(actualDir);
  const expectedFiles = walkFiles(expectedDir);
  const actualSet = new Set(actualFiles);
  const expectedSet = new Set(expectedFiles);

  const missing = expectedFiles.filter((file) => !actualSet.has(file));
  const extra = actualFiles.filter((file) => !expectedSet.has(file));
  if (missing.length > 0 || extra.length > 0) {
    fail(
      [
        "Marketplace package is out of sync.",
        missing.length > 0 ? `Missing: ${missing.join(", ")}` : "",
        extra.length > 0 ? `Extra: ${extra.join(", ")}` : "",
        "Run npm run marketplace:sync.",
      ].filter(Boolean).join("\n"),
    );
  }

  for (const relativePath of expectedFiles) {
    const actual = fs.readFileSync(path.join(actualDir, relativePath));
    const expected = fs.readFileSync(path.join(expectedDir, relativePath));
    if (!actual.equals(expected)) {
      fail(`Marketplace package is out of sync at ${relativePath}. Run npm run marketplace:sync.`);
    }
  }
}

if (CHECK_MODE) {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "skill-usage-audit-marketplace-"));
  try {
    const expectedDir = path.join(tempDir, "skill-usage-audit");
    syncInto(expectedDir);
    assertDirectoriesMatch(MARKETPLACE_PACKAGE_DIR, expectedDir);
    console.log("Marketplace package is in sync.");
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
} else {
  syncInto(MARKETPLACE_PACKAGE_DIR);
  console.log("Marketplace package synced.");
}
