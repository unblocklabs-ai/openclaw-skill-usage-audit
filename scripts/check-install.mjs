#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";

const ROOT = process.cwd();
const MARKETPLACE_SOURCE = "marketplace/skill-usage-audit";
const RUNTIME_FILES = [
  "index.ts",
  "skill-roots.mjs",
  "skill-router-helpers.mjs",
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

function assertFile(relativePath) {
  const absolutePath = path.join(ROOT, relativePath);
  if (!fs.existsSync(absolutePath) || !fs.statSync(absolutePath).isFile()) {
    fail(`Missing expected file: ${relativePath}`);
  }
}

const packageJson = readJson("package.json");
const pluginManifest = readJson("openclaw.plugin.json");
const marketplace = readJson(".claude-plugin/marketplace.json");

if (!Array.isArray(packageJson.openclaw?.extensions) || packageJson.openclaw.extensions.length === 0) {
  fail("package.json missing openclaw.extensions");
}

for (const extension of packageJson.openclaw.extensions) {
  if (typeof extension !== "string" || !extension.trim()) {
    fail("package.json openclaw.extensions contains an invalid entry");
  }
  assertFile(extension.replace(/^\.\//, ""));
}

for (const file of RUNTIME_FILES) assertFile(file);
for (const file of ASSET_FILES) assertFile(file);

if (!Array.isArray(packageJson.files)) {
  fail("package.json files must be an array");
}
for (const file of RUNTIME_FILES) {
  if (!packageJson.files.includes(file)) fail(`package.json files must include ${file}`);
}
for (const file of ASSET_FILES) {
  if (!packageJson.files.includes(file)) fail(`package.json files must include ${file}`);
}

if (pluginManifest.id !== "skill-usage-audit") {
  fail(`Unexpected openclaw.plugin.json id: ${pluginManifest.id}`);
}

if (pluginManifest.version !== packageJson.version) {
  fail(`Version mismatch: openclaw.plugin.json=${pluginManifest.version} package.json=${packageJson.version}`);
}

if (marketplace.version !== packageJson.version) {
  fail(`Version mismatch: marketplace=${marketplace.version} package.json=${packageJson.version}`);
}

const marketplacePlugin = Array.isArray(marketplace.plugins)
  ? marketplace.plugins.find((entry) => entry?.name === "skill-usage-audit")
  : undefined;
if (!marketplacePlugin) fail("Marketplace manifest missing skill-usage-audit plugin entry");
if (marketplacePlugin.version !== packageJson.version) {
  fail(`Version mismatch: marketplace plugin=${marketplacePlugin.version} package.json=${packageJson.version}`);
}
if (marketplacePlugin.source !== MARKETPLACE_SOURCE) {
  fail(`Unexpected marketplace source: ${marketplacePlugin.source}`);
}

const marketplacePackage = readJson(path.join(MARKETPLACE_SOURCE, "package.json"));
const marketplacePluginManifest = readJson(path.join(MARKETPLACE_SOURCE, "openclaw.plugin.json"));
if (marketplacePackage.version !== packageJson.version) {
  fail(`Version mismatch: marketplace package=${marketplacePackage.version} package.json=${packageJson.version}`);
}
if (marketplacePackage.scripts || marketplacePackage.devDependencies) {
  fail("Marketplace package must not include development scripts or devDependencies");
}
if (marketplacePluginManifest.version !== pluginManifest.version) {
  fail(
    `Version mismatch: marketplace openclaw.plugin.json=${marketplacePluginManifest.version} root openclaw.plugin.json=${pluginManifest.version}`,
  );
}
for (const file of RUNTIME_FILES) assertFile(path.join(MARKETPLACE_SOURCE, file));
for (const file of ASSET_FILES) assertFile(path.join(MARKETPLACE_SOURCE, file));

console.log("Install shape check passed.");
