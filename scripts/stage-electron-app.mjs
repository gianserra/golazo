import { cpSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";

const root = resolve(".");
const stage = resolve("release", "electron-app");
const sourcePackage = JSON.parse(readFileSync(resolve("package.json"), "utf8"));
const electronPackage = JSON.parse(readFileSync(resolve("node_modules", "electron", "package.json"), "utf8"));

rmSync(stage, { recursive: true, force: true });
mkdirSync(stage, { recursive: true });
cpSync(resolve("electron"), resolve(stage, "electron"), { recursive: true });

const build = {
  ...sourcePackage.build,
  electronDist: resolve("node_modules", "electron", "dist"),
  electronVersion: electronPackage.version,
  directories: { output: resolve("dist") },
  extraResources: [{ from: resolve("dist", "backend"), to: "backend" }],
};
const stagedPackage = {
  name: sourcePackage.name,
  version: sourcePackage.version,
  description: sourcePackage.description,
  author: sourcePackage.author,
  private: true,
  main: sourcePackage.main,
  build,
};

writeFileSync(resolve(stage, "package.json"), `${JSON.stringify(stagedPackage, null, 2)}\n`);
console.log(`Staged Electron application at ${stage.replace(`${root}/`, "")}`);
