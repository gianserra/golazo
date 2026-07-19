import { createHash } from "node:crypto";
import { constants, copyFileSync, cpSync, existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { spawn, spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const electronRoot = resolve(root, "node_modules", "electron");
const electronPackage = JSON.parse(readFileSync(resolve(electronRoot, "package.json"), "utf8"));
const appArguments = [root, ...process.argv.slice(2)];

function run(command, args) {
  const result = spawnSync(command, args, { stdio: "inherit" });
  if (result.status !== 0) throw new Error(`${command} failed with exit code ${result.status}`);
}

function patchPlist(plist, key, value) {
  run("/usr/bin/plutil", ["-replace", key, "-string", value, plist]);
}

function macExecutable() {
  const sourceBundle = resolve(electronRoot, "dist", "Electron.app");
  const icon = resolve(root, "electron", "assets", "icon.icns");
  if (!existsSync(icon)) throw new Error("Missing electron/assets/icon.icns. Run `node scripts/generate-icons.mjs` first.");

  const stage = resolve(root, "release", "dev-electron");
  const bundle = resolve(stage, "Golazo.app");
  const markerPath = resolve(stage, ".identity.json");
  const identity = JSON.stringify({
    schema: 1,
    electronVersion: electronPackage.version,
    iconHash: createHash("sha256").update(readFileSync(icon)).digest("hex"),
  });
  const currentIdentity = existsSync(markerPath) ? readFileSync(markerPath, "utf8") : "";

  if (!existsSync(bundle) || currentIdentity !== identity) {
    rmSync(stage, { recursive: true, force: true });
    mkdirSync(stage, { recursive: true });
    cpSync(sourceBundle, bundle, { recursive: true, verbatimSymlinks: true, mode: constants.COPYFILE_FICLONE });

    const plist = resolve(bundle, "Contents", "Info.plist");
    copyFileSync(icon, resolve(bundle, "Contents", "Resources", "Golazo.icns"));
    patchPlist(plist, "CFBundleDisplayName", "Golazo");
    patchPlist(plist, "CFBundleName", "Golazo");
    patchPlist(plist, "CFBundleIdentifier", "com.gianserra.golazo.dev");
    patchPlist(plist, "CFBundleIconFile", "Golazo.icns");
    run("/usr/bin/codesign", ["--force", "--sign", "-", "--timestamp=none", bundle]);
    writeFileSync(markerPath, identity);
  }

  return resolve(bundle, "Contents", "MacOS", "Electron");
}

const executable = process.platform === "darwin"
  ? macExecutable()
  : resolve(electronRoot, readFileSync(resolve(electronRoot, "path.txt"), "utf8").trim());
const child = spawn(executable, appArguments, { cwd: root, env: process.env, stdio: "inherit" });

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => child.kill(signal));
}
child.on("error", (error) => {
  console.error(error);
  process.exitCode = 1;
});
child.on("exit", (code, signal) => {
  process.exitCode = code ?? (signal ? 1 : 0);
});
