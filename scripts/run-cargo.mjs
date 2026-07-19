import { existsSync } from "node:fs";
import { homedir } from "node:os";
import { delimiter, join } from "node:path";
import { spawnSync } from "node:child_process";

const executableName = process.platform === "win32" ? "cargo.exe" : "cargo";
const pathCandidates = (process.env.PATH || "")
  .split(delimiter)
  .filter(Boolean)
  .map((directory) => join(directory, executableName));
const cargo = [
  process.env.CARGO,
  ...pathCandidates,
  join(homedir(), ".cargo", "bin", executableName),
].find((candidate) => candidate && existsSync(candidate));

if (!cargo) {
  console.error("Cargo was not found. Install Rust from https://rustup.rs before building Golazo.");
  process.exit(1);
}

const result = spawnSync(cargo, process.argv.slice(2), { stdio: "inherit" });
process.exit(result.status ?? 1);
