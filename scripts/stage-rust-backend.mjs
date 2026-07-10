import { copyFileSync, mkdirSync, chmodSync } from "node:fs";
import { join } from "node:path";

const filename = process.platform === "win32" ? "golazo-backend.exe" : "golazo-backend";
const source = join("target", "release", filename);
const destinationDirectory = join("dist", "backend");
const destination = join(destinationDirectory, filename);

mkdirSync(destinationDirectory, { recursive: true });
copyFileSync(source, destination);
if (process.platform !== "win32") chmodSync(destination, 0o755);
console.log(`Staged ${destination}`);
