import { copyFileSync, mkdirSync, chmodSync } from "node:fs";
import { join } from "node:path";

const destinationDirectory = join("dist", "backend");
mkdirSync(destinationDirectory, { recursive: true });

for (const name of ["golazo-backend", "golazo-tracker"]) {
  const filename = process.platform === "win32" ? `${name}.exe` : name;
  const source = join("target", "release", filename);
  const destination = join(destinationDirectory, filename);
  copyFileSync(source, destination);
  if (process.platform !== "win32") chmodSync(destination, 0o755);
  console.log(`Staged ${destination}`);
}
