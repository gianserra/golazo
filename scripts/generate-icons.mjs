import { spawnSync } from "node:child_process";
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { deflateSync } from "node:zlib";

const root = resolve(".");
const assetDir = resolve(root, "electron", "assets");
const iconsetDir = resolve(assetDir, "icon.iconset");

function crc32(buffer) {
  let crc = 0xffffffff;
  for (const byte of buffer) {
    crc ^= byte;
    for (let bit = 0; bit < 8; bit += 1) crc = (crc >>> 1) ^ (0xedb88320 & -(crc & 1));
  }
  return (crc ^ 0xffffffff) >>> 0;
}

function chunk(type, data) {
  const typeBuffer = Buffer.from(type);
  const length = Buffer.alloc(4);
  length.writeUInt32BE(data.length);
  const checksum = Buffer.alloc(4);
  checksum.writeUInt32BE(crc32(Buffer.concat([typeBuffer, data])));
  return Buffer.concat([length, typeBuffer, data, checksum]);
}

function png(width, height, pixels) {
  const raw = Buffer.alloc((width * 4 + 1) * height);
  for (let y = 0; y < height; y += 1) {
    raw[y * (width * 4 + 1)] = 0;
    pixels.copy(raw, y * (width * 4 + 1) + 1, y * width * 4, (y + 1) * width * 4);
  }
  const header = Buffer.alloc(13);
  header.writeUInt32BE(width, 0);
  header.writeUInt32BE(height, 4);
  header[8] = 8;
  header[9] = 6;
  return Buffer.concat([
    Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
    chunk("IHDR", header),
    chunk("IDAT", deflateSync(raw)),
    chunk("IEND", Buffer.alloc(0)),
  ]);
}

function blend(pixels, width, x, y, color, alpha = 1) {
  if (x < 0 || y < 0 || x >= width || y >= width || alpha <= 0) return;
  const index = (y * width + x) * 4;
  const sourceAlpha = Math.max(0, Math.min(1, alpha * color[3]));
  const targetAlpha = pixels[index + 3] / 255;
  const outAlpha = sourceAlpha + targetAlpha * (1 - sourceAlpha);
  if (outAlpha <= 0) return;
  pixels[index] = Math.round(((color[0] * sourceAlpha) + (pixels[index] * targetAlpha * (1 - sourceAlpha))) / outAlpha);
  pixels[index + 1] = Math.round(((color[1] * sourceAlpha) + (pixels[index + 1] * targetAlpha * (1 - sourceAlpha))) / outAlpha);
  pixels[index + 2] = Math.round(((color[2] * sourceAlpha) + (pixels[index + 2] * targetAlpha * (1 - sourceAlpha))) / outAlpha);
  pixels[index + 3] = Math.round(outAlpha * 255);
}

function drawDisc(pixels, width, centerX, centerY, radius, color) {
  const minX = Math.floor(centerX - radius - 1);
  const maxX = Math.ceil(centerX + radius + 1);
  const minY = Math.floor(centerY - radius - 1);
  const maxY = Math.ceil(centerY + radius + 1);
  for (let y = minY; y <= maxY; y += 1) {
    for (let x = minX; x <= maxX; x += 1) {
      const distance = Math.hypot(x + 0.5 - centerX, y + 0.5 - centerY);
      blend(pixels, width, x, y, color, Math.min(1, radius + 0.5 - distance));
    }
  }
}

function drawLine(pixels, width, x1, y1, x2, y2, thickness, color) {
  const radius = thickness / 2;
  const minX = Math.floor(Math.min(x1, x2) - radius - 1);
  const maxX = Math.ceil(Math.max(x1, x2) + radius + 1);
  const minY = Math.floor(Math.min(y1, y2) - radius - 1);
  const maxY = Math.ceil(Math.max(y1, y2) + radius + 1);
  const dx = x2 - x1;
  const dy = y2 - y1;
  const lengthSquared = dx * dx + dy * dy;
  for (let y = minY; y <= maxY; y += 1) {
    for (let x = minX; x <= maxX; x += 1) {
      const t = Math.max(0, Math.min(1, (((x + 0.5 - x1) * dx) + ((y + 0.5 - y1) * dy)) / lengthSquared));
      const px = x1 + t * dx;
      const py = y1 + t * dy;
      const distance = Math.hypot(x + 0.5 - px, y + 0.5 - py);
      blend(pixels, width, x, y, color, Math.min(1, radius + 0.5 - distance));
    }
  }
  drawDisc(pixels, width, x1, y1, radius, color);
  drawDisc(pixels, width, x2, y2, radius, color);
}

function drawRingArc(pixels, width, centerX, centerY, radius, thickness, startDeg, endDeg, color) {
  const min = Math.floor(Math.min(centerX, centerY) - radius - thickness);
  const max = Math.ceil(Math.max(centerX, centerY) + radius + thickness);
  const start = startDeg * Math.PI / 180;
  const end = endDeg * Math.PI / 180;
  for (let y = min; y <= max; y += 1) {
    for (let x = min; x <= max; x += 1) {
      const dx = x + 0.5 - centerX;
      const dy = y + 0.5 - centerY;
      let angle = Math.atan2(dy, dx);
      if (angle < 0) angle += Math.PI * 2;
      const inArc = start < end ? angle >= start && angle <= end : angle >= start || angle <= end;
      if (!inArc) continue;
      const distance = Math.abs(Math.hypot(dx, dy) - radius);
      blend(pixels, width, x, y, color, Math.min(1, thickness / 2 + 0.5 - distance));
    }
  }
}

function drawRoundedRect(pixels, width, x, y, size, radius, color) {
  for (let py = Math.floor(y); py < Math.ceil(y + size); py += 1) {
    for (let px = Math.floor(x); px < Math.ceil(x + size); px += 1) {
      const cx = Math.max(x + radius, Math.min(px + 0.5, x + size - radius));
      const cy = Math.max(y + radius, Math.min(py + 0.5, y + size - radius));
      const distance = Math.hypot(px + 0.5 - cx, py + 0.5 - cy);
      blend(pixels, width, px, py, color, Math.min(1, radius + 0.5 - distance));
    }
  }
}

function render(size) {
  const pixels = Buffer.alloc(size * size * 4);
  const scale = size / 1024;
  const s = (value) => value * scale;
  const dark = [17, 20, 17, 1];
  const panel = [29, 34, 29, 1];
  const line = [216, 245, 138, 1];
  const cream = [244, 242, 223, 1];
  const shadow = [5, 8, 5, 0.28];

  drawRoundedRect(pixels, size, s(56), s(56), s(912), s(206), dark);
  drawRoundedRect(pixels, size, s(92), s(92), s(840), s(176), panel);
  drawRoundedRect(pixels, size, s(92), s(92), s(840), s(176), [255, 255, 255, 0.035]);
  drawLine(pixels, size, s(343), s(685), s(680), s(685), s(120), shadow);
  drawRingArc(pixels, size, s(512), s(505), s(268), s(122), 35, 325, line);
  drawLine(pixels, size, s(540), s(505), s(754), s(505), s(112), line);
  drawLine(pixels, size, s(742), s(505), s(742), s(596), s(112), line);
  drawDisc(pixels, size, s(742), s(596), s(84), line);
  drawDisc(pixels, size, s(742), s(596), s(38), dark);
  drawDisc(pixels, size, s(742), s(596), s(18), cream);
  drawDisc(pixels, size, s(346), s(363), s(56), cream);
  return png(size, size, pixels);
}

function normalizePng(file) {
  const temporary = `${file}.normalized.png`;
  const result = spawnSync("sips", ["-s", "format", "png", file, "--out", temporary], { stdio: "ignore" });
  if (result.status === 0) {
    rmSync(file, { force: true });
    writeFileSync(file, Buffer.from(readFileSync(temporary)));
    rmSync(temporary, { force: true });
  }
  spawnSync("xattr", ["-d", "com.apple.provenance", file], { stdio: "ignore" });
}

mkdirSync(assetDir, { recursive: true });
rmSync(iconsetDir, { recursive: true, force: true });
mkdirSync(iconsetDir, { recursive: true });

writeFileSync(resolve(assetDir, "icon.png"), render(1024));
normalizePng(resolve(assetDir, "icon.png"));

for (const [name, size] of [
  ["icon_16x16.png", 16],
  ["icon_16x16@2x.png", 32],
  ["icon_32x32.png", 32],
  ["icon_32x32@2x.png", 64],
  ["icon_128x128.png", 128],
  ["icon_128x128@2x.png", 256],
  ["icon_256x256.png", 256],
  ["icon_256x256@2x.png", 512],
  ["icon_512x512.png", 512],
  ["icon_512x512@2x.png", 1024],
]) {
  const output = resolve(iconsetDir, name);
  const result = spawnSync("sips", ["--resampleWidth", String(size), resolve(assetDir, "icon.png"), "--out", output], { stdio: "ignore" });
  if (result.status !== 0) writeFileSync(output, render(size));
  normalizePng(output);
}

spawnSync("xattr", ["-d", "com.apple.provenance", iconsetDir], { stdio: "ignore" });
spawnSync("xattr", ["-d", "com.apple.provenance", assetDir], { stdio: "ignore" });

const iconutil = spawnSync("iconutil", ["-c", "icns", iconsetDir, "-o", resolve(assetDir, "icon.icns")], { stdio: "inherit" });
if (iconutil.status !== 0) console.warn("Generated icon.png, but macOS iconutil did not accept the temporary ICNS iconset.");
rmSync(iconsetDir, { recursive: true, force: true });

console.log(`Generated ${resolve(assetDir, "icon.png").replace(`${dirname(root)}/`, "")}`);
