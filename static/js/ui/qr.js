// Zero-dependency ISO/IEC 18004 QR Code encoder (Byte mode, EC Level M, Versions 1-10)
// Produces compact inline SVG elements without external CDN dependencies.

const SVG_NS = "http://www.w3.org/2000/svg";

// [version, dataCodewords, ecPerBlock, [[numBlocks, dataPerBlock], ...], alignmentCoords]
const VERSION_TABLE = [
  null,
  [1, 16, 10, [[1, 16]], []],
  [2, 28, 16, [[1, 28]], [6, 18]],
  [3, 44, 26, [[1, 44]], [6, 22]],
  [4, 64, 18, [[2, 32]], [6, 26]],
  [5, 86, 24, [[2, 43]], [6, 30]],
  [6, 108, 16, [[4, 27]], [6, 34]],
  [7, 124, 18, [[4, 31]], [6, 22, 38]],
  [8, 154, 22, [[2, 38], [2, 39]], [6, 24, 42]],
  [9, 182, 22, [[3, 36], [2, 37]], [6, 26, 46]],
  [10, 216, 26, [[4, 43], [1, 44]], [6, 28, 50]],
];

const GF_EXP = new Uint8Array(512);
const GF_LOG = new Uint8Array(256);
{
  let x = 1;
  for (let i = 0; i < 255; i += 1) {
    GF_EXP[i] = x;
    GF_LOG[x] = i;
    x <<= 1;
    if (x & 0x100) x ^= 0x11d;
  }
  for (let i = 255; i < 512; i += 1) {
    GF_EXP[i] = GF_EXP[i - 255];
  }
}

function gfMul(a, b) {
  if (a === 0 || b === 0) return 0;
  return GF_EXP[GF_LOG[a] + GF_LOG[b]];
}

function rsGeneratorPoly(degree) {
  let poly = [1];
  for (let i = 0; i < degree; i += 1) {
    const next = new Array(poly.length + 1).fill(0);
    const factor = GF_EXP[i];
    for (let j = 0; j < poly.length; j += 1) {
      next[j] ^= poly[j];
      next[j + 1] ^= gfMul(poly[j], factor);
    }
    poly = next;
  }
  return poly;
}

function rsRemainder(data, ecCount) {
  const gen = rsGeneratorPoly(ecCount);
  const rem = new Uint8Array(ecCount);
  for (let i = 0; i < data.length; i += 1) {
    const factor = data[i] ^ rem[0];
    rem.copyWithin(0, 1);
    rem[ecCount - 1] = 0;
    for (let j = 0; j < ecCount; j += 1) {
      rem[j] ^= gfMul(gen[j + 1], factor);
    }
  }
  return rem;
}

function utf8Bytes(text) {
  return new TextEncoder().encode(String(text || ""));
}

function pickVersion(byteLen) {
  for (let v = 1; v < VERSION_TABLE.length; v += 1) {
    const dataCw = VERSION_TABLE[v][1];
    const headerBits = 4 + (v <= 9 ? 8 : 16);
    const maxBytes = Math.floor((dataCw * 8 - headerBits) / 8);
    if (byteLen <= maxBytes) return v;
  }
  return -1;
}

function buildDataCodewords(bytes, version) {
  const dataCw = VERSION_TABLE[version][1];
  const bits = [];
  const pushBits = (val, len) => {
    for (let i = len - 1; i >= 0; i -= 1) {
      bits.push((val >>> i) & 1);
    }
  };

  // Byte mode indicator (0100)
  pushBits(0b0100, 4);
  pushBits(bytes.length, version <= 9 ? 8 : 16);
  for (let i = 0; i < bytes.length; i += 1) {
    pushBits(bytes[i], 8);
  }

  const maxBits = dataCw * 8;
  const termLen = Math.min(4, maxBits - bits.length);
  for (let i = 0; i < termLen; i += 1) bits.push(0);
  while (bits.length % 8 !== 0) bits.push(0);

  const codewords = new Uint8Array(dataCw);
  let idx = 0;
  for (let i = 0; i < bits.length; i += 8) {
    let b = 0;
    for (let j = 0; j < 8; j += 1) b = (b << 1) | bits[i + j];
    codewords[idx++] = b;
  }
  const padBytes = [0xec, 0x11];
  let padIdx = 0;
  while (idx < dataCw) {
    codewords[idx++] = padBytes[padIdx];
    padIdx ^= 1;
  }
  return codewords;
}

function interleaveBlocks(dataCw, version) {
  const ecPerBlock = VERSION_TABLE[version][2];
  const groups = VERSION_TABLE[version][3];
  const dataBlocks = [];
  const ecBlocks = [];
  let offset = 0;
  for (const [count, len] of groups) {
    for (let b = 0; b < count; b += 1) {
      const slice = dataCw.subarray(offset, offset + len);
      offset += len;
      dataBlocks.push(slice);
      ecBlocks.push(rsRemainder(slice, ecPerBlock));
    }
  }

  const out = [];
  const maxDataLen = Math.max(...dataBlocks.map((b) => b.length));
  for (let i = 0; i < maxDataLen; i += 1) {
    for (const block of dataBlocks) {
      if (i < block.length) out.push(block[i]);
    }
  }
  for (let i = 0; i < ecPerBlock; i += 1) {
    for (const block of ecBlocks) {
      out.push(block[i]);
    }
  }
  return out;
}

function bchFormatBits(mask) {
  // EC Level M = 00
  const data = (0b00 << 3) | (mask & 0b111);
  let rem = data << 10;
  for (let i = 14; i >= 10; i -= 1) {
    if ((rem >>> i) & 1) rem ^= 0x537 << (i - 10);
  }
  return ((data << 10) | rem) ^ 0x5412;
}

function bchVersionBits(version) {
  let rem = version << 12;
  for (let i = 17; i >= 12; i -= 1) {
    if ((rem >>> i) & 1) rem ^= 0x1f25 << (i - 12);
  }
  return (version << 12) | rem;
}

function createEmptyGrid(size) {
  return Array.from({ length: size }, () => new Uint8Array(size));
}

function placeFunctionPatterns(version) {
  const size = version * 4 + 17;
  const modules = createEmptyGrid(size);
  const isFunc = createEmptyGrid(size);

  const setFunc = (r, c, dark) => {
    if (r < 0 || c < 0 || r >= size || c >= size) return;
    modules[r][c] = dark ? 1 : 0;
    isFunc[r][c] = 1;
  };

  const drawFinder = (top, left) => {
    for (let dr = -1; dr <= 7; dr += 1) {
      for (let dc = -1; dc <= 7; dc += 1) {
        const r = top + dr;
        const c = left + dc;
        if (r < 0 || c < 0 || r >= size || c >= size) continue;
        const inOuter = dr >= 0 && dr <= 6 && dc >= 0 && dc <= 6;
        const dark =
          inOuter &&
          (dr === 0 ||
            dr === 6 ||
            dc === 0 ||
            dc === 6 ||
            (dr >= 2 && dr <= 4 && dc >= 2 && dc <= 4));
        setFunc(r, c, dark);
      }
    }
  };

  drawFinder(0, 0);
  drawFinder(0, size - 7);
  drawFinder(size - 7, 0);

  // Timing patterns
  for (let i = 8; i < size - 8; i += 1) {
    setFunc(6, i, i % 2 === 0);
    setFunc(i, 6, i % 2 === 0);
  }

  // Alignment patterns
  const coords = VERSION_TABLE[version][4];
  for (const r of coords) {
    for (const c of coords) {
      if (isFunc[r][c]) continue;
      for (let dr = -2; dr <= 2; dr += 1) {
        for (let dc = -2; dc <= 2; dc += 1) {
          const dist = Math.max(Math.abs(dr), Math.abs(dc));
          setFunc(r + dr, c + dc, dist !== 1);
        }
      }
    }
  }

  // Reserve format info areas and dark module
  for (let i = 0; i <= 8; i += 1) {
    if (!isFunc[8][i]) setFunc(8, i, false);
    if (!isFunc[i][8]) setFunc(i, 8, false);
  }
  for (let i = size - 8; i < size; i += 1) {
    if (!isFunc[8][i]) setFunc(8, i, false);
    if (!isFunc[i][8]) setFunc(i, 8, false);
  }
  setFunc(size - 8, 8, true);

  // Version info for v7+
  if (version >= 7) {
    const vBits = bchVersionBits(version);
    for (let i = 0; i < 18; i += 1) {
      const bit = Boolean((vBits >>> i) & 1);
      const a = size - 11 + (i % 3);
      const b = Math.floor(i / 3);
      setFunc(a, b, bit);
      setFunc(b, a, bit);
    }
  }

  return { size, modules, isFunc };
}

function writeFormatInfo(modules, size, mask) {
  const bits = bchFormatBits(mask);
  const bit = (idx) => (bits >>> idx) & 1;

  // Copy 1: around top-left finder (bits 14..0)
  for (let i = 0; i <= 5; i += 1) modules[8][i] = bit(14 - i);
  modules[8][7] = bit(8);
  modules[8][8] = bit(7);
  modules[7][8] = bit(6);
  for (let i = 0; i <= 5; i += 1) modules[5 - i][8] = bit(5 - i);

  // Copy 2: bottom-left col 8 (bits 14..8) and top-right row 8 (bits 7..0)
  for (let i = 0; i < 7; i += 1) modules[size - 1 - i][8] = bit(14 - i);
  for (let i = 0; i < 8; i += 1) modules[8][size - 8 + i] = bit(7 - i);
}

function maskBit(mask, r, c) {
  switch (mask) {
    case 0: return (r + c) % 2 === 0;
    case 1: return r % 2 === 0;
    case 2: return c % 3 === 0;
    case 3: return (r + c) % 3 === 0;
    case 4: return (Math.floor(r / 2) + Math.floor(c / 3)) % 2 === 0;
    case 5: return ((r * c) % 2) + ((r * c) % 3) === 0;
    case 6: return (((r * c) % 2) + ((r * c) % 3)) % 2 === 0;
    case 7: return (((r + c) % 2) + ((r * c) % 3)) % 2 === 0;
    default: return false;
  }
}

function placeDataAndMask(base, codewords, mask) {
  const { size, isFunc } = base;
  const grid = base.modules.map((row) => Uint8Array.from(row));
  let bitIdx = 0;
  const totalBits = codewords.length * 8;

  for (let right = size - 1; right >= 1; right -= 2) {
    if (right === 6) right = 5;
    const upward = ((right + 1) & 2) === 0;
    for (let vert = 0; vert < size; vert += 1) {
      const r = upward ? size - 1 - vert : vert;
      for (let j = 0; j < 2; j += 1) {
        const c = right - j;
        if (isFunc[r][c]) continue;
        let dark = false;
        if (bitIdx < totalBits) {
          dark = Boolean((codewords[bitIdx >>> 3] >>> (7 - (bitIdx & 7))) & 1);
          bitIdx += 1;
        }
        if (maskBit(mask, r, c)) dark = !dark;
        grid[r][c] = dark ? 1 : 0;
      }
    }
  }
  writeFormatInfo(grid, size, mask);
  return grid;
}

function penaltyScore(grid) {
  const n = grid.length;
  let penalty = 0;

  // Rule 1: adjacent modules in row/col with same color
  for (let r = 0; r < n; r += 1) {
    let runColor = grid[r][0];
    let runLen = 1;
    for (let c = 1; c < n; c += 1) {
      if (grid[r][c] === runColor) {
        runLen += 1;
      } else {
        if (runLen >= 5) penalty += 3 + (runLen - 5);
        runColor = grid[r][c];
        runLen = 1;
      }
    }
    if (runLen >= 5) penalty += 3 + (runLen - 5);
  }
  for (let c = 0; c < n; c += 1) {
    let runColor = grid[0][c];
    let runLen = 1;
    for (let r = 1; r < n; r += 1) {
      if (grid[r][c] === runColor) {
        runLen += 1;
      } else {
        if (runLen >= 5) penalty += 3 + (runLen - 5);
        runColor = grid[r][c];
        runLen = 1;
      }
    }
    if (runLen >= 5) penalty += 3 + (runLen - 5);
  }

  // Rule 2: 2x2 blocks of same color
  for (let r = 0; r < n - 1; r += 1) {
    for (let c = 0; c < n - 1; c += 1) {
      const v = grid[r][c];
      if (v === grid[r][c + 1] && v === grid[r + 1][c] && v === grid[r + 1][c + 1]) {
        penalty += 3;
      }
    }
  }

  // Rule 4: dark module balance
  let dark = 0;
  for (let r = 0; r < n; r += 1) {
    for (let c = 0; c < n; c += 1) dark += grid[r][c];
  }
  const pct = (dark * 100) / (n * n);
  penalty += Math.floor(Math.abs(pct - 50) / 5) * 10;
  return penalty;
}

export function encodeQrMatrix(text) {
  const bytes = utf8Bytes(text);
  if (!bytes.length) return null;
  const version = pickVersion(bytes.length);
  if (version < 0) return null;
  const dataCw = buildDataCodewords(bytes, version);
  const codewords = interleaveBlocks(dataCw, version);
  const base = placeFunctionPatterns(version);

  let bestGrid = null;
  let bestScore = Infinity;
  for (let mask = 0; mask < 8; mask += 1) {
    const candidate = placeDataAndMask(base, codewords, mask);
    const score = penaltyScore(candidate);
    if (score < bestScore) {
      bestScore = score;
      bestGrid = candidate;
    }
  }
  return bestGrid;
}

export function renderQrSvg(text, { quietZone = 4, label = "Scan QR code to join Watch Party" } = {}) {
  const matrix = encodeQrMatrix(text);
  if (!matrix) return null;
  const n = matrix.length;
  const total = n + quietZone * 2;

  const svg = document.createElementNS(SVG_NS, "svg");
  svg.setAttribute("viewBox", `0 0 ${total} ${total}`);
  svg.setAttribute("role", "img");
  svg.setAttribute("aria-label", label);
  svg.setAttribute("shape-rendering", "crispEdges");
  svg.classList.add("wp-host-qr-svg");

  const bg = document.createElementNS(SVG_NS, "rect");
  bg.setAttribute("x", "0");
  bg.setAttribute("y", "0");
  bg.setAttribute("width", String(total));
  bg.setAttribute("height", String(total));
  bg.setAttribute("fill", "#ffffff");
  svg.appendChild(bg);

  const parts = [];
  for (let r = 0; r < n; r += 1) {
    const row = matrix[r];
    let c = 0;
    while (c < n) {
      if (!row[c]) {
        c += 1;
        continue;
      }
      const start = c;
      while (c < n && row[c]) c += 1;
      const run = c - start;
      parts.push(`M${start + quietZone},${r + quietZone}h${run}v1h-${run}z`);
    }
  }

  const path = document.createElementNS(SVG_NS, "path");
  path.setAttribute("d", parts.join(""));
  path.setAttribute("fill", "#0d1117");
  svg.appendChild(path);

  return svg;
}
