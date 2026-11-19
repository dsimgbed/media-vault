// Zero-dependency FIPS 180-4 SHA-256 implementation with streaming support
// Works identically on plain HTTP (no crypto.subtle) and HTTPS origins.

const H_INIT = new Uint32Array([
  0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
  0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
]);

const K = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
  0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
  0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
]);

const HEX_CHARS = "0123456789abcdef";

function toUint8Array(input) {
  if (input instanceof Uint8Array) return input;
  if (input instanceof ArrayBuffer) return new Uint8Array(input);
  if (ArrayBuffer.isView(input)) {
    return new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
  }
  throw new TypeError("Expected Uint8Array or ArrayBuffer");
}

function bytesToHex(bytes) {
  let out = "";
  for (let i = 0; i < bytes.length; i++) {
    const b = bytes[i];
    out += HEX_CHARS[(b >>> 4) & 0x0f] + HEX_CHARS[b & 0x0f];
  }
  return out;
}

function writeUint32BE(buf, offset, val) {
  buf[offset] = (val >>> 24) & 0xff;
  buf[offset + 1] = (val >>> 16) & 0xff;
  buf[offset + 2] = (val >>> 8) & 0xff;
  buf[offset + 3] = val & 0xff;
}

export class IncrementalSha256 {
  constructor() {
    this._h = new Uint32Array(H_INIT);
    this._w = new Uint32Array(64);
    this._buf = new Uint8Array(64);
    this._bufLen = 0;
    this._byteCount = 0n;
  }

  _processBlock(block, offset) {
    const w = this._w;
    for (let i = 0; i < 16; i++) {
      const p = offset + i * 4;
      w[i] =
        ((block[p] << 24) |
          (block[p + 1] << 16) |
          (block[p + 2] << 8) |
          block[p + 3]) >>>
        0;
    }

    for (let i = 16; i < 64; i++) {
      const w15 = w[i - 15];
      const w2 = w[i - 2];
      const s0 = ((w15 >>> 7) | (w15 << 25)) ^ ((w15 >>> 18) | (w15 << 14)) ^ (w15 >>> 3);
      const s1 = ((w2 >>> 17) | (w2 << 15)) ^ ((w2 >>> 19) | (w2 << 13)) ^ (w2 >>> 10);
      w[i] = (w[i - 16] + s0 + w[i - 7] + s1) | 0;
    }

    const h = this._h;
    let a = h[0];
    let b = h[1];
    let c = h[2];
    let d = h[3];
    let e = h[4];
    let f = h[5];
    let g = h[6];
    let hh = h[7];

    for (let i = 0; i < 64; i++) {
      const S1 = ((e >>> 6) | (e << 26)) ^ ((e >>> 11) | (e << 21)) ^ ((e >>> 25) | (e << 7));
      const ch = (e & f) ^ (~e & g);
      const temp1 = (hh + S1 + ch + K[i] + w[i]) | 0;
      const S0 = ((a >>> 2) | (a << 30)) ^ ((a >>> 13) | (a << 19)) ^ ((a >>> 22) | (a << 10));
      const maj = (a & b) ^ (a & c) ^ (b & c);
      const temp2 = (S0 + maj) | 0;

      hh = g;
      g = f;
      f = e;
      e = (d + temp1) | 0;
      d = c;
      c = b;
      b = a;
      a = (temp1 + temp2) | 0;
    }

    h[0] = (h[0] + a) | 0;
    h[1] = (h[1] + b) | 0;
    h[2] = (h[2] + c) | 0;
    h[3] = (h[3] + d) | 0;
    h[4] = (h[4] + e) | 0;
    h[5] = (h[5] + f) | 0;
    h[6] = (h[6] + g) | 0;
    h[7] = (h[7] + hh) | 0;
  }

  update(chunk) {
    const data = toUint8Array(chunk);
    const len = data.length;
    if (len === 0) return this;

    this._byteCount += BigInt(len);
    let pos = 0;

    if (this._bufLen > 0) {
      const needed = 64 - this._bufLen;
      if (len < needed) {
        this._buf.set(data, this._bufLen);
        this._bufLen += len;
        return this;
      }
      this._buf.set(data.subarray(0, needed), this._bufLen);
      this._processBlock(this._buf, 0);
      this._bufLen = 0;
      pos = needed;
    }

    while (pos + 64 <= len) {
      this._processBlock(data, pos);
      pos += 64;
    }

    if (pos < len) {
      this._buf.set(data.subarray(pos), 0);
      this._bufLen = len - pos;
    }

    return this;
  }

  digestBytes() {
    const hBackup = new Uint32Array(this._h);
    try {
      const rem = this._bufLen;
      const padLen = rem < 56 ? 64 : 128;
      const pad = new Uint8Array(padLen);
      if (rem > 0) {
        pad.set(this._buf.subarray(0, rem), 0);
      }
      pad[rem] = 0x80;

      const bitLen = this._byteCount * 8n;
      const hi = Number((bitLen >> 32n) & 0xffffffffn);
      const lo = Number(bitLen & 0xffffffffn);
      writeUint32BE(pad, padLen - 8, hi);
      writeUint32BE(pad, padLen - 4, lo);

      this._processBlock(pad, 0);
      if (padLen === 128) {
        this._processBlock(pad, 64);
      }

      const out = new Uint8Array(32);
      for (let i = 0; i < 8; i++) {
        writeUint32BE(out, i * 4, this._h[i]);
      }
      return out;
    } finally {
      this._h.set(hBackup);
    }
  }

  hex() {
    return bytesToHex(this.digestBytes());
  }
}

export async function sha256Bytes(data) {
  const bytes = toUint8Array(data);
  const subtle = globalThis.crypto?.subtle;
  if (subtle && typeof subtle.digest === "function") {
    const digest = await subtle.digest("SHA-256", bytes);
    return bytesToHex(new Uint8Array(digest));
  }
  return new IncrementalSha256().update(bytes).hex();
}

function throwIfAborted(signal) {
  if (signal?.aborted) {
    throw new DOMException("Aborted", "AbortError");
  }
}

export async function hashFileStreaming(
  file,
  { chunkSize = 2 * 1024 * 1024, onProgress = null, signal = null } = {}
) {
  throwIfAborted(signal);

  const subtle = globalThis.crypto?.subtle;
  if (file.size <= 32 * 1024 * 1024 && subtle && typeof subtle.digest === "function") {
    const buf = await file.arrayBuffer();
    throwIfAborted(signal);
    const digest = await subtle.digest("SHA-256", buf);
    throwIfAborted(signal);
    onProgress?.({ loaded: file.size, total: file.size, pct: 100 });
    return bytesToHex(new Uint8Array(digest));
  }

  const hasher = new IncrementalSha256();
  if (file.size === 0) {
    onProgress?.({ loaded: 0, total: 0, pct: 100 });
    return hasher.hex();
  }

  for (let offset = 0; offset < file.size; offset += chunkSize) {
    throwIfAborted(signal);
    const nextOffset = Math.min(file.size, offset + chunkSize);
    const buf = await file.slice(offset, nextOffset).arrayBuffer();
    throwIfAborted(signal);
    hasher.update(new Uint8Array(buf));
    const pct = Math.min(100, Math.round((nextOffset / file.size) * 100));
    onProgress?.({ loaded: nextOffset, total: file.size, pct });
    await new Promise((r) => setTimeout(r, 0));
  }

  throwIfAborted(signal);
  return hasher.hex();
}
