// ============================================================================================================
// CANDLE STORAGE — an APPEND LOG beside each candle file.
//
// THE PROBLEM THIS SOLVES. updateInstrument/updateDuka rewrote the ENTIRE trailing-year file to append 1-3 new
// bars, because there was no append format. Measured on the live data dir: the 134 crypto candle files total
// 3.33 GB (lunc.json 37.1 MB, pepe.json 36.8 MB), the crypto lane runs every ~2.7 min, so that is ~74 GB of SSD
// writes per hour and ~1.78 TB per day -- a 1 TB consumer SSD's entire 600 TBW endurance budget in about a year
// of leaving the app open, and roughly four months on a budget QLC drive.
//
// THE DESIGN, and why this shape and not a rewrite of the format.
//   <fk>.json    the BASE. Unchanged format: {sym, interval, candles:[[t,o,h,l,c,v],...]}. Still a complete,
//                valid file at every instant, so anything that reads it today keeps working -- it simply does
//                not see bars newer than the last fold-in.
//   <fk>.ndjson  the LOG. One bar per line, appended. A minute's worth is a few hundred bytes.
// Reading = base + log, merged by timestamp. Appending = fs.appendFileSync of the new lines. Once a day the log
// is folded into the base and truncated, so a full rewrite happens ~once per instrument per day instead of ~22
// times an hour: ~1.78 TB/day becomes ~3.4 GB/day.
//
// WHY NOT convert to a binary or sharded format: it would mean rewriting all 368 existing files (6.566 GB) as a
// migration step, and an interrupted migration is exactly the data-loss event that must not happen. This design
// touches no existing byte -- the log simply starts empty -- so there is nothing to migrate and nothing to lose.
//
// CRASH SAFETY at every interruption point:
//   * mid-append  -> only the final line can be torn; readLog drops a trailing line it cannot parse, and that bar
//                    is re-fetched next run because lastT comes from the last VALID bar.
//   * mid-compact -> writeAtomic is tmp+rename, so the base is either the old one or the new one, never partial.
//   * between the compact write and the log truncate -> the log still holds bars already in the base, and the
//                    merge dedups by timestamp, so a repeat is a no-op. Compaction is idempotent.
// Nothing here deletes or shortens a candle file except the deliberate trailing-year trim inside compaction,
// which is the same trim the old write did on every single run.
// ============================================================================================================
const fs = require("fs");
const path = require("path");

const LOG_EXT = ".ndjson";
// The derived binary sidecar beside each base file -- see the block above readBin for why it exists and why it is
// safe. BIN_VER is the invalidation lever: bump it and every sidecar is ignored and rewritten at the next
// compaction, with no migration step, because the base files are the only source of truth.
const BIN_EXT = ".bin";
const BIN_MAGIC = 0x444e4143;   // "CAND" little-endian
const BIN_VER = 2;              // 2 = 40-byte header carrying a body checksum (see binSum). 1 had no integrity check.
const BIN_COLS = 6;             // the MINIMUM arity: [t,o,h,l,c,v]. The header's cols field carries the real width,
const BIN_COLS_MAX = 32;        // so a wider bar is stored in full rather than truncated -- see writeBin. Ceiling is
                                // a sanity bound so a corrupt header cannot ask for an absurd allocation.
const BIN_HDR = 40;             // magic u32, ver u32, base mtimeMs f64, base size f64, bars u32, cols u32, sum u32, pad u32
                                // 40 not 36: the body is float64s, so the header must stay a multiple of 8.

// CHECKSUM OF THE BODY, and it is not belt-and-braces -- it is the difference between a derived cache and a way to
// destroy the source. compact() reads through read() (sidecar included) and writes the result straight back as the
// new base, so ANY corruption inside a sidecar whose length is still right gets laundered permanently into the base
// file. Both halves of that were demonstrated: one flipped float64 made a bar's open 999999 and the next compaction
// wrote 999999 into the base; a NaN written into a bar made okBar drop it, so read() returned 499 of 500 bars and the
// compaction left the base permanently one bar SHORT. Base mtime and size are untouched in both cases, so nothing
// could ever notice. The length check only ever caught damage that changed the length.
// n and cols are folded in as the seed, so a RELABELLED header field fails too -- otherwise {n=7k,cols=6} and
// {n=6k,cols=7} describe byte-identical files and would validate each other. This is a checksum, not a MAC: it is
// aimed at accidental damage (miss rate 2^-32), and anyone who edits the body and recomputes the sum, or who cancels
// one word's change against the next, defeats it by construction. That is not the threat here.
// FNV-1a over 32-bit words. Measured on a real 25.2 MB sidecar across two independent runs: 5.9-7.8 ms, taking
// readBin to 25-34 ms against 175-203 ms for the JSON.parse it replaces -- 5.9x to 7.3x depending on the asset
// (btc 5.9, doge 6.2, shib 6.4, eth 6.9, sol 7.3), which is also why the figure quoted further up is 6.57x, not 9x.
// Ranges, not single figures, because repeat runs on this machine vary ~5-10% and stating one number as exact is how
// three of these comments came to be wrong.
function binSum(buf, off, bytes, n, cols){
  let h = Math.imul(2166136261 ^ n, 16777619);
  h = Math.imul(h ^ cols, 16777619);
  if ((buf.byteOffset + off) % 4 === 0){
    const w = new Uint32Array(buf.buffer, buf.byteOffset + off, bytes >>> 2);
    for (let i = 0; i < w.length; i++) h = Math.imul(h ^ w[i], 16777619);
  } else {
    const end = off + bytes;                                          // unaligned: no view possible, read it out
    for (let i = off; i < end; i += 4) h = Math.imul(h ^ buf.readUInt32LE(i), 16777619);
  }
  return h >>> 0;
}
// Fold the log into the base once it holds a day of bars. Derived, not picked: 1440 minutes is a day, and a day
// is how often the trailing-year trim needs to run anyway.
const COMPACT_AFTER_BARS = 1440;

// tmp+rename with a retry on the transient Windows locks (Defender / Search indexer / a reader holding the
// destination). Deliberately a private copy rather than an import, so this module stands alone and both the
// engine and the main process can use it without a dependency cycle.
function sleepSync(ms){ try{ Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,ms); }catch(e){} }
function writeAtomic(file, data){
  const tmp = file + "." + process.pid + ".tmp";
  fs.writeFileSync(tmp, data);
  // Clean up our own tmp when the rename cannot be made to work. Without this a persistently locked destination
  // leaves the full payload behind forever, and with sidecars in the picture that is a ~25.2 MB orphan per pid per
  // instrument sitting in the data dir with nothing to attribute it to. The tmp is ours alone (pid in the name),
  // so removing it on the way out cannot touch anyone else's file, and the throw still reaches the caller.
  for(let i=0;;i++){ try{ fs.renameSync(tmp,file); return; }
    catch(e){ if(i>=5 || !["EPERM","EACCES","EBUSY"].includes(e.code)){ try{ fs.unlinkSync(tmp); }catch(e2){} throw e; }
              sleepSync(120*(i+1)); } }
}

const basePath = (dir, fk) => path.join(dir, fk + ".json");
const logPath  = (dir, fk) => path.join(dir, fk + LOG_EXT);
const binPath  = (dir, fk) => path.join(dir, fk + BIN_EXT);

// ------------------------------------------------------------------------------------------------------------
// THE BINARY SIDECAR. Reading a base file costs ~234 ms, and the JSON.parse alone is most of it: disk 39 ms, parse
// 181 ms, okBar filter 13 ms. (An earlier version of this line said 236 = 47 + 198 + 15, which sums to 260 -- the
// parts were from one sample and the total from another.) 134 x 234 ms is 31 s serial, ~12 s pooled at the measured
// 2.7x -- an earlier line said 26.6 s / 9.8 s, which is the size-weighted total (mean crypto base 24.9 MB, ~188 ms)
// without saying so, and did not follow from the 234 ms beside it. On every
// single 60 s tick, spent re-parsing bytes that did not change. The sidecar is the same bars as packed float64:
// measured 193 ms -> 29 ms on btc, 6.57x (see the per-asset spread at binSum: 5.9x-7.3x), and EXACT on all 2.1 M bars tested (every bar is arity 6, finite, and survives
// a float64 round trip -- verified on 2.63 M real bars before this was written).
//
// It is a DERIVED CACHE, never a format change, and that distinction is the whole safety argument -- the header of
// this file rejected a binary format because migrating 6.566 GB of existing files risks exactly the data-loss event
// that must not happen. Nothing here migrates anything: the base file is never touched or read differently, the
// sidecar simply appears beside it, and if it is missing, stale, torn, or from another version the read falls
// straight back to JSON.parse. Deleting every .bin in the data dir costs nothing but speed.
//
// WHEN IT IS WRITTEN: only from writeBase, i.e. only where the base is being rewritten anyway (compaction, once a
// day, and first download). Deliberately NOT lazily on read: a read-path write that fails would retry on every
// tick, and at 25.2 MB x 134 instruments that failure mode is far worse than the cost it was saving. The price of
// this choice is that an instrument gets no speed-up until its next compaction -- one day for crypto at a bar a
// minute, but ~4 trading days for stocks (~390 bars/day against COMPACT_AFTER_BARS), and never for a stalled feed,
// which costs nothing because an unchanged stamp() means recompute never reads those at all.
//
// WHAT IT COSTS, stated here because the top of this file argues SSD endurance and this changes that number.
// Measured across the live data dir (368 candle bases, 6.566 GB, 121,308,396 bars): sidecars add +5.823 GB on disk,
// which is +88.7% overall -- +95.4% for the 127 files holding a full year of bars, +81.7% for the other 241. A 525,601-bar
// sidecar is 25.2 MB; bases range 0.29-37.1 MB, so 67 of the 368 sidecars are actually LARGER than the base they sit
// beside (a base with few bars still carries its JSON overhead, while the sidecar is a flat 48 bytes a bar).
// AND WHAT THE SAFETY COSTS, measured rather than assumed: verifying the base by bytes (baseIs, twice per write)
// takes writeBase on a 525,601-bar file from 269-284 ms to 386-408 ms -- +117 to +124 ms per compaction, ~15 s/day
// across the fleet. Those endpoints are with and without baseIs; writing no sidecar AT ALL is 227-234 ms, a different
// comparison, and an earlier version of this comment mixed the two (234 -> 419, labelled +114, which is neither
// difference). It also
// raises peak RSS from 220 MB to 310 MB, because two 31 MB strings and their source buffers are alive at once on top
// of the 25.2 MB sidecar buffer. That +90 MB lands in the engine utilityProcess, which is also where the 6 pool
// workers live and which peaks near 4.1 GB, so it is ~2% of the peak against pool.js's 1.5 GB reserve. Compaction is
// main-thread only and recompute never yields, so only one instrument is ever doing this at a time. If that 90 MB
// ever matters, compare the base in 1 MB chunks instead of materialising the whole string.
// Writes: +3.318 GB/day across the 144 instruments that actually tick (crypto proper is 3.147 GB/day of that), plus
// the base rewrites themselves, taking this file's ~3.4 GB/day to roughly 6.6-6.9 GB/day -- still ~200x better than the
// 1.78 TB/day it replaced, and against a 1 TB drive's 600 TBW that is well over a century. An earlier version of this
// comment said +4.62 GB / +81% / 5.67 GB of bases; all three were wrong, derived from one large-decimal file instead
// of the fleet. NOTE ON UNITS, since this is where two rounds of review disagreed: every GB above is decimal (1e9),
// which is why this block's "3.33 GB" of crypto bases and "3.104 GiB" are the same measurement. The crypto class is
// 134 symbols (engine_data.classes.crypto.rows, matching membership.coins, all 134 with a candle file) totalling
// 3.33 GB and 65,559,874 bars -- sidecars add +94.4% there. A claim of 100 files / 2.62 GB briefly stood here and was
// simply wrong.
//
// HOW STALENESS IS CAUGHT: the header records the base's own mtimeMs and byte size, read back AFTER the base was
// renamed into place. A read accepts the sidecar only if the base's current stat still matches, the magic and
// version match, and the file is exactly the length the bar count implies. So a base rewritten by anything that
// does not know about sidecars (an older build, a manual edit, a partial fetch) invalidates it automatically, and
// a torn sidecar fails the length check -- which catches only damage that CHANGES the length, so the body also
// carries a checksum (see binSum) for the damage that does not. The metrics-cache stamp() is deliberately NOT extended to cover the
// sidecar: it is derived, it changes no value, and coupling it there would invalidate metrics for no reason.
// ------------------------------------------------------------------------------------------------------------
function readBin(dir, fk){
  try {
    const bst = fs.statSync(basePath(dir, fk));                       // no base -> the sidecar means nothing
    const buf = fs.readFileSync(binPath(dir, fk));
    if (buf.length < BIN_HDR) return null;
    if (buf.readUInt32LE(0) !== BIN_MAGIC || buf.readUInt32LE(4) !== BIN_VER) return null;
    if (buf.readDoubleLE(8) !== bst.mtimeMs || buf.readDoubleLE(16) !== bst.size) return null;   // base moved on
    const n = buf.readUInt32LE(24), cols = buf.readUInt32LE(28);
    if (!(cols >= BIN_COLS && cols <= BIN_COLS_MAX)) return null;
    if (buf.length !== BIN_HDR + n * cols * 8) return null;           // torn or truncated (LENGTH damage only)
    // and the checksum for everything the length cannot see -- a flipped bit, a NaN, a swapped block
    if (buf.readUInt32LE(32) !== binSum(buf, BIN_HDR, n * cols * 8, n, cols)) return null;
    // A Float64Array VIEW requires an 8-byte-aligned byte offset, and a Buffer's byteOffset comes from Node's
    // internal pool -- today it is always 8-aligned so the view always works, but that is an allocator detail, not
    // a promise. If it ever stopped holding, the constructor would throw, readBin would return null, and the
    // sidecar would silently stop working everywhere with nothing on screen to say so. So check it, and copy into a
    // fresh (therefore aligned) buffer on the unaligned path rather than depending on luck. Normal path is the view.
    const off = buf.byteOffset + BIN_HDR;
    const f = (off % 8 === 0) ? new Float64Array(buf.buffer, off, n * cols)
                              : new Float64Array(buf.buffer.slice(off, off + n * cols * 8));
    const out = new Array(n);
    if (cols === BIN_COLS){                                           // the only shape that exists today: unrolled
      for (let i = 0, k = 0; i < n; i++, k += cols) out[i] = [f[k], f[k+1], f[k+2], f[k+3], f[k+4], f[k+5]];
    } else {
      for (let i = 0, k = 0; i < n; i++, k += cols){                   // wider bars: every column, nothing dropped
        const b = new Array(cols);
        for (let c = 0; c < cols; c++) b[c] = f[k+c];
        out[i] = b;
      }
    }
    return out;
  } catch (e) { return null; }                                        // absent, unreadable, anything -> JSON path
}

// Is the base file on disk byte-for-byte the JSON our caller wrote? A size comparison is NOT enough, and the
// difference matters: in the real flow every base is already at the trailing-year cap, so two concurrent compactors
// BOTH write exactly 525,601 bars -- one oldest bar dropped, one newest appended. The sizes then differ only if the
// dropped bar's JSON happens to be a different length from the added one, which on real data is a coin flip:
// re-measured as the pairing that actually occurs -- the length of the DROPPED-OLDEST bar against the length of the
// APPENDED-NEWEST one: BTC 1.5%, ETH 25.1%, SOL 20.7%, DOGE 16.8%, SHIB 11.2%, mean ~15%. Stating it loosely once
// led a reviewer to measure all-pairs-within-the-file instead and get BTC 13.6%, so the pairing is spelled out here:
// the disagreement was over WHICH quantity, not over the arithmetic. So a size check leaves a
// sidecar whose newest bar is a minute stale, for up to a day, roughly one time in seven. Comparing the bytes is
// conclusive instead of probabilistic. Cost, measured: 58-61 ms per call on a 31 MB base, and writeBase calls it TWICE
// (once before the sidecar write, once after), so 117-124 ms per compaction and ~15 s/day across the fleet -- matching
// the figure in the cost block above. An earlier version said "~47 ms ... about 6 s a day", which was one call's worth
// at an optimistic read speed and contradicted its own other number.
function baseIs(dir, fk, json){
  try {
    const st = fs.statSync(basePath(dir, fk));
    if (st.size !== Buffer.byteLength(json)) return false;              // cheap reject first
    return fs.readFileSync(basePath(dir, fk), "utf8") === json;
  } catch (e) { return false; }
}

// Best-effort and SILENT BY DESIGN: a sidecar that cannot be written costs speed and nothing else, so it must
// never fail a base write that has already succeeded. Written tmp+rename so a torn sidecar can never be read.
// expectJson is the exact base JSON the CALLER just wrote, and it is REQUIRED. It was optional once, which made the
// concurrent-writer protection default-OFF on an exported function: any future caller that forgot it would silently
// get the old defective behaviour with nothing to say so. A guard that can be skipped by omission is not a guard.
function writeBin(dir, fk, arr, expectJson){
  try {
    if (typeof expectJson !== "string") return false;                   // no way to verify -> do not write at all
    if (!Array.isArray(arr)) return false;
    // okBar accepts length >= 6, so a bar could carry extra trailing fields. This must be LOSSLESS, and getting that
    // wrong was a real bug: an earlier version stored only the first six columns, and because compact() reads through
    // read() -- sidecar included -- and writes the result straight back as the new base, a seventh column would have
    // been silently and PERMANENTLY deleted from the base file on the next compaction. A derived cache must never be
    // able to destroy its own source. So store the bar's REAL width in the header's cols field and write every column.
    // Refuse only what cannot be represented faithfully: a bar shorter than the six required columns, a width beyond
    // the sanity ceiling, or a file whose bars disagree with each other (no per-bar width in this format).
    const cols = Array.isArray(arr[0]) ? arr[0].length : 0;
    if (!(cols >= BIN_COLS && cols <= BIN_COLS_MAX)) return false;
    for (const b of arr) if (!Array.isArray(b) || b.length !== cols) return false;
    const bst = fs.statSync(basePath(dir, fk));                       // stat AFTER the rename: the real identity
    // IS THE BASE ON DISK THE ONE OUR CALLER WROTE? Two processes can writeBase the same instrument (the manual
    // refresh runs a full sweep, crypto included, while the 60 s crypto lane keeps ticking). If the other one's
    // base landed after ours, this stat describes THEIR base -- and stamping our array with their identity would
    // produce a sidecar that validates perfectly while holding a different bar series. Reproduced deterministically:
    // base held 101 bars, sidecar served 100, and since stamp() is base+log only, nothing would ever have noticed
    // and the wrong series would sit in the metrics cache until the next base write, up to a day.
    // Note this is a CONTENT mismatch, not a stat race -- the base identity is perfectly stable in that
    // interleaving, so re-stating alone cannot see it. The byte length our caller actually wrote can.
    if (!baseIs(dir, fk, expectJson)) return false;                     // not our base -> write nothing at all
    const n = arr.length;
    const buf = Buffer.allocUnsafe(BIN_HDR + n * cols * 8);
    buf.writeUInt32LE(BIN_MAGIC, 0); buf.writeUInt32LE(BIN_VER, 4);
    buf.writeDoubleLE(bst.mtimeMs, 8); buf.writeDoubleLE(bst.size, 16);
    buf.writeUInt32LE(n, 24); buf.writeUInt32LE(cols, 28);
    const woff = buf.byteOffset + BIN_HDR;                            // same alignment rule as readBin, both or neither
    const f = (woff % 8 === 0) ? new Float64Array(buf.buffer, woff, n * cols)
                               : new Float64Array(n * cols);
    if (cols === BIN_COLS){
      for (let i = 0, k = 0; i < n; i++, k += cols){
        const b = arr[i]; f[k]=b[0]; f[k+1]=b[1]; f[k+2]=b[2]; f[k+3]=b[3]; f[k+4]=b[4]; f[k+5]=b[5];
      }
    } else {
      for (let i = 0, k = 0; i < n; i++, k += cols){
        const b = arr[i]; for (let c = 0; c < cols; c++) f[k+c] = b[c];
      }
    }
    if (woff % 8 !== 0) Buffer.from(f.buffer, 0, n * cols * 8).copy(buf, BIN_HDR);
    buf.writeUInt32LE(binSum(buf, BIN_HDR, n * cols * 8, n, cols), 32);   // AFTER the body is in place
    buf.writeUInt32LE(0, 36);                                             // reserved. NOT covered by the checksum,
                                                                          // which starts at BIN_HDR -- nothing reads
                                                                          // or validates it today.
    writeAtomic(binPath(dir, fk), buf);
    // SECOND HALF of the same hazard: the other writer's base can land between our stat above and our rename just
    // now, so re-check. If the base is no longer the one in our header, this sidecar is a lie -- remove it. Removing
    // a derived cache costs one JSON parse on the next read; keeping a wrong one costs silently wrong numbers.
    // Byte-verified on BOTH sides of the write, which kills the length-collision case entirely (measured: the real
    // year-capped one-bar shift, 31,022,082 bytes either way, is refused). What this does NOT do is make the READ
    // side conclusive, and an earlier version of this comment wrongly claimed it did. readBin still validates by
    // (mtimeMs, size) alone -- it cannot read the base without giving up the whole point of the sidecar. So a wrong
    // sidecar still survives if the NEXT base lands in the same mtime bucket at the same byte size. Measured here:
    // the mtimeMs quantum is 0.38-0.50 ms (it tracks the Windows timer resolution, so treat it as a range measured on
    // two runs, not a constant) and renameSync preserves the tmp's write time, and while a full writeBin
    // (~160-450 ms) normally forces a bigger gap than that, writeAtomic's 120/240/360 ms rename backoff decouples
    // the two. Combined with the ~1-in-7 size collision that leaves roughly 1e-4 per concurrent-compaction event,
    // exposed for ~200 ms to 1 s until the other writer's own writeBin overwrites the sidecar. Three orders of
    // magnitude better than where this started, and not zero.
    const after = fs.statSync(basePath(dir, fk));
    if (after.mtimeMs !== bst.mtimeMs || !baseIs(dir, fk, expectJson)){
      // Usually our own file and now known to be a lie. It can also be a sidecar the OTHER writer wrote correctly in
      // the meantime (demonstrated) -- but our writeAtomic above already replaced that one, so the unlink is removing
      // our bytes either way. Cost of being wrong here is one JSON parse on the next read. Fail-safe, not data loss.
      try { fs.unlinkSync(binPath(dir, fk)); } catch (e2) {}
      return false;
    }
    return true;
  } catch (e) { return false; }
}

// The metrics cache is invalidated on the candle file's mtime. Appends no longer touch the BASE file, so a
// base-only mtime would never change and recompute would serve cached metrics forever -- the whole app would
// silently freeze on the first run after this change. The stamp therefore covers the log's size and mtime too.
function stamp(dir, fk){
  let s = "";
  try { const st = fs.statSync(basePath(dir,fk)); s = st.mtimeMs + ":" + st.size; } catch(e){ s = "0:0"; }
  try { const lt = fs.statSync(logPath(dir,fk));  s += ":" + lt.mtimeMs + ":" + lt.size; } catch(e){ s += ":0:0"; }
  return s;
}

// Parse the log. Returns the bars it could read plus how many lines it had to skip, so a caller can report rather
// than swallow. A trailing torn line is expected and harmless; anything else is worth surfacing.
function readLog(dir, fk, okBar){
  const p = logPath(dir, fk);
  // Read as BYTES and measure the buffer, not the decoded string: invalid UTF-8 decodes each bad byte to U+FFFD
  // (3 bytes), so Buffer.byteLength(txt) overshoots the real file length -- and `bytes` is the offset writeBase
  // truncates from, so an overshoot silently slices the start off a good line. Measured: 33 reported vs 29 actual.
  let buf; try { buf = fs.readFileSync(p); } catch(e){ return { bars:[], lines:0, skipped:0, bytes:0 }; }
  const txt = buf.toString("utf8");
  const raw = txt.length ? txt.split("\n") : [];
  const bars = []; let skipped = 0, lines = 0, bad = false;
  for (const ln of raw){
    if (!ln) continue;
    lines++;
    let b = null, good = false;
    try { b = JSON.parse(ln); good = okBar(b); } catch(e){ good = false; }
    if (!good){                                    // STOP. Everything after an unreadable line is unreachable data:
      skipped = 1;                                 // keeping it would advance lastT past the hole and lose the gap.
      bad = (lines < raw.filter(x => x.trim()).length);   // a torn FINAL line is normal; a mid-file one is damage. Trim, or a
                                                     // whitespace-only line after a torn tail would read as damage.
      break;
    }
    bars.push(b);
  }
  return { bars, lines, skipped, bad, bytes: buf.length };
}

// READ. Base + log, merged by timestamp with the log winning, time-ascending.
// Throws if the BASE exists but cannot be read -- callers must treat that as "abort", never as "no history".
// That is the guard that stops a partial fetch from replacing a year with a sliver.
function read(dir, fk, okBar){
  const bp = basePath(dir, fk);
  const exists = fs.existsSync(bp);
  let base = [];
  if (exists){
    // The sidecar is tried first and is accepted only if it still matches this exact base file. okBar is applied
    // either way, so the sidecar never has to be invalidated when okBar changes -- it holds bars, not a verdict.
    // The EBASEREAD guard below is unaffected: anything that could corrupt or shorten the base changes its size or
    // mtime, which fails the sidecar's header check and sends the read down the JSON path to throw as before.
    const packed = readBin(dir, fk);
    if (packed) base = packed.filter(okBar);
    else {
      let j; try { j = JSON.parse(fs.readFileSync(bp, "utf8")); }
      catch(e){ const err = new Error("base-unreadable"); err.code = "EBASEREAD"; throw err; }
      base = (j.candles || []).filter(okBar);
    }
  }
  const lg = readLog(dir, fk, okBar);
  if (!lg.bars.length) return { exists, candles: base, logLines: lg.lines, logSkipped: lg.skipped, logBad: !!lg.bad, logBytes: lg.bytes };
  // FAST PATH, and it is not an optimisation nicety -- it is what keeps this change from trading writes for CPU.
  // In the normal case every logged bar is strictly newer than the base last bar, so the answer is a concatenation.
  // Building a Map over the base instead costs a 525,601-entry Map plus a sort on EVERY read: measured 274 ms ->
  // 413 ms on btc.json, which across 134 assets is ~18 s added to every sweep -- the same table-rebuild lag that
  // had to be fixed once already. Concatenating is O(log bars).
  const baseLast = base.length ? base[base.length-1][0] : -Infinity;
  let ascending = true, prev = baseLast;
  for (const bar of lg.bars){ if (bar[0] <= prev){ ascending = false; break; } prev = bar[0]; }
  if (ascending) return { exists, candles: base.concat(lg.bars), logLines: lg.lines, logSkipped: lg.skipped, logBad: !!lg.bad, logBytes: lg.bytes };
  // Only an out-of-order or overlapping log needs the full merge (a duplicate timestamp, or a log written before a
  // base that was later rebuilt). The log wins on a collision.
  const m = new Map();
  for (const c of base) m.set(c[0], c);
  for (const bar of lg.bars) m.set(bar[0], bar);
  return { exists, candles: [...m.values()].sort((x,y)=>x[0]-y[0]), logLines: lg.lines, logSkipped: lg.skipped, logBad: !!lg.bad, logBytes: lg.bytes };
}

// APPEND. One write, proportional to the new bars. Returns how many lines are now in the log so the caller can
// decide whether to compact.
function append(dir, fk, bars, priorLines){
  if (!bars.length) return priorLines || 0;
  const txt = bars.map(b => JSON.stringify(b)).join("\n") + "\n";
  fs.appendFileSync(logPath(dir, fk), txt);
  return (priorLines || 0) + bars.length;
}

// WRITE THE BASE FROM SCRATCH. Used on a first run (no base yet) and by compaction. Truncates the log only AFTER
// the base is safely renamed into place.
// foldedBytes: how many bytes of the LOG the caller has already folded into `arr`. Compaction passes it so a
// concurrent appender's bars survive the truncate; a first-run seed passes nothing and the whole log is cleared.
function writeBase(dir, fk, sym, intervalMin, arr, foldedBytes){
  const json = JSON.stringify({ sym: String(sym).toUpperCase(), interval: intervalMin + "m", candles: arr });
  writeAtomic(basePath(dir, fk), json);
  // The ONLY place a sidecar is written: the base is being rewritten here anyway, so this adds one write per
  // instrument per compaction and none at all on the per-minute tick. It cannot fail this function -- the base is
  // already safely in place, and a missing sidecar only costs the JSON path. The JSON is passed so writeBin can tell
  // OUR base from a concurrent writer's before it stamps this array with the wrong identity.
  // Its result is RETURNED rather than swallowed: every refusal in there -- a width this format cannot hold, a
  // concurrent writer, a locked rename -- was invisible to every caller. Nothing consumes it yet; it exists so a
  // caller CAN ask instead of assuming. Captured here and returned at the END, because the log truncation below is
  // the whole point of this function and must not be skipped.
  const binOk = writeBin(dir, fk, arr, json);
  // TRUNCATE ONLY WHAT WE FOLDED IN. foldedBytes (compaction passes the byte length its own read consumed) keeps any
  // bytes a concurrent appender added after that point; without it those bars are silently destroyed. Callers that
  // are creating a base from scratch pass nothing and the whole log goes, which is the old behaviour and correct
  // there. If the log somehow shrank below what we folded, blank it -- that is the torn case, and the dedup on the
  // next read makes a repeat harmless while keeping a mystery tail is not.
  try {
    const lp = logPath(dir, fk);
    let keep = "";
    // `!= null`, not `> 0`: zero means "this caller folded in nothing, so keep the whole log", which is exactly the
    // set-aside case below. Treating 0 as "no offset given" truncated unconditionally -- the original bug, back again.
    if (foldedBytes != null){
      const cur = fs.readFileSync(lp);
      if (cur.length > foldedBytes) keep = cur.subarray(foldedBytes).toString("utf8");
    }
    fs.writeFileSync(lp, keep);
  } catch(e){}   // a failure here only means the next read dedups a repeat
  return binOk;
}

// COMPACT. Fold the log into the base, trim to a trailing year, truncate the log. Idempotent.
function compact(dir, fk, sym, intervalMin, yearMs, okBar){
  const r = read(dir, fk, okBar);
  if (!r.candles.length) return { ok:false, bars:0 };
  let arr = r.candles;
  const cutoff = arr[arr.length-1][0] - yearMs;
  arr = arr.filter(c => c[0] >= cutoff);
  // A damaged log: set the unreadable remainder aside BEFORE writing the base, so a crash in between leaves the
  // original log intact rather than a base plus a half-handled log.
  let setAside = null;
  if (r.logBad){
    setAside = logPath(dir, fk) + ".bad." + Date.now();   // stamped: a fixed name would overwrite an earlier set-aside
    try { fs.renameSync(logPath(dir, fk), setAside); }
    catch(e){ return { ok:false, bars:0, reason:"cannot-set-aside" }; }   // leave everything alone rather than half-fix it
  }
  // How many bytes of log did the read above actually FOLD IN? writeBase truncates the log, and truncating it
  // unconditionally throws away anything the OTHER lane appended while this compaction was running -- reproduced:
  // lane A read 3440 bars, lane B appended 3, the truncate left 3440 and the 3 were gone. They come back on the next
  // gap fetch, but a silent write-loss inside the read-modify-write path is exactly what must not happen here.
  // So keep whatever arrived after our read instead of blanking the file.
  // On the set-aside path the log we measured was RENAMED away, so that byte offset now addresses whatever a
  // concurrent appender has since created -- applying it sliced a good file into an unparseable one, which the next
  // compaction then set aside as ANOTHER .bad file, on and on. Zero there means "keep whatever is in the new log".
  const foldedBytes = setAside ? 0 : (r.logBytes || 0);
  try { writeBase(dir, fk, sym, intervalMin, arr, foldedBytes); }
  catch(e){ if(setAside){ try{ fs.renameSync(setAside, logPath(dir, fk)); }catch(e2){} } throw e; }   // put it back
  return { ok:true, bars: arr.length, recovered: !!r.logBad, setAside };
}

module.exports = { read, append, writeBase, compact, stamp, readLog, writeAtomic,
                  basePath, logPath, binPath, readBin, writeBin,
                  LOG_EXT, BIN_EXT, BIN_VER, COMPACT_AFTER_BARS };
