// One pool worker: read an instrument's candles and compute its metrics. Nothing else lives here -- every RULE
// (which instruments are stale, whether the slow half may be reused, what goes in the cache entry) stays in the
// parent, so there is exactly one place those decisions are made and the worker cannot drift from the inline path.
const { workerData, parentPort } = require("worker_threads");
const fs = require("fs");
const E = require("./engine.js");
const CS = require("./candles.js");

const done = new Int32Array(workerData.done);
const port = workerData.port;

// btcSign is a ~105k-entry map, derived here rather than cloned per task, and kept while it is VALID.
// It is deliberately NOT latched on failure. The first version set a "tried" flag once and never retried, so a single
// unreadable btc.json -- a torn base, a sharing violation while the other lane renames it, anything transient --
// pinned btcSign to null for the worker's whole life. That null then went into the metrics cache and was served from
// there for a full hour, blanking the BTC-correlation column with nothing on screen to say so. The inline path
// retries every recompute, so this must too: a null result is simply not remembered.
// It is also KEYED to the generation the parent names (btcKey = CS.stamp of btc.json). The worker holds btcSign
// across tasks, but recompute runs several times per run and re-reads btc.json each time, so an unkeyed cache let
// the worker answer from an older btc than the inline path was using -- measured as sol/doge/shib getting a stale
// numeric btcCorr where inline produced null. A changed key means re-derive, full stop.
// The key the parent sends is derived from the BARS it used (count + last bar time), so this worker can compute the
// same thing from its own read and check that the two really are the same series. Verifying after the read is what
// makes the check sound: a key checked only against a cached label would still match while the underlying file had
// moved on. A mismatch returns null, which the caller turns into a decline, so the parent recomputes that one inline.
let btcSign = null, btcKey = null;
function btc(dataDir, key){
  if (btcSign && btcKey === key) return btcSign;
  btcSign = null; btcKey = null;
  try {
    if (fs.existsSync(CS.basePath(dataDir, "btc"))){
      let bc = CS.read(dataDir, "btc", E.okBar).candles;
      // Demanding an exact match would be correct but expensive: the other lane appends to btc.json every minute, so
      // a read that lands a bar later than the parent's would decline EVERY crypto task and send the whole pass
      // serial. The store is append-only and time-ascending, so the parent's series is a PREFIX of a longer read --
      // trim to its bar count and confirm the last bar time agrees, which reconstructs the parent's exact series
      // rather than guessing at it. A compaction that trimmed the front instead fails that check and declines.
      const cut = key ? key.split(":") : [];
      const want = cut.length === 2 ? +cut[0] : -1, wantT = cut.length === 2 ? +cut[1] : -1;
      if (Array.isArray(bc) && bc.length > want && want > 0) bc = bc.slice(0, want);
      if (Array.isArray(bc) && bc.length === want && bc[bc.length-1][0] === wantT){
        btcSign = E.sign5(bc) || null;              // provably the same series the parent used
        if (btcSign) btcKey = key;
      }
    }
  } catch (e) { btcSign = null; btcKey = null; }
  return btcSign;                                   // null -> declined, and retried on the next task that needs it
}

port.on("message", (msg) => {
  const i = msg && msg.i, t = msg && msg.t;
  let res = { i: i, ok: false };
  try {
    const B = CS.read(t.dataDir, t.fk, E.okBar).candles;
    if (Array.isArray(B) && B.length >= 50){
      // WHETHER BTC IS AVAILABLE IS THE PARENT'S ANSWER, NOT THIS WORKER'S. t.btcOk is what the parent's own read
      // produced, so the worker uses null exactly where the inline path would -- which is the whole invariant.
      // An earlier version instead declined any task it could not get btcSign for, on the theory that it avoided
      // publishing a row with a silently-null btcCorr. That was false: the inline path computes the very same null
      // and caches it either way, so the decline published nothing different -- and on a cold cache, where needBtc
      // is true for 100% of crypto tasks, it turned the entire pass serial, ~150 s instead of ~55 s, for no gain.
      // The ONE case still worth declining is the parent reading btc.json fine while this worker cannot (a
      // transient lock, the other lane renaming it): there the parent has a value, so publishing null WOULD differ.
      // A null btcCorr surviving an hour is a separate, pre-existing question about the parent's freshness rule.
      const wantBtc = (t.btcOk && t.needBtc && t.C.key === "crypto" && String(t.fk).toLowerCase() !== "btc");
      const bs = wantBtc ? btc(t.dataDir, t.btcKey) : null;
      if (wantBtc && !bs){
        res = { i: i, ok: false };                  // parent had BTC and we do not -> inline, rather than diverge
      } else {
        const m = E.computeMetrics(B, t.C, bs, t.priorM || null);
        res = { i: i, ok: true, m: m, lastT: B[B.length - 1][0] };
      }
    }
  } catch (e) { res = { i: i, ok: false }; }
  // The counter may ONLY advance for a message the parent can actually retrieve. Incrementing unconditionally is a
  // hang: if postMessage throws (a clone failure, an allocation failure under exactly the memory pressure the pool
  // size exists to bound, or any future non-cloneable field in the metrics), `done` runs ahead of the messages, the
  // parent's Atomics.wait is skipped forever because v never equals consumed, and run() hot-spins on a pegged core
  // with the 20 s backstop never arming -- a permanently wedged engine process, worse than the 180 s freeze this
  // pool was written to remove. Not incrementing instead lets the wait time out, which marks the pool broken and
  // sends every uncollected task inline. That is the degradation this file is supposed to have.
  let posted = false;
  try { port.postMessage(res); posted = true; } catch (e) { posted = false; }
  if (posted){
    Atomics.add(done, 0, 1);                        // increment AFTER posting: the parent relies on that ordering
    Atomics.notify(done, 0);
  }
});
