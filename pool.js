// A SYNCHRONOUS worker pool.
//
// WHY SYNCHRONOUS. recompute() carries an invariant the two-lane scheduler depends on (see the note above it in
// engine.js): it and every writeAtomic are fully synchronous, with no `await` anywhere, so on Node's single thread
// they run to completion without yielding and the concurrent fetch streams can never interleave with them. Turning
// recompute async to await workers would quietly break that -- a stream could resume mid-recompute and call
// afterClass, re-entering recompute on a half-built table. So the parent BLOCKS on Atomics.wait instead: the work
// leaves the thread, the thread does not yield, and the invariant is untouched.
// (Verified in the real host: Atomics.wait is permitted on an Electron utilityProcess main thread, and workers load
// from inside app.asar. receiveMessageOnPort dequeues directly and runs no user JS, so nothing pumps the loop.)
//
// SELF-SCHEDULING, NOT WAVES. The first version dispatched N tasks and blocked on them in dispatch order, so every
// wave cost max(wave) instead of mean(wave) -- per-asset cost varies 316-1325 ms (CV 0.18), which is more than enough
// for that to hurt. Measured, 32 assets, warmed workers:
//     workers      1      2      4      6      8     12
//     waves    36354  20054  14466  13116  12286  12397 ms
//     pull     33628  17843  11958  10614  10610  10728 ms
// Self-scheduling wins at every count. The plateau above is ~3.4x at 6 on 32 assets, but an independent re-run on
// 21 cold assets got 2.25-2.73x -- fewer tasks over 6 workers leaves a raggeder tail, so the two are not in conflict
// and 3.4x should be read as the 32-asset figure it is, not as the pool's general speed-up. An earlier comment here
// claimed it "saturates at 4" and blamed memory bandwidth; that was wrong -- it was measuring this file's own
// dispatch flaw, in a harness that re-spawned workers inside the timed region. Deleted rather than left to mislead.
//
// FAIL-SAFE BY CONSTRUCTION: every entry point returns null (or a per-task null) rather than throwing. A caller that
// gets null must do the work inline, which is exactly the behaviour that shipped before this file existed.
// That property rests on ONE invariant: the shared counter may never run ahead of the messages the parent can
// actually retrieve. A version of this pool incremented it even when postMessage threw, and the result was not a
// slow pass but a permanently hot-spinning engine process -- see the worker, and the second backstop in run().
const path = require("path");
let W = null;
try { W = require("worker_threads"); } catch (e) { W = null; }

const WORKER_FILE = path.join(__dirname, "pool_worker.js");
// A wedged worker blocks this thread, and w.on("error") CANNOT fire while it is blocked -- it is an event-loop
// callback. So the timeout is the only backstop and it must be short: a stall costs the whole engine this long.
// 20 s is ~15x the slowest observed single asset (1.3 s) and a third of the tick it protects.
const TASK_TIMEOUT_MS = 20000;
// Marginal RSS per worker, measured twice by external sampling and NOT agreeing: ~467 MB in one run, ~567 MB in an
// independent 21-asset run (inline 922 MB vs pooled 4,323 MB peak). 500 MB therefore under-reserves by ~13% against
// the higher figure; the 1.5 GB headroom below still covers it in aggregate, but the honest statement is a range,
// not a number, so treat this as ~500-570 MB rather than a measured constant.
const PER_WORKER_BYTES = 500 * 1024 * 1024;
let pool = null, poolBroken = false;

function size(){
  let cores = 4, freeB = 0;
  try { const os = require("os"); cores = os.cpus().length; freeB = os.freemem(); } catch (e) {}
  // ZERO IS A REAL ANSWER, and clamping it up to 1 was a measured regression: a single worker runs 34,090 ms where
  // the plain inline path runs 32,530 ms, because one worker adds spawn, clone and hand-off cost and removes no
  // serialisation. So "cannot afford a worker" and "can afford exactly one" must BOTH mean no pool -- run() already
  // handles 0 correctly (nothing dispatched, every result null, the caller computes inline, which is the behaviour
  // that shipped before this file existed). Floors of 1 here would hand small or busy machines a guaranteed
  // slowdown, which is worse than not having the pool.
  let n = Math.min(6, cores - 2);
  // Each worker is heavy: PER_WORKER_BYTES is the ~500-570 MB of marginal RSS noted above, and an externally sampled cold
  // pooled pass peaked at 4.1 GB for ONE engine process with 6 workers. Keep ~1.5 GB of headroom so a busy machine
  // degrades to fewer workers rather than paging or OOM-ing one, since an OOM costs a stall and not just a slow
  // pass. NOTE what this reserve does NOT do: the two lanes are separate processes and each calls freemem()
  // independently, so both can size themselves to 6 at the same moment and neither sees the other's footprint.
  // Sizing across lanes would need shared state that does not exist here.
  if (freeB > 0){
    const afford = Math.floor((freeB - 1.5 * 1024 * 1024 * 1024) / PER_WORKER_BYTES);
    n = Math.min(n, afford);
  }
  return n > 1 ? n : 0;
}

function spawn(){
  if (poolBroken || !W || !W.Worker || typeof SharedArrayBuffer === "undefined" || !Atomics || !W.receiveMessageOnPort) return null;
  if (pool) return pool;
  try {
    const n = size();
    if (n < 1) return null;                                  // no pool at all -> the caller works inline
    const workers = [];
    const done = new Int32Array(new SharedArrayBuffer(4));   // ONE counter every worker increments -- see run()
    for (let i = 0; i < n; i++){
      const ch = new W.MessageChannel();
      const w = new W.Worker(WORKER_FILE, { workerData: { done: done.buffer, port: ch.port2 }, transferList: [ch.port2] });
      w.unref();                                             // never hold the process open; cannot fire mid-task
      w.on("error", () => { poolBroken = true; });
      workers.push({ w, port: ch.port1, busy: -1 });
    }
    pool = { workers, done };
    return pool;
  } catch (e) { poolBroken = true; pool = null; return null; }
}

// tasks: [{fk, dataDir, C:{key,thr,base}, priorM, needBtc}]  ->  results aligned to tasks, each {m,lastT} or null.
// Null anywhere means "caller computes this one inline".
function run(tasks){
  // A POOL THAT CANNOT PAY FOR ITSELF MUST NOT EXIST -- the same rule size() enforces on workers, enforced here on
  // tasks. One task cannot be parallelised, so it pays spawn + clone + hand-off and saves nothing: measured 1 asset
  // inline 1487/1567/1548 ms vs pooled 1742/1799/1744 ms, a 13-16% LOSS, while 2 assets already win (3445 -> 2465)
  // and 3 win bigger (4578 -> 2579). Break-even is between 1 and 2, so 1 goes inline. This is reachable in normal
  // operation, not a corner: afterClass recomputes per class (oil has 2 symbols), and any tick where exactly one
  // instrument changed since the last pass produces a single-task pre-pass.
  if (!Array.isArray(tasks) || tasks.length < 2) return null;
  const p = spawn();
  if (!p) return null;
  const out = new Array(tasks.length).fill(null);
  try {
    const { workers, done } = p;
    Atomics.store(done, 0, 0);
    let next = 0, consumed = 0, dispatched = 0;
    const send = (slot) => {
      if (next >= tasks.length) return false;
      const i = next++;
      try { slot.port.postMessage({ i, t: tasks[i] }); slot.busy = i; dispatched++; return true; }
      catch (e) { slot.busy = -1; return false; }            // out[i] stays null -> inline
    };
    for (const s of workers) send(s);
    // Block until every dispatched task has reported. A worker increments `done` AFTER posting its result, so
    // done > consumed guarantees at least one message is sitting in some port.
    let lastProgress = Date.now();
    while (consumed < dispatched){
      const v = Atomics.load(done, 0);
      if (v === consumed){
        const r = Atomics.wait(done, 0, v, TASK_TIMEOUT_MS);
        if (r === "timed-out" && Atomics.load(done, 0) === consumed){
          poolBroken = true;                                 // a wedged worker: stop trusting the pool for this run
          shutdown();                                        // and TERMINATE it: a worker stuck in a loop otherwise
          return out;                                        // keeps burning a core for the rest of the run
                                                             // everything not yet collected stays null -> inline
        }
      }
      // SECOND BACKSTOP, and it is not redundant. The wait above only arms while `done` equals `consumed`. If the
      // counter ever runs AHEAD of the retrievable messages, that branch is skipped on every pass and this loop
      // spins hot forever with no timeout -- the worst failure this file can have. The worker is what guarantees
      // the counter never runs ahead (it increments only on a successful post), so this is the guard for the case
      // where that guarantee is broken by something we did not foresee. Same 20 s budget, measured from the last
      // real completion rather than from the last wait, so a slow-but-progressing pass is never cut short.
      const before = consumed;
      for (const slot of workers){
        if (slot.busy < 0) continue;
        let msg = null;
        try { msg = W.receiveMessageOnPort(slot.port); } catch (e) { msg = null; }
        if (!msg) continue;
        const got = msg.message;
        if (got && typeof got.i === "number"){
          if (got.ok && got.i === slot.busy) out[got.i] = { m: got.m, lastT: got.lastT };
          consumed++;
          slot.busy = -1;
          send(slot);                                        // hand this worker the next task immediately
        }
      }
      if (consumed !== before) lastProgress = Date.now();
      else if (Date.now() - lastProgress > TASK_TIMEOUT_MS){
        poolBroken = true;
        shutdown();                                          // same reason as above: do not leave it spinning
        return out;                                          // uncollected stay null -> inline, same as a timeout
      }
    }
    return out;
  } catch (e) { poolBroken = true; return out; }
}

function shutdown(){
  if (!pool) return;
  for (const s of pool.workers){ try { s.w.terminate(); } catch (e) {} try { s.port.close(); } catch (e) {} }
  pool = null;
}

module.exports = { run, shutdown, size };
