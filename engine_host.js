// Runs inside an Electron utilityProcess — OFF the main/UI thread. The update engine does heavy synchronous
// work (recompute over gigabytes of candles, large JSON parse/stringify, file I/O); running it here means it
// can never block the window. The main process forks this, sends one {type:'start'} message, and gets
// progress + done/error messages back. This process writes engine_data.json to disk itself (engine.recompute),
// so the data never has to cross the process boundary — only small status messages do.
const engine = require('./engine');

process.parentPort.on('message', async (e) => {
  const m = (e && e.data) || {};
  if (m.type !== 'start') return;
  try {
    const log = await engine.update({
      dataDir: m.dataDir,
      polygonKey: m.polygonKey,
      enabled: m.enabled,
      only: m.only,                                                   // restrict this run to these source groups (null = full sweep)
      onProgress: (done, total, detail) => process.parentPort.postMessage({ type: 'progress', payload: { done, total, detail } }),
      onClassDone: (key, label, done, total) => process.parentPort.postMessage({ type: 'partial', payload: { key, label, done, total } }),
    });
    process.parentPort.postMessage({ type: 'done', log });
  } catch (err) {
    process.parentPort.postMessage({ type: 'error', error: (err && err.message) ? err.message : String(err) });
  }
});
