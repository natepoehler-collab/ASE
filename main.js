// Asset Selection Engine — Electron main process.
// Owns the app's data folder. The update ENGINE runs in a background utilityProcess (engine_host.js) — OFF
// the UI thread — so its heavy work (recompute over gigabytes, big JSON parse/write) can never freeze the
// window. Update runs on launch (first run = full download) and hourly; the renderer can also trigger one
// via Refresh (IPC). Progress + completion are pushed to the window so it can show a download screen.
const { app, BrowserWindow, WebContentsView, nativeTheme, ipcMain, utilityProcess, Notification } = require('electron');
const path = require('path');
const { dfile, isAliased } = require('./file_alias.js');
const CS = require('./candles.js');   // the SAME alias resolution the engine uses -- see file_alias.js
const fs = require('fs');
nativeTheme.themeSource = 'dark';   // dark native title bar on Windows

const DATA_DIR = path.join(app.getPath('userData'), 'data');
const CONFIG = path.join(app.getPath('userData'), 'config.json');
const readConfig = () => { try { return JSON.parse(fs.readFileSync(CONFIG, 'utf8')); } catch (e) { return {}; } };
const writeConfig = (c) => { try { fs.mkdirSync(path.dirname(CONFIG), { recursive: true }); fs.writeFileSync(CONFIG, JSON.stringify(c)); return true; } catch (e) { return false; } };
const getKey = () => readConfig().polygonKey || '';
const setKey = (k) => { const c = readConfig(); c.polygonKey = k; return writeConfig(c); };
// refresh toggles: which source GROUPS auto-update. Default ON; turning one off freezes that source on current data.
const getToggles = () => { const r = readConfig().refresh || {}; return { crypto: r.crypto !== false, duka: r.duka !== false, stock: r.stock !== false }; };
const setToggle = (g, on) => { const c = readConfig(); c.refresh = Object.assign({}, c.refresh); c.refresh[g] = !!on; return writeConfig(c); };
const SEED = path.join(__dirname, 'seed_engine_data.json');   // the snapshot that ships INSIDE the app (read-only)
// Ship-with-data: show the bundled snapshot until fresh data downloads. We do NOT copy it to disk (that copy can
// fail silently on some machines — it's what left a recipient's table blank). Instead loadData falls back to
// reading the seed straight from inside the app when the on-disk table isn't there yet. recompute() preserves
// not-yet-downloaded rows from the same bundled seed, so classes (e.g. keyless stocks) can never blank.
// Read the on-disk table, then MERGE the bundled seed to fill any instrument/class the on-disk table is missing.
// Fresh install (no engine_data.json) already returned the seed; this also covers an UPGRADE, where the prior
// version's engine_data.json sits in userData and lacks newly-added instruments (e.g. v1.2.1's 5 new metals) — those
// would otherwise be absent at first paint until the first recompute. On-disk ALWAYS wins (the seed only fills gaps,
// never overwrites fresh data); the merge is in-memory only (never written back — the launch recompute rewrites
// engine_data.json properly within seconds, re-sorted and with notFresh set).
// A cross-class aliased instrument (see file_alias.js) has no trustworthy row in the SEED: the seed was built
// before the alias existed, so its row was computed from the OTHER class's candle file -- seed usstock CVX is
// price 2.26 / btcCorr 69.6, byte-identical to seed crypto CVX, and btcCorr is a crypto-only field. Dropping
// those rows at load is why the gap-fill merge below needs no special case, and it also covers the fresh-install
// branch that returns the seed outright. The row reappears the moment the instrument's OWN file is computed.
// The ON-DISK table needs the same treatment, but CONDITIONALLY. At first paint after upgrading, engine_data.json
// was still written by a pre-alias build, so its usstock CVX row is Convex Finance -- and loadData returns the
// on-disk table here (the seed is an older calc, so it loses), which is why the fix reached the seed and Nate
// still saw 2.04 for the few minutes until the launch recompute landed. But once the instrument's OWN file exists
// the on-disk row is a GENUINE one, so stripping unconditionally would delete the real Chevron row forever.
// onlyWhenNoFile makes the distinction: no file of its own => no trustworthy row for it, whatever the table says.
const stripAliased = (t, onlyWhenNoFile) => {
  if (!t || !t.classes) return t;
  const gone = (k, a) => !onlyWhenNoFile || !fs.existsSync(path.join(DATA_DIR, dfile(k, a) + '.json'));
  for (const k in t.classes) { const c = t.classes[k]; if (c && Array.isArray(c.rows)) c.rows = c.rows.filter(r => !(r && isAliased(k, r.a) && gone(k, r.a))); }
  return t;
};
// ---- YUBIT GATE, RENDERER SIDE ------------------------------------------------------------------------------
// The bundled seed_engine_data.json predates the gate and still holds crypto rows Yubit does not list. Both seed
// paths below go through this, so a fresh install and a CACHE_VER bump paint the same universe the engine keeps.
// Missing/unreadable list -> fails OPEN (everything shown), never silently empties the board.
let _YB = null;
const yubitBases = () => {
  if (_YB) return _YB;
  try { const j = JSON.parse(fs.readFileSync(path.join(__dirname, "yubit_perps.json"), "utf8"));
        _YB = new Set((j.syms || []).map(x => String(x).toUpperCase())); }
  catch (e) { _YB = new Set(); }
  return _YB;
};
const onYubitRow = a => { const Y = yubitBases(); if (!Y.size || !a) return true; const u = String(a).toUpperCase(); return Y.has(u) || Y.has("1000" + u); };
const yubitOnly = (t) => {                                  // a COPY: the bundled seed object is never mutated
  try {
    if (!t || !t.classes || !t.classes.crypto) return t;
    const out = Object.assign({}, t, { classes: Object.assign({}, t.classes) });
    out.classes.crypto = Object.assign({}, t.classes.crypto,
      { rows: (t.classes.crypto.rows || []).filter(r => onYubitRow(r && r.a)) });
    return out;
  } catch (e) { return t; }
};
const loadData = () => {
  let seed = null, disk = null;
  try { seed = stripAliased(JSON.parse(fs.readFileSync(SEED, 'utf8'))); } catch (e) {}
  try { disk = stripAliased(JSON.parse(fs.readFileSync(path.join(DATA_DIR, 'engine_data.json'), 'utf8')), true); } catch (e) {}
  if (!disk || !disk.classes) return disk || seed;          // fresh install / unreadable on-disk -> seed (or null)
  if (!seed || !seed.classes) return disk;                   // no seed -> on-disk as-is
  if (seed.metricsVer > disk.metricsVer) return yubitOnly(seed);   // a CACHE_VER bump paints the WHOLE seed -- filter it, or every non-Yubit coin reappears        // show the seed ONLY when it's a NEWER calc than the on-disk table (a fresh upgrade whose bundled seed carries the new calc) -> correct-calc first paint, then the launch recompute rewrites engine_data.json. If on-disk is the SAME-or-NEWER calc (already recomputed this session, OR the shipped seed simply lags the code) it's current -> fall through and show it. This prevents a stale bundled seed from masking freshly-computed on-disk data (the bug that hid the mkt-cap column: disk metricsVer 6 was being shadowed by a metricsVer-5 seed)
  try {
    disk.classOrder = disk.classOrder || [];
    for (const k of (seed.classOrder || [])) {
      if (!disk.classes[k]) {                                 // whole class missing on disk -> take it from the seed
        disk.classes[k] = seed.classes[k];
        if (!disk.classOrder.includes(k)) disk.classOrder.push(k);
        continue;
      }
      const cls = disk.classes[k]; cls.rows = cls.rows || [];
      const have = new Set(cls.rows.map(r => r && r.a));
      let added = 0;
      const okRow = r => (k !== "crypto") || onYubitRow(r && r.a);            // the seed is a snapshot from before the Yubit gate; it still carries rows he cannot trade
      for (const r of ((seed.classes[k] && seed.classes[k].rows) || [])) if (r && !have.has(r.a) && okRow(r)) { cls.rows.push(r); added++; }   // seed fills gaps only; on-disk rows untouched (aliased rows are already stripped from the seed above)
      const baseIdx = (cls.thresholds || []).indexOf(cls.base);
      if (added && baseIdx >= 0) cls.rows.sort((x, y) => ((y.counts && y.counts[baseIdx]) || 0) - ((x.counts && x.counts[baseIdx]) || 0));   // keep the class sorted so filled-in rows aren't stranded at the bottom
    }
  } catch (e) { /* any merge hiccup -> return the on-disk table unharmed */ }
  return disk;
};

let win;   // busy/child moved into the two lanes above

function createWindow() {
  win = new BrowserWindow({
    width: 1480, height: 920, backgroundColor: '#0e1116', title: 'Asset Selection Engine',
    icon: path.join(__dirname, 'icon.ico'),
    webPreferences: { preload: path.join(__dirname, 'preload.js'), contextIsolation: true, nodeIntegration: false }
  });
  win.removeMenu();
  win.on('closed', () => { win = null; chartView = null; });   // the docked view is a child of win -- it dies with it, just drop the reference
  win.on('focus', () => { try { win.flashFrame(false); } catch (err) {} });   // an alert flashed the taskbar; looking at it clears the flash
  win.on('resize', layoutChartView);   // keep the docked chart's bounds matched to the window as it's resized; a no-op while no docked chart is open
  // SPLIT BY LANE, exactly as the hourly sweep does. An unrestricted launch run put crypto in the FULL lane while the
  // 60 s tick was already pulling crypto in its own lane, so on a first run every crypto asset was downloaded twice --
  // ~10 GB of duplicated kline traffic on a metered connection with a monthly cap. Each lane now owns its own sources,
  // and firing the crypto lane here rather than waiting for the first tick keeps the launch download just as prompt.
  win.webContents.once('did-finish-load', () => { try { win.webContents.setZoomFactor(1.1); } catch (e) {} runUpdate('launch', ['stock','duka']); runUpdate('launch-crypto', ['crypto']); });   // +10% native page zoom (uses the side space; no vh/scrollbar issue, unlike CSS zoom) + catch-up/first-run download
  win.loadFile(path.join(__dirname, 'index.html'));
}

// ---- per-asset MOVE CHART window (opened from a table row). Loads chart.html with the symbol; the chart asks back
// for that symbol's 1-minute candles (get-candles) and recomputes the SAME dual-gate the table uses, so its move
// counts match exactly. ONE reusable window — clicking another asset reloads THIS window, never stacks a second. ----
let chartWin = null, lastChartInfo = null, lastDockedChartInfo = null;
function openChartWindow(p) {
  p = p || {};
  lastChartInfo = { row: p.row || null, pat: p.pat || null, tf: p.tf || null, zones: p.zones || null };   // pat = the Lifer pattern for the timeframe the chart is opening on   // the clicked/searched asset's table row, served to the chart window via get-chart-info (for the detail-page stats)   // zones = that asset's range-extreme/flip-zone list, from the Zones tab, drawn read-only on the chart
  const q = 'cls=' + encodeURIComponent(p.cls || '') + '&sym=' + encodeURIComponent(p.sym || '') + '&base=' + encodeURIComponent(p.base || 20) + '&minBase=' + encodeURIComponent(p.minBase || 5) + '&pf=' + encodeURIComponent(p.pf || 25) + '&thr=' + encodeURIComponent((p.thr || []).join(',')) + '&live=' + (p.live ? 1 : 0) + (p.tf ? '&tf=' + encodeURIComponent(p.tf) : '');   // tf is OPTIONAL — only Lifer sends it, so every other caller keeps opening on 1D
  const title = String(p.sym || '').toUpperCase() + ' — moves';
  if (chartWin && !chartWin.isDestroyed()) {                    // reuse it: retitle, reload with the new asset, bring to front
    chartWin.setTitle(title);
    chartWin.loadFile(path.join(__dirname, 'chart.html'), { search: q });
    if (chartWin.isMinimized()) chartWin.restore();
    chartWin.focus();
    return;
  }
  const cw = 1280, ch = 820;
  let cpos = {};   // open the chart centered OVER the main window (same monitor) instead of wherever the OS drops it
  if (win && !win.isDestroyed()) { const b = win.getBounds(); cpos = { x: Math.round(b.x + (b.width - cw) / 2), y: Math.round(b.y + (b.height - ch) / 2) }; }
  chartWin = new BrowserWindow({
    width: cw, height: ch, ...cpos, backgroundColor: '#131722',
    title: title, icon: path.join(__dirname, 'icon.ico'),
    webPreferences: { preload: path.join(__dirname, 'chart_preload.js'), contextIsolation: true, nodeIntegration: false }
  });
  chartWin.removeMenu();
  chartWin.on('closed', () => { chartWin = null; });
  chartWin.loadFile(path.join(__dirname, 'chart.html'), { search: q });
}

// ---- DOCKED chart panel, Zones tab only (Nate, 2026-09-25: "I dont want a chart that pops up externally. I want
// a chart thats up on the right side of the screen in the empty space"). A WebContentsView pinned to the right
// portion of the MAIN window's own content area, loading the SAME chart.html/chart_preload.js the popup uses --
// no chart logic duplicated. Every other tab keeps using openChartWindow() above, untouched; this is additive,
// not a replacement (Nate didn't ask to change how the other tabs open a chart, only the one he was just using).
// WIDTH is a FRACTION, not a synced pixel rect: the renderer reserves the SAME 42% via CSS so the table naturally
// leaves room, sidestepping the page's own zoomFactor entirely for that axis.
// HEIGHT/Y-OFFSET can't use the same trick: the header (#bar + #classbar, with Zones' own filter row) has no
// fixed height, so a hardcoded top offset is wrong on any tab whose header wraps differently. BUG FIX (Nate,
// 2026-09-26: "the chart covers up the top of the app and I cant see the settings") -- this used to hardcode
// y:0/height:full, so the docked view sat OVER the search box and "How this works"/"Screenshot" buttons on the
// right side of the header. Now the renderer measures its own header's real bottom edge (getBoundingClientRect,
// in the page's OWN zoomed-CSS-px space) and reports it via set-docked-chart-top; PAGE_ZOOM converts that into
// the window's physical-px space (setBounds() doesn't know about a child page's zoom, so this multiply is
// required here, unlike the width fraction above which never needed it).
const CHART_DOCK_FRAC = 0.42;
const PAGE_ZOOM = 1.1;   // MUST match win.webContents.setZoomFactor(1.1) in createWindow() below
let chartView = null;
let dockedChartTopCss = 0;   // header bottom edge, reported by the renderer, in ITS OWN css-px (zoomed) space
function layoutChartView() {
  if (!chartView || !win || win.isDestroyed()) return;
  const cb = win.getContentBounds();
  const w = Math.round(cb.width * CHART_DOCK_FRAC);
  const y = Math.round(dockedChartTopCss * PAGE_ZOOM);
  try { chartView.setBounds({ x: cb.width - w, y, width: w, height: Math.max(0, cb.height - y) }); } catch (e) {}
}
function openDockedChart(p) {
  p = p || {};
  if (!win || win.isDestroyed()) return false;
  lastDockedChartInfo = { row: p.row || null, pat: p.pat || null, tf: p.tf || null, zones: p.zones || null, fib: p.fib || null, windowStart: p.windowStart || null, windowEnd: p.windowEnd || null };   // SEPARATE from lastChartInfo (the popup's) -- a docked chart and a popup chart can be open at once (different tabs), and sharing one global would let one clobber the other's info between the loadFile below and that page's own get-*-chart-info call. windowStart/windowEnd = the exact 90-day extreme-detection window (Nate, 2026-09-26: chart must show both extremes by default -- anchoring to the WINDOW, not to a zone's own birth time, see chart.html)
  const q = 'cls=' + encodeURIComponent(p.cls || '') + '&sym=' + encodeURIComponent(p.sym || '') + '&base=' + encodeURIComponent(p.base || 20) + '&minBase=' + encodeURIComponent(p.minBase || 5) + '&pf=' + encodeURIComponent(p.pf || 25) + '&thr=' + encodeURIComponent((p.thr || []).join(',')) + '&live=' + (p.live ? 1 : 0) + (p.tf ? '&tf=' + encodeURIComponent(p.tf) : '') + (p.acls ? '&acls=' + encodeURIComponent(p.acls) : '') + '&docked=1';   // acls = the asset's REAL class; cls is the UI-mode flag ('zones'), which resolves to the wrong candle file for an aliased ticker
  if (!chartView) {
    chartView = new WebContentsView({ webPreferences: { preload: path.join(__dirname, 'chart_preload.js'), contextIsolation: true, nodeIntegration: false } });
    win.contentView.addChildView(chartView);
  }
  layoutChartView();
  chartView.webContents.loadFile(path.join(__dirname, 'chart.html'), { search: q });
  return true;
}
function closeDockedChart() {
  if (chartView && win && !win.isDestroyed()) { try { win.contentView.removeChildView(chartView); } catch (e) {} }
  // DESTROY IT, don't just detach (Nate, 2026-09-26: "fix them all"). Dropping the reference left the page ALIVE
  // with its own venue WebSocket open -- beforeunload never fires because nothing ever unloads it. leaveZones()
  // runs on every tab switch away from Zones and reopening always builds a fresh WebContentsView, so one orphaned
  // streaming page accumulated per open/close for the life of the process, and nothing reclaimed them.
  if (chartView) { try { chartView.webContents.close(); } catch (e) { try { chartView.webContents.destroy(); } catch (e2) {} } }
  chartView = null;
  return true;
}

const send = (ch, p) => { if (win && !win.isDestroyed()) win.webContents.send(ch, p); };
// TWO INDEPENDENT LANES. The tradfi sweep paces 123 Polygon stocks 12.5s apart (~26 min) plus 92 Dukascopy
// instruments; while it held the single shared lane EVERY crypto tick was dropped, so the EMA tab went dark for
// ~33 minutes an hour (measured from candle-file write gaps) and then dumped a batch of half-dead 5m signals.
// Crypto is keyless and fast, so it gets its own lane and runs regardless of what the slow rate-capped lane is
// doing. Both lanes write via writeAtomic and recompute() rereads every candle file from disk, so the later
// writer simply publishes the fresher table — last-writer-wins is safe here, never torn.
const lanes = { crypto: { busy:false, child:null, watchdog:null }, full: { busy:false, child:null, watchdog:null } };
function cleanupChild(L) {
  L.busy = false;
  if (L.watchdog) { clearTimeout(L.watchdog); L.watchdog = null; }
  if (L.child) { try { L.child.removeAllListeners(); } catch (e) {} try { L.child.kill(); } catch (e) {} L.child = null; }
}

// Spawn the engine in a background utilityProcess so its heavy work never blocks the UI thread. Fire-and-forget:
// progress/done/error come back as messages; the engine writes engine_data.json itself, so the data never crosses.
function runUpdate(trigger, only) {
  const L = (only && only.length === 1 && only[0] === 'crypto') ? lanes.crypto : lanes.full;   // crypto-only runs take the fast lane; anything unrestricted is a full sweep
  if (L.busy) return { busy: true };
  L.busy = true;
  const d0 = loadData();
  send('update-start', { trigger, firstRun: !(d0 && d0.classOrder && d0.classOrder.length) });   // a parseable-but-empty file is still "first run" (show the download screen)
  let settled = false;
  const finish = (payload) => { if (settled) return; settled = true; send('update-done', payload); cleanupChild(L); };   // single-finish latch: done/error/exit/timeout can never double-fire
  try {
    L.child = utilityProcess.fork(path.join(__dirname, 'engine_host.js'));
    L.child.on('spawn', () => L.child.postMessage({ type: 'start', dataDir: DATA_DIR, polygonKey: getKey(), enabled: getToggles(), only: only || null }));   // only = restrict THIS run to these source groups (e.g. ['crypto'] for the 5-min crypto refresh); null = full sweep
    L.child.on('message', (m) => {
      if (!m) return;
      if (m.type === 'progress') send('update-progress', m.payload);
      else if (m.type === 'partial') send('update-partial', m.payload);
      else if (m.type === 'done') finish({ ok: true, log: m.log });
      else if (m.type === 'error') finish({ ok: false, error: m.error });
    });
    L.child.on('exit', (code) => finish({ ok: false, error: 'engine process exited (code ' + code + ')' }));
    L.watchdog = setTimeout(() => finish({ ok: false, error: 'engine timed out' }), 3 * 3600 * 1000);   // 3h backstop: the UI can never stay frozen even if the child dies without an exit event
    return { ok: true };
  } catch (e) {
    finish({ ok: false, error: e.message });
    return { ok: false, error: e.message };
  }
}

ipcMain.handle('load-data', () => loadData());
// The alert ping. A desktop notification always; the webhook ONLY when the user has pasted a URL -- it is the
// app's first outbound call to a host that is not a data source, so it is empty by default and never inferred.
// DISCORD webhook format (Nate, 2026-09-26: "we will use discord. i prefer it to pushover.") -- this was
// previously posting plain text (Content-Type: text/plain), which Discord's webhook API rejects outright; it
// requires Content-Type: application/json with a {"content": "..."} body. This is the ONE shared low-level
// notify() every alert path calls (Zones, Jackpot armed, Lifer, the combined Alerts tab all use the same
// generic webhook field/IPC), so fixing it here fixes phone delivery for all of them, not just Zones.
// Discord messages cap at 2000 chars -- title+body combined and trimmed to fit, same as the desktop notification
// above is already trimmed to fit its own OS limits.
// r.ok is now actually checked (it wasn't before): a rejected/failed Discord post used to come back as "sent"
// regardless -- a silently-lost alert is exactly the failure mode this whole feature exists to prevent.
// desktop param (Nate, 2026-09-26: "I dont want the alerts coming from the app. only the discord."): an OPTIONAL
// 5th arg, {desktop:false}, suppresses the OS notification popup AND the taskbar flash -- both are "the app"
// grabbing attention, which is exactly what he wants gone; Discord keeps working underneath, untouched. Callers
// that don't pass it (Jackpot armed, Lifer, the combined Alerts tab) are UNCHANGED -- he only asked about Zones,
// which is the one caller now passing {desktop:false}.
ipcMain.handle('notify', async (e, title, body, hook, opts) => {
  const showDesktop = !(opts && opts.desktop === false);
  if (showDesktop) {
    try { if (Notification.isSupported()) new Notification({ title: String(title||'').slice(0,120), body: String(body||'').slice(0,1200) }).show(); } catch (err) {}
    try { if (win && !win.isDestroyed()) win.flashFrame(true); } catch (err) {}   // cleared again on focus (see createWindow)
  }
  const url = String(hook||'').trim();
  if (!/^https:\/\//i.test(url)) return { desktop:showDesktop, phone:'not configured' };   // https only, and only if he set one
  const content = (String(title||'')+'\n'+String(body||'')).slice(0, 2000);
  try {
    const r = await fetch(url, { method:'POST', body: JSON.stringify({ content }),
                       headers:{'Content-Type':'application/json'}, signal: AbortSignal.timeout(10000) });
    if (!r.ok) {
      let detail = ''; try { detail = (await r.text()).slice(0, 200); } catch (e2) {}
      return { desktop:showDesktop, phone:'failed: Discord HTTP '+r.status+(detail ? ' — '+detail : '') };
    }
    return { desktop:showDesktop, phone:'sent' };
  } catch (err) { return { desktop:showDesktop, phone:'failed: '+(err.message||'') }; }   // surfaced, never retried in a loop
});
ipcMain.handle('get-key', () => !!getKey());                 // only report whether a key is set, never the key
ipcMain.handle('set-key', (e, k) => setKey(k));
// Same lane split as launch and the hourly sweep: unrestricted meant the full lane fetched crypto on top of the crypto
// lane already doing it, duplicating the gap fetch every time the button was pressed. Both lanes are kicked so the
// button still means "refresh everything"; the crypto result does not touch the hourly clock, which is the slow lane's.
ipcMain.handle('refresh', () => { const r = runUpdate('manual', ['stock','duka']); const c = runUpdate('manual-crypto', ['crypto']); if (r && r.ok) lastFullMs = Date.now(); return (r && r.ok) ? r : (c || r); });   // a manual sweep resets the hourly clock so the timer cannot fire a second one right behind it — but ONLY if it actually started; a click into a busy lane is a no-op and must not eat the hour
ipcMain.handle('is-busy', () => lanes.crypto.busy || lanes.full.busy);
ipcMain.handle('get-toggles', () => getToggles());
ipcMain.handle('set-toggle', (e, g, on) => setToggle(g, on));
ipcMain.handle('open-chart', (e, p) => { openChartWindow(p || {}); return true; });
ipcMain.handle('open-docked-chart', (e, p) => openDockedChart(p || {}));
ipcMain.handle('close-docked-chart', () => closeDockedChart());
ipcMain.handle('get-docked-chart-info', () => lastDockedChartInfo);
ipcMain.handle('set-docked-chart-top', (e, px) => { dockedChartTopCss = Math.max(0, +px || 0); layoutChartView(); return true; });
ipcMain.handle('get-candles', (e, sym, cls) => {                             // a chart window asking for its symbol's 1-minute candles (ts ms->sec for lightweight-charts)
  // cls = the CLASS the symbol was clicked in. Without it an aliased ticker resolves to the other class's file:
  // CVX would draw Convex Finance under a Chevron row forever, which no amount of waiting fixes. Omitted (or
  // unknown) falls back to the bare symbol, which is correct for all 347 non-aliased instruments.
  // base + append log (see candles.js). Reading the base alone would silently stop at the last daily fold-in.
  // Same timestamp bound as engine.js's okBar. This copy is read-only (it feeds the chart), so an unbounded one only
  // plots a candle in the year 2286 rather than destroying anything -- but two okBars that disagree is exactly the
  // drift file_alias.js exists to prevent, and the next person to copy this one may not be read-only.
  try { const tHi = Date.now() + 86400000;
    const okBar = r => Array.isArray(r) && r.length>=6 && Number.isFinite(r[0]) && r[0]>=1262304000000 && r[0]<=tHi && Number.isFinite(r[1]) && r[1]>0 && Number.isFinite(r[2]) && r[2]>0 && Number.isFinite(r[3]) && r[3]>0 && Number.isFinite(r[4]) && r[4]>0 && Number.isFinite(r[5]) && r[5]>=0;
    const c = CS.read(DATA_DIR, dfile(cls, sym), okBar).candles.map(r => [Math.floor(r[0] / 1000), r[1], r[2], r[3], r[4], r[5] || 0]); return { sym, candles: c }; }
  catch (err) { return { sym, candles: [] }; }
});
ipcMain.handle('get-chart-info', () => lastChartInfo);
ipcMain.handle('get-version', () => app.getVersion());
// KuCoin's public websocket needs a token first: POST /api/v1/bullet-public, no API key, no signature. It must be
// fetched HERE rather than in the renderer -- a file:// page has no CORS path to api.kucoin.com, and KuCoin's edge
// resets connections that arrive without a browser-shaped User-Agent/Origin (measured: bare handshake -> ECONNRESET).
// The token is good for 24h; the renderer re-asks on every reconnect, which is cheap and self-healing.
ipcMain.handle('ku-token', async () => {
  try {
    const r = await fetch('https://api.kucoin.com/api/v1/bullet-public', { method:'POST',
      headers:{ 'User-Agent':'Mozilla/5.0', 'Origin':'https://www.kucoin.com', 'Content-Type':'application/json' },
      signal: AbortSignal.timeout(15000) });
    if (!r.ok) return null;
    const j = await r.json();
    const srv = j && j.data && j.data.instanceServers && j.data.instanceServers[0];
    if (!j || !j.data || !j.data.token || !srv) return null;
    return { token: j.data.token, endpoint: srv.endpoint, pingInterval: srv.pingInterval || 18000 };
  } catch (e) { return null; }
});   // the header's version tag -- read from package.json at runtime, never a literal in the renderer
// KuCoin FUTURES has its own public-websocket token (api-futures), same handshake rules as the spot one above.
ipcMain.handle('kuf-token', async () => {
  try {
    const r = await fetch('https://api-futures.kucoin.com/api/v1/bullet-public', { method:'POST',
      headers:{ 'User-Agent':'Mozilla/5.0', 'Origin':'https://futures.kucoin.com', 'Content-Type':'application/json' },
      signal: AbortSignal.timeout(15000) });
    if (!r.ok) return null;
    const j = await r.json();
    const srv = j && j.data && j.data.instanceServers && j.data.instanceServers[0];
    if (!j || !j.data || !j.data.token || !srv) return null;
    return { token: j.data.token, endpoint: srv.endpoint, pingInterval: srv.pingInterval || 18000 };
  } catch (e) { return null; }
});
// The chart window's live candle comes from the same perp contract the engine downloads (perp_routes.json).
let PERP_ROUTES = null;
ipcMain.handle('perp-route', (e, sym, cls) => {
  try {
    if (!PERP_ROUTES) { PERP_ROUTES = new Map(); const j = JSON.parse(fs.readFileSync(path.join(__dirname, 'perp_routes.json'), 'utf8'));
      for (const r of (j.routes || [])) PERP_ROUTES.set(r.cls + ':' + String(r.sym).toUpperCase(), { v:r.venue, p:r.perp, s:+r.scale||1, m:+r.mult||1 }); }
    return PERP_ROUTES.get(String(cls || 'crypto') + ':' + String(sym || '').toUpperCase()) || null;
  } catch (err) { return null; }
});

let updateTimer = null, lastFullMs = Date.now();
// Single-instance lock: a 2nd copy must NOT launch. Two engines writing engine_data.json at once (most likely when
// a user relaunches or updates without quitting the first) corrupts the JSON -> blank table. The 2nd copy quits;
// relaunching just refocuses the window that's already running.
if (!app.requestSingleInstanceLock()) {
  app.quit();
} else {
  app.on('second-instance', () => { if (!win) { createWindow(); } else { if (win.isMinimized()) win.restore(); win.focus(); } });   // relaunch REOPENS the window if it was closed (Mac), else just refocuses — never a silent no-op
  app.whenReady().then(() => {
    createWindow();
    // Per-source cadence: CRYPTO refreshes every MINUTE — the EMA tab reads 5m closes, so a 5-min poll cost a full
    // bar of lag before a cross could even be computed (measured: candles p50 ~8 min stale, table up to 5 min older
    // still). 125 incremental kline calls/min sits far under Binance data-api limits. STOCKS (Polygon, rate-capped) and
    // DUKASCOPY (throttle-prone) stay HOURLY, and the sweep is DUE-BASED (elapsed since the last one), NOT tick-counted:
    // a crypto run takes ~51-54 s against the 60s tick (measured 2026-09-13: 9 consecutive publishes 57.4-61.4 s apart,
    // mean 59.3, none dropped -- but only ~6 s of headroom, so the busy guard CAN still eat a tick). A tick-counted
    // "every 60th tick" therefore ALIASES — tick 60 is essentially never a free tick and the full sweep fired 0-6x/day
    // instead of 24 (simulated + confirmed). Due-based means a deferred sweep lands on the next free tick instead.
    // The launch run (in createWindow) is a full sweep; the busy-guard skips any tick that fires while a run is still going.
    // PHASED TO THE MINUTE, not a free-running interval. setInterval(…, 60000) fires at whatever offset the app
    // happened to become ready at, and then drifts: one launch was measured at :57.1 with +6.6 ms per tick (~10 s a
    // day), another at :25.6. That offset is a pure tax on the alert budget. The Lifer alert gate needs the newest
    // CONFIRMED bar to be less than one timeframe old at publish, and the lateness is (this offset) + (how long the
    // run takes) -- so a tick at :57 spends 57 of a 1-minute budget before any work starts, which is why a 1m alert
    // could not fire off-tab at all on the pre-pool build.
    // :03, not :00 -- tfCloses drops the bucket the newest bar sits in, so fetching before the exchange has opened
    // the next 1m bar would discard the bar that just closed and cost a full extra minute. A few seconds past the
    // boundary is enough for it to exist without spending the budget.
    const TICK_OFFSET_MS = 3000;
    const armTick = () => {
      let wait = TICK_OFFSET_MS - (Date.now() % 60000);
      if (wait <= 0) wait += 60000;                       // already past :03 this minute -> aim at the next one
      updateTimer = setTimeout(() => { armTick(); tick(); }, wait);   // re-arm FIRST: a throw in tick() must not stop the clock
    };
    const tick = () => {
      runUpdate('crypto-1m', ['crypto']);                                                  // fast lane, every tick, never blocked by the sweep
      if ((Date.now() - lastFullMs) >= 3600000) {                                          // slow lane, due-based (not tick-counted: ticks get dropped while a lane is busy, which aliased the sweep away)
        const r = runUpdate('hourly', ['stock','duka']);   // NOT null: crypto has its own lane now, and letting the sweep fetch crypto too meant both lanes writing the same 126 candle files
        if (r && r.ok) lastFullMs = Date.now();                                            // consume the hour only when the sweep ACTUALLY started — a busy lane OR a failed spawn must leave it due
      }
    };
    armTick();
  });
  app.on('activate', () => { if (BrowserWindow.getAllWindows().length === 0) createWindow(); });
}
app.on('window-all-closed', () => app.quit());   // single-window tool, no app menu: closing the window QUITS (no headless zombie on Mac); reopening = double-click start.command, which catches up the data gap on launch
app.on('before-quit', () => { if (updateTimer) { clearTimeout(updateTimer); updateTimer = null; } cleanupChild(lanes.crypto); cleanupChild(lanes.full); });   // stop the timer + don't leave the engine running
