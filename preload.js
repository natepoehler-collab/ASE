// Bridges the Node/main side to the page over IPC. (Electron file:// can't fetch() local files, and the
// data + key live in the app's user-data folder that main owns.)
const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('engine', {
  load: () => ipcRenderer.invoke('load-data'),         // -> parsed engine_data.json, or null on first run
  hasKey: () => ipcRenderer.invoke('get-key'),         // -> boolean (key set?) — never returns the key itself
  saveKey: (k) => ipcRenderer.invoke('set-key', k),
  refresh: () => ipcRenderer.invoke('refresh'),        // trigger an update now
  isBusy: () => ipcRenderer.invoke('is-busy'),
  getToggles: () => ipcRenderer.invoke('get-toggles'), // -> {crypto,duka,stock} booleans
  setToggle: (g, on) => ipcRenderer.invoke('set-toggle', g, on),
  openChart: (p) => ipcRenderer.invoke('open-chart', p),   // open the per-asset move chart window
  openDockedChart: (p) => ipcRenderer.invoke('open-docked-chart', p),   // Zones only: dock the chart to the right side of THIS window instead of a popup
  closeDockedChart: () => ipcRenderer.invoke('close-docked-chart'),
  setDockedChartTop: (px) => ipcRenderer.invoke('set-docked-chart-top', px),   // reports the header's real bottom edge (css px, this page's own zoom space) so the docked panel starts below it, not over it
  getCandles: (sym, cls) => ipcRenderer.invoke('get-candles', sym, cls),   // cls so an aliased ticker resolves to ITS class's file   // 1m candles for the Lifer tab's inline chart pane (same IPC the chart window uses)
  getVersion: () => ipcRenderer.invoke('get-version'),
  notify: (title, body, hook, opts) => ipcRenderer.invoke('notify', title, body, hook, opts),   // desktop ping, + an optional user-supplied webhook; opts:{desktop:false} suppresses the desktop ping, Discord/webhook still sends
  kufToken: () => ipcRenderer.invoke('kuf-token'),   // KuCoin FUTURES public-websocket token
  kuToken: () => ipcRenderer.invoke('ku-token'),   // KuCoin public-websocket token (fetched in main: no CORS from file://)   // the real app version for the header tag
  onStart: (cb) => ipcRenderer.on('update-start', (e, d) => cb(d)),
  onProgress: (cb) => ipcRenderer.on('update-progress', (e, d) => cb(d)),
  onPartial: (cb) => ipcRenderer.on('update-partial', (e, d) => cb(d)),   // a class finished -> table can refresh
  onDone: (cb) => ipcRenderer.on('update-done', (e, d) => cb(d)),
});
