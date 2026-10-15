// Bridge for the per-asset chart window: lets chart.html pull its symbol's 1-minute candles from the main process
// (Electron file:// can't fetch the user-data folder directly). The chart recomputes the SAME dual-gate as the table.
const { contextBridge, ipcRenderer } = require('electron');
contextBridge.exposeInMainWorld('chartAPI', {
  getCandles: (sym, cls) => ipcRenderer.invoke('get-candles', sym, cls),   // cls = the class the row was clicked in, so an aliased ticker resolves to ITS file (see file_alias.js)
  getChartInfo: () => ipcRenderer.invoke('get-chart-info'),
  getDockedChartInfo: () => ipcRenderer.invoke('get-docked-chart-info'),   // the docked (Zones, right-side-panel) variant reads its own info, kept separate so a popup and a docked chart open at once can't clobber each other's row/zones payload
  perpRoute: (sym, cls) => ipcRenderer.invoke('perp-route', sym, cls),   // the perp contract this asset's live candle streams from
  kufToken: () => ipcRenderer.invoke('kuf-token'),
});
