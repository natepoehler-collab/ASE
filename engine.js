// Asset Selection Engine — in-app update engine (runs in Electron's main process).
// Fetches incrementally from 3 sources (binance / Dukascopy / massive-Polygon) into the app's own data
// dir, recomputes the table, writes engine_data.json. First run (no data) pulls a trailing year per
// instrument; after that it pulls only the gap. Crypto + Dukascopy are keyless; US stocks need the user's
// massive key. Resolution is 1-minute candles (INTERVAL_MIN below) — matching all our calibration.
const fs = require("fs");
const path = require("path");
let getHistoricalRates; try { ({ getHistoricalRates } = require("dukascopy-node")); } catch (e) { getHistoricalRates = null; }

const INTERVAL_MIN = 1;                         // candle size in minutes — 1m (all calibration/testing is 1m)
const STEP = INTERVAL_MIN * 60000;
const YEAR = 365 * 86400000, DAY = 86400000;
const HOST = "https://data-api.binance.vision";
const KU_HOST = "https://api.kucoin.com";          // KuCoin serves REAL historical 1m (verified) for the ~25 pool coins Binance lacks. Its API differs from Binance (seconds, newest-first, [t,o,c,h,l,v]) -> see fetchKuCoin. (MEXC was rejected: its klines ignore startTime and return only recent bars.)
const sleep = ms => new Promise(r => setTimeout(r, ms));

// ---- instrument universe (same as the project's _recompute_all + _rolling_update) ----
// Top-100 by market cap (ex stablecoins/wrapped/exchange tokens/gold/junk/non-ASCII), each on Binance OR KuCoin —
// the two sources VERIFIED to serve real historical 1m. Locked 2026-06-18 via research/_lock_pool.cjs: every coin
// live-checked, dead listings (e.g. RAIN/BSV on KuCoin) dropped and the pool drawn deeper to net exactly 100.
const CRYPTO = ["btc","eth","xrp","sol","trx","hype","doge","zec","xlm","cc","ada","xmr","link","gram","lab","bch","hbar","ltc","sui","avax","shib","near","tao","wld","wlfi","uni","aster","ondo","dot","sky","morpho","icp","pepe","etc","aave","qnt","atom","algo","ena","render","kas","pol","vvv","jst","flr","fil","jup","bdx","xdc","apt","beat","arb","inj","pump","night","adi","dash","fet","trump","h","cake","kite","vet","pengu","aero","lit","bonk","virtual","lunc","spx","tia","sei","stx","ethfi","9bit","jto","crv","sun","pyth","ub","gno","xpl","nft","btt","xtz","2z","cfx","grass","zro","mon","gwei","zbcn","chz","floki","pendle","jasmy","ldo","tel","op","strk"];
const KUCOIN_CRYPTO = new Set(["hype","cc","xmr","gram","lab","kas","vvv","flr","bdx","xdc","beat","adi","h","aero","lit","spx","9bit","ub","nft","btt","grass","mon","gwei","zbcn","tel"]);   // the 25 fetched from KuCoin (Binance lacks them); all others use Binance
// exclusion sets for the STICKY auto-add (a coin entering the top-100 is added only if it's a real tradeable, not these):
const POOL_STABLE=new Set("usdt,usdc,dai,usde,fdusd,tusd,usds,pyusd,usd1,susds,gusd,usdd,frax,lusd,susd,usdo,usdtb,buidl,usdg,rlusd,usdf,bfusd,gho,usd0,ylds,stable,usyc,ustb,eutbl,jtrsy,jaaa,bcap,figr_heloc,usdy,m,u,eurc,eurs,ausd,crvusd,rusd,usdai,usdm,usda,ousg,eurcv,a7a5,usx,apxusd,onyc,apyusd,ustbl,reusd,satusd,fdit,safo,eursafo,frxusd,usdat,thbill,acred".split(","));
const POOL_WRAPPED=new Set("wbtc,weth,wsteth,weeth,wbeth,steth,reth,cbeth,meth,rseth,ezeth,wbnb,lbtc,solvbtc,cbbtc,bnsol,msol,jitosol,jupsol,oseth".split(","));
const POOL_EXCH=new Set("bnb,okb,leo,cro,bgb,kcs,gt,mx,ht,ftt,bmx,wbt,mnt,htx,nexo,dexe,btse".split(","));
const POOL_GOLD=new Set("xaut,paxg,kau,kag".split(","));
// ---- YUBIT IS THE UNIVERSE ----------------------------------------------------------------------------------
// Nate: "only things yubit lists." He trades on Yubit BY HAND, so an asset he cannot trade there is noise on the
// board. The pool was NEVER Yubit-derived before this: a hardcoded CRYPTO seed grown by updateMcap's CoinGecko
// top-165 auto-add, which is why 37 of 135 coins were not listed on Yubit. yubit_perps.json is the 502-symbol
// LinearPerpetual list read off yubit.com on 2026-09-13. Refreshing it is a MANUAL browser lookup -- their ToS
// forbids automated collection without prior written consent -- so this file is a snapshot, replaced by hand.
// If it is missing or unreadable the gate FAILS OPEN (old behaviour) rather than silently emptying the board.
let YUBIT=null;
function yubitSet(){
  if(YUBIT) return YUBIT;
  try{ const j=JSON.parse(fs.readFileSync(path.join(__dirname,"yubit_perps.json"),"utf8"));
       YUBIT=new Set((j.syms||[]).map(x=>String(x).toUpperCase())); }
  catch(e){ YUBIT=new Set(); }
  return YUBIT;
}
// Yubit lists the high-supply coins as 1000x contracts (SHIB -> 1000SHIB), so both forms count as listed.
const onYubit=s=>{ const Y=yubitSet(); if(!Y.size) return true; const u=String(s).toUpperCase(); return Y.has(u)||Y.has("1000"+u); };
const poolExcluded=s=>{const l=s.toLowerCase();return POOL_STABLE.has(l)||POOL_WRAPPED.has(l)||POOL_EXCH.has(l)||POOL_GOLD.has(l)||/^pc\d{5,}$/i.test(l);};
let kuActive = KUCOIN_CRYPTO;   // the routing set of KuCoin-sourced members; update() refreshes it from the live (sticky) membership each run
const PAIR_OVERRIDE = { rndr: "RENDERUSDT" };
const DUKA_MAP = {
  eurusd:"eurusd",usdjpy:"usdjpy",gbpusd:"gbpusd",audusd:"audusd",usdcad:"usdcad",usdchf:"usdchf",nzdusd:"nzdusd",
  usdcnh:"usdcnh",usdhkd:"usdhkd",usdsgd:"usdsgd",eurgbp:"eurgbp",eurjpy:"eurjpy",eurchf:"eurchf",euraud:"euraud",
  eurcad:"eurcad",eurnzd:"eurnzd",gbpjpy:"gbpjpy",gbpchf:"gbpchf",gbpcad:"gbpcad",gbpaud:"gbpaud",gbpnzd:"gbpnzd",
  audjpy:"audjpy",audcad:"audcad",audchf:"audchf",audnzd:"audnzd",cadjpy:"cadjpy",cadchf:"cadchf",chfjpy:"chfjpy",
  nzdjpy:"nzdjpy",nzdcad:"nzdcad",nzdchf:"nzdchf",
  // EM / exotic forex added 2026-06-19 (Dukascopy ids = lowercase sym; all verified to serve 1m data):
  usdtry:"usdtry",usdzar:"usdzar",usdmxn:"usdmxn",usdnok:"usdnok",usdsek:"usdsek",usdpln:"usdpln",usdhuf:"usdhuf",usdczk:"usdczk",usdils:"usdils",usdthb:"usdthb",usdron:"usdron",
  eurtry:"eurtry",eurzar:"eurzar",eurmxn:"eurmxn",eurnok:"eurnok",eursek:"eursek",eurpln:"eurpln",eurhuf:"eurhuf",eurczk:"eurczk",
  tryjpy:"tryjpy",zarjpy:"zarjpy",mxnjpy:"mxnjpy",
  audsgd:"audsgd",nzdsgd:"nzdsgd",eursgd:"eursgd",chfsgd:"chfsgd",cadhkd:"cadhkd",eurhkd:"eurhkd",hkdjpy:"hkdjpy",sgdjpy:"sgdjpy",
  audpln:"audpln",cadpln:"cadpln",nzdpln:"nzdpln",chfpln:"chfpln",
  xauusd:"xauusd",xagusd:"xagusd",ukousd:"brentcmdusd",usousd:"lightcmdusd",
  us30:"usa30idxusd",sp500:"usa500idxusd",nas100:"usatechidxusd",ger30:"deuidxeur",fra40:"fraidxeur",uk100:"gbridxgbp",
  jpn225:"jpnidxjpy",eu50:"eusidxeur",hk50:"hkgidxhkd",aus200:"ausidxaud",sugar:"sugarcmdusd",ojuice:"ojuicecmdusx",
  gasoil:"dieselcmdusd",coffee:"coffeecmdusx",cocoa:"cocoacmdusd",cotton:"cottoncmdusx",soybean:"soybeancmdusx",ng:"gascmdusd",
  xcuusd:"coppercmdusd",xpdusd:"xpdcmdusd",xptusd:"xptcmdusd",gaucnh:"xaucnh",   // metals: copper / palladium / platinum (Duka commodity CFDs — single USD CFD each, no crosses)
};
// Symbols that LIVE in a duka-group class (so they show under that tab) but whose DATA comes from Binance, keyless.
// The two gold tokens PAXG/XAUT trade on Binance (PAXGUSDT/XAUTUSDT via fetchCrypto's default pairing), NOT Dukascopy,
// so the update loop routes them through the Binance fetcher even though their class group is "duka".
const BINANCE_IN_DUKA = { paxg:1, xaut:1 };
// "classKey:sym" for every instrument an actual fetch returned NOTHING for while it still has no candle file.
// Written by the download loop, read by recompute. Module-level because both run in the same utilityProcess.
// A symbol is deleted from it the instant it produces anything, so the mark can never outlive the condition.
const NO_BARS = new Set();
const ATTEMPTED = new Set();   // "classKey:sym" this process actually tried to download (see noSrc in recompute)
let PERP_ERRS = null;          // this minute-lane run's perp refusals ["class SYM: message"], published as out.perpErrors (null = this lane does not own the perps)
// US stocks + the per-class config come from yubit_classes.json (bundled next to this file)
const FULL = [5,6,7,8,9,10,11,12,15,20];

// ---- metric helpers (verbatim from _recompute_all.mjs; resolution-agnostic — they use timestamps) ----
function clean(B){ const C=B.map(r=>r.slice()); for(let i=1;i<C.length-1;i++){const o=C[i][1],h=C[i][2],l=C[i][3],c=C[i][4],pc=B[i-1][4],nc=B[i+1][4];const bh=Math.max(o,c),bl=Math.min(o,c);if(l>0&&l<0.5*Math.min(bl,pc,nc))C[i][3]=bl;else if(h>2*Math.max(bh,pc,nc))C[i][2]=bh;}
  // FIRST bar (the loop above skips i=0): a LISTING PRINT routinely stamps a stub open+low on the very first bar
  // (e.g. LAB opened 0.01 then immediately traded ~0.09). The normal body-low fix fails here because the OPEN is
  // itself the stub, so judge bar 0 against its CLOSE + the next bar and clamp a wild low up / wild high down to
  // the close. Otherwise the ZigZag seeds its first pivot off the stub and invents a huge phantom opening leg
  // (LAB: a fake 0.01->0.09 first leg inflating tpct by ~1300%). Only fires on a genuine first-bar outlier.
  if(C.length>1){ const c0=C[0][4], n1=C[1];
    if(c0>0 && C[0][3]>0 && C[0][3] < 0.5*Math.min(c0, n1[3]>0?n1[3]:c0)) C[0][3]=Math.min(c0, C[0][2]);   // stub low  -> close (capped at the bar's own high)
    if(c0>0 && C[0][2] > 2*Math.max(c0, n1[2]))                           C[0][2]=Math.max(c0, C[0][3]);   // spike high -> close (floored at the bar's low)
  }
  return C; }
// ---- THE MOVE RULE (Nate's dual-gate, verified on the chart). A move ends when price pulls back BOTH >= `retrace`
// (the PULLBACK FILTER: 0.25/0.5/0.75/0.9 — "how big a pullback you'll sit through before calling the move over") AND
// >= `floor` (the class MIN BASE: 0.05 crypto/stocks, 0.03 metals... — so a sub-min-base wiggle can't end a move and a
// real grind survives whole). The move's end is the extreme (the high if it was rising, the low if falling). NO speed
// filter. Returns pivots; a swing between consecutive pivots is a COUNTED MOVE if |%| >= the move size (caller-applied).
function retraceZig(A, retrace, floor){
  const piv=[{p:A[0][4],t:A[0][0],i:0}]; let dir=0, anchorP=A[0][4];
  let hi=A[0][2], hiI=0, lo=A[0][3], loI=0, extP=A[0][4], extI=0;
  for(let i=1;i<A.length;i++){
    const h=A[i][2], l=A[i][3];
    if(dir>=0 && h>hi){ hi=h; hiI=i; } if(dir<=0 && l<lo){ lo=l; loI=i; }
    if(dir===0){ if((hi-anchorP)/anchorP>=floor){ dir=1; extP=hi; extI=hiI; lo=l; loI=i; } else if((anchorP-lo)/anchorP>=floor){ dir=-1; extP=lo; extI=loI; hi=h; hiI=i; } }
    if(dir===1){ if(h>extP){ extP=h; extI=i; lo=l; loI=i; } else if(l<lo){ lo=l; loI=i; } const retr=(extP-lo)/(extP-anchorP), pull=(extP-lo)/extP; if(retr>=retrace && pull>=floor){ piv.push({p:extP,t:A[extI][0],i:extI}); anchorP=extP; dir=-1; extP=lo; extI=loI; hi=h; hiI=i; } }
    else if(dir===-1){ if(l<extP){ extP=l; extI=i; hi=h; hiI=i; } else if(h>hi){ hi=h; hiI=i; } const retr=(hi-extP)/(anchorP-extP), pull=(hi-extP)/extP; if(retr>=retrace && pull>=floor){ piv.push({p:extP,t:A[extI][0],i:extI}); anchorP=extP; dir=1; extP=hi; extI=hiI; lo=l; loI=i; } }
  }
  if(dir!==0 && extI!==piv[piv.length-1].i) piv.push({p:extP,t:A[extI][0],i:extI});
  return piv;
}
// 882 tab (Nate, 2026-09-30). See the F882 block in computeMetrics for why each of these is what it is.
const F882_WIN_1M = Math.round(0.75*1440);   // 0.75 day of 1m bars -> 216 closed 5m bars
const F882_RETRACE = 0.96, F882_FLOOR = 0.001, F882_MIN_LEG = 0.20;   // Nate, 2026-10-01: "lets expand the leg size to 20%" (was 0.05)
const PFS = [25,50,75,90];        // the PULLBACK FILTER ladder shared by the app + chart (% retrace that ends a move)
const DEFAULT_PF = 25;            // the table's default pullback filter (matches the chart's default; a button changes it)
// A bar is usable only if its timestamp + OHLC are finite and prices are > 0. A 0/negative/NaN price would
// blow up the ZigZag division ((h-pL)/pL etc.) -> Infinity/NaN -> null/wrong metrics. Volume may be 0 but
// must be finite. Drop anything else at the load points so a bad bar can never reach the math, cache, or file.
// A BAR'S TIMESTAMP MUST BE A PLAUSIBLE MILLISECOND TIME, and leaving that unchecked was a way to destroy a year of
// data. compact() anchors its trailing-year trim on the LAST bar's timestamp, so a single parseable log line with a
// garbage-large t passed this test, became the anchor, and the trim deleted everything behind it: 525,601 bars -> 1,
// returning {ok:true}. Unrecoverable, too -- the next fetch starts from that future lastT, asks for nothing, and
// reports ok forever, while recompute falls back to the bundled seed row with nothing on screen to say so.
// Bounds: after 2010-01-01 (before any instrument here has data) and no more than a day ahead of now, which allows
// a forming bar and clock skew but not a corrupted one.
const T_MIN = 1262304000000;                 // 2010-01-01T00:00:00Z
const T_SLACK = 86400000;                    // a day of headroom for a forming bar / clock skew
// The ceiling is refreshed every 1024 calls rather than read from the clock on every bar. okBar runs per-bar over
// ~525k bars per asset, and calling Date.now() that many times took it from 13.3 ms to 47.2 ms and CS.read from
// 80.3 ms to 125.3 ms -- i.e. the check cost more than the binary sidecar saves, which would have quietly undone
// the whole point of it. The bound has a day of slack, so a ceiling up to 1024 bars stale changes no verdict.
let _tHi = 0, _tN = 0;
const tHi = () => { if ((_tN++ & 1023) === 0) _tHi = Date.now() + T_SLACK; return _tHi; };
const okBar = r => Array.isArray(r) && r.length>=6 && Number.isFinite(r[0]) && r[0]>=T_MIN && r[0]<=tHi() && Number.isFinite(r[1]) && r[1]>0 && Number.isFinite(r[2]) && r[2]>0 && Number.isFinite(r[3]) && r[3]>0 && Number.isFinite(r[4]) && r[4]>0 && Number.isFinite(r[5]) && r[5]>=0;

// ---- source fetchers (interval-aware; return new bars with t > lastT) ----
async function binanceKlines(pair, start, host=HOST, tries=0){ const url=`${host}/api/v3/klines?symbol=${pair}&interval=${INTERVAL_MIN}m&startTime=${start}&limit=1000`; try{const r=await fetch(url,{headers:{Accept:"application/json"},signal:AbortSignal.timeout(60000)}); if(r.status===429||r.status>=500)throw new Error("HTTP "+r.status); if(!r.ok)return null; return await r.json();}catch(e){if(tries>=6)throw e;await sleep(400*2**tries);return binanceKlines(pair,start,host,tries+1);} }   // 60s timeout: a hung request aborts -> throws -> retries (no more indefinite stall)
async function fetchCrypto(sym, lastT, now){ const pair=PAIR_OVERRIDE[sym]||(sym.toUpperCase()+"USDT"); const out=[]; let cur=lastT+STEP; while(cur<now){const rows=await binanceKlines(pair,cur);if(!rows||!rows.length)break;for(const k of rows){const t=+k[0];if(t>lastT&&t+STEP<=now)out.push([t,+k[1],+k[2],+k[3],+k[4],+k[5]]);}const last=+rows[rows.length-1][0];if(last<cur)break;cur=last+STEP;await sleep(40);} return out; }
// KuCoin historical 1m for every symbol in kuActive (NOT just "the coins Binance lacks" -- see the ku note in
// recompute). KuCoin's API differs from Binance: candles come back NEWEST-FIRST,
// max ~1500/response, time in SECONDS, fields [t,open,close,high,low,vol,turnover] (note close/high/low order differs).
// So pull in 1-day windows (1440 bars, under the cap) walking forward, remapping each bar to the engine's [t,o,h,l,c,v].
async function kucoinCandles(sym, startSec, endSec, tries=0){
  const url=`${KU_HOST}/api/v1/market/candles?type=1min&symbol=${sym.toUpperCase()}-USDT&startAt=${startSec}&endAt=${endSec}`;
  try{ const r=await fetch(url,{headers:{"User-Agent":"Mozilla/5.0"},signal:AbortSignal.timeout(60000)}); if(r.status===429||r.status>=500)throw new Error("HTTP "+r.status); if(!r.ok)return null; const j=await r.json(); return (j&&j.code==="200000"&&Array.isArray(j.data))?j.data:[]; }
  catch(e){ if(tries>=6)throw e; await sleep(500*2**tries); return kucoinCandles(sym,startSec,endSec,tries+1); }
}
async function fetchKuCoin(sym, lastT, now){
  const out=[]; const DAY_S=86400, nowSec=Math.floor(now/1000); let cs=Math.floor((lastT+STEP)/1000);
  while(cs < nowSec){
    const ce=Math.min(cs+DAY_S, nowSec);
    const rows=await kucoinCandles(sym, cs, ce);
    if(rows===null) break;                                                              // hard error after retries -> keep what we have, resume next run (like fetchCrypto)
    for(const k of rows){ const t=(+k[0])*1000; if(t>lastT && t+STEP<=now) out.push([t, +k[1], +k[3], +k[4], +k[2], +k[5]]); }   // KuCoin [t,open,close,high,low,vol] -> engine [t,open,high,low,close,vol]
    cs=ce; await sleep(120);                                                            // pace KuCoin between day-windows
  }
  return out;
}
// ---- PERPETUAL FUTURES (Nate, 2026-09-17: "I trade perpetual futures. spot isnt an accurate price for what I trade."
// "EVERYTHING, should be perpetual futures over spot. except maybe forex." "do 14 days. and have it be perps moving
// forward." Then: "move to perps. as many perp assets as you can do that yubit has." "leave forex be.")
// perp_routes.json (bundled, built by data/perp/build_routes.js) maps class:symbol -> the perp contract that IS that
// asset on one of four venues, each checked by price so a same-named different asset cannot slip in (the CVX contract
// is Convex Finance; Chevron is CVXSTOCK). An instrument with a route is fetched from its perp; one without keeps its
// old source. Existing files keep their spot/Polygon/Dukascopy history and CATCH UP with perp bars from their last bar
// (Nate: "catch up"); an instrument with NO file starts 14 days back.
//   Bitget USDT-FUTURES  /candles 1000 bars a request, oldest first, recent ~month only (MEASURED: empty at 40 days);
//                        /history-candles 200 a request for anything older. [t,o,h,l,c,baseVol,quoteVol]
//   KuCoin futures       /kline/query <=200 a request, [t,o,h,l,c,lots,turnover]; lots x multiplier = coins
//   OKX                  /history-candles 100 a request, NEWEST first, [t,o,h,l,c,contracts,baseVol,quoteVol,confirm]
//   MEXC                 /contract/kline <=2000 a window (the newest 2000 in it), columns; vol x contractSize = coins.
//                        MEASURED: empty at 40 days back, so a MEXC-routed file more than ~a month stale cannot fully catch up.
// A 1000x contract (1000BONKUSDT) quotes 1000 coins: price / scale, volume x scale. Closed bars only (t+STEP<=now).
// Each fetcher returns the bars plus .err (a venue or HTTP refusal: reported, never silent) and .scanT (the time up
// to which the venue has ANSWERED, bars or not -- see PERP_SCAN).
const PERP_DAYS = 14;
const BG_HOST = "https://api.bitget.com", KF_HOST = "https://api-futures.kucoin.com", OK_HOST = "https://www.okx.com", MX_HOST = "https://contract.mexc.com";
const PERP_VENUES = new Set(["bitget","kucoin","okx","mexc"]);
let PERP = null;
const unS = (x, sc) => sc===1 ? +x : +(x/sc).toPrecision(10);   // a scaled price without float noise (2.687e-6, not 2.6870000000000003e-6)
function perpRoutes(){
  if(PERP) return PERP;
  PERP = new Map();
  try{ const j=JSON.parse(fs.readFileSync(path.join(__dirname,"perp_routes.json"),"utf8"));
       for(const r of (j.routes||[])) if(r && r.perp && PERP_VENUES.has(r.venue))
         PERP.set(r.cls+":"+String(r.sym).toLowerCase(), { v:r.venue, p:r.perp, s:+r.scale||1, m:+r.mult||1 }); }
  catch(e){}                                                         // missing file -> no routes -> every instrument on its old source
  return PERP;
}
const perpOf = (classKey, sym) => perpRoutes().get(String(classKey)+":"+String(sym).toLowerCase()) || null;
// the Stoch Jackpot / stream key of a perp-routed tradfi instrument: its symbol, or its file alias when that symbol is
// also a crypto symbol (usstock CVX = Chevron -> CVX.US; crypto CVX = Convex Finance stays CVX)
const xKey = (classKey, sym) => isAliased(classKey, sym) ? dfile(classKey, sym).toUpperCase() : String(sym).toUpperCase();
// One shared pace per venue across every stream and pool in this process, under each venue's published public limit:
// Bitget candles 20/s per IP, OKX history-candles 20 per 2 s, MEXC contract market data 20 per 2 s. KuCoin futures
// has no per-endpoint figure published for kline/query; 8/s ran without a refusal on every test.
function rateGate(perSec){ let next=0; return async () => { const t=Date.now(), at=Math.max(t,next); next=at+1000/perSec; if(at>t) await sleep(at-t); }; }
const SLOT = { bitget: rateGate(18), kucoin: rateGate(8), okx: rateGate(9), okxRecent: rateGate(18), mexc: rateGate(9) };   // OKX /market/candles (recent) allows 40 per 2 s
// null = a refusal that retrying cannot fix (4xx): the caller reports it. 429/5xx/timeouts retry with backoff.
// ONE VENUE DOWN MUST NOT STALL THE LANE (review round 4: an OKX outage held the minute lane -- crypto included -- for
// ~13 min on 503s, hours on a hang). Per run: after 8 failed requests IN A ROW on a venue (a 429 "slow down" is NOT counted -- Nate, option A), every further request to it
// this run fails at once, is reported in perpErrors, and is asked again next run. Reset by update() each run.
const SLOT_V = new Map([[SLOT.bitget,"bitget"],[SLOT.kucoin,"kucoin"],[SLOT.okx,"okx"],[SLOT.okxRecent,"okx"],[SLOT.mexc,"mexc"]]);
let VENUE_FAILS = {}, VENUE_DOWN = new Set();
async function perpJson(url, slot, tries=0){
  const v = SLOT_V.get(slot);
  if(VENUE_DOWN.has(v)) throw new Error(v+" not answering -- skipped for the rest of this run");
  await slot();
  try{ const r=await fetch(url,{headers:{"User-Agent":"Mozilla/5.0",Accept:"application/json"},signal:AbortSignal.timeout(20000)});
       if(r.status===429||r.status>=500) throw new Error("HTTP "+r.status); VENUE_FAILS[v]=0; if(!r.ok) return { __http:r.status }; return await r.json(); }
  catch(e){
    if(e.message!=="HTTP 429") VENUE_FAILS[v]=(VENUE_FAILS[v]||0)+1;          // a rate-limit reply means the venue is UP
    if(VENUE_FAILS[v]>=8){ VENUE_DOWN.add(v); throw e; }
    if(tries>=6) throw e; await sleep(400*2**tries); return perpJson(url, slot, tries+1); }
}
const SCAN_MARGIN = 60 * STEP;  // a stretch counts as ANSWERED only once it is an hour old: a venue's brief outage or lag (an empty but successful reply) is asked again, never recorded as a permanent hole
const perpErr = (R, j) => R.v+" "+R.p+": "+(j && j.__http ? "HTTP "+j.__http : "refused ("+((j && (j.msg||j.code))||"no data")+")");
async function fetchBitgetPerp(R, lastT, now){
  const out=[]; let cur=Math.floor(lastT/STEP)*STEP+STEP;             // whole minutes: `from` can be an answered point, not a bar time
  out.scanT=0;
  while(cur+STEP<=now){
    // /candles only reaches back about a month; older windows go to /history-candles (200 a request)
    const old = cur < now - 28*DAY, span = old ? 199 : 999;   // MEASURED: /candles still answers 28 days back, nothing at 40
    const end=Math.min(cur+span*STEP, now);
    const url = old ? `${BG_HOST}/api/v2/mix/market/history-candles?symbol=${R.p}&productType=USDT-FUTURES&granularity=1m&startTime=${cur}&endTime=${end}&limit=200`
                    : `${BG_HOST}/api/v2/mix/market/candles?symbol=${R.p}&productType=USDT-FUTURES&granularity=1m&startTime=${cur}&endTime=${end}&limit=1000`;
    const j=await perpJson(url, SLOT.bitget);
    if(!j || j.code!=="00000" || !Array.isArray(j.data)){ out.err=perpErr(R,j); break; }   // keep what we have, report, resume next run
    const rows=j.data.slice().sort((x,y)=>(+x[0])-(+y[0]));          // oldest first, whatever order the venue sends
    for(const k of rows){ const t=+k[0], top=out.length?out[out.length-1][0]:lastT;
      if(t>top && t+STEP<=now) out.push([t, unS(k[1],R.s), unS(k[2],R.s), unS(k[3],R.s), unS(k[4],R.s), +k[5]*R.s]); }
    out.scanT=Math.floor(Math.min(end, now-SCAN_MARGIN)/STEP)*STEP;
    cur=end+STEP;
  }
  return out;
}
async function fetchKuCoinPerp(R, lastT, now){
  const out=[]; let cur=Math.floor(lastT/STEP)*STEP+STEP;             // whole minutes: `from` can be an answered point, not a bar time
  out.scanT=0;
  while(cur+STEP<=now){
    const end=Math.min(cur+199*STEP, now);                            // inclusive both ends: 200 minutes, never more than one page
    const j=await perpJson(`${KF_HOST}/api/v1/kline/query?symbol=${R.p}&granularity=1&from=${cur}&to=${end}`, SLOT.kucoin);
    if(!j || j.code!=="200000" || !Array.isArray(j.data)){ out.err=perpErr(R,j); break; }
    const rows=j.data.slice().sort((x,y)=>(+x[0])-(+y[0]));
    for(const k of rows){ const t=+k[0], top=out.length?out[out.length-1][0]:lastT;
      if(t>top && t+STEP<=now) out.push([t, unS(k[1],R.s), unS(k[2],R.s), unS(k[3],R.s), unS(k[4],R.s), +k[5]*R.m*R.s]); }
    out.scanT=Math.floor(Math.min(end, now-SCAN_MARGIN)/STEP)*STEP;
    cur=end+STEP;
  }
  return out;
}
async function fetchOkxPerp(R, lastT, now){
  // NEWEST first, 100 a page, paged backwards with `after` (= older than). Collect, then order oldest first.
  // A gap under 300 minutes -- every minute's run -- is one request to /market/candles (300 a page, the faster limit);
  // anything longer pages /history-candles.
  const got=[]; let after=now, err=null, done=false;
  const recent = now - lastT < 299*STEP;
  // ASK FOR THE GAP, NOT THE PAGE. This always sent limit=300 whatever was missing, so a caught-up instrument paid
  // 9,448 gzipped bytes a minute to collect one or two new bars -- 112 OKX routes x 1440 min = 1.52 GB/day, 91% of
  // the app's entire REST traffic, on a metered connection (Nate, 2026-10-01: "should be kbs per grab not gbs").
  // The other three venues already window their requests; only OKX pages by count, so only OKX needs this.
  // Sized from the gap with 2 bars of slack, clamped to OKX's own 1..300 -- never fewer bars than are missing, so
  // it cannot under-fetch; the paging loop below still walks further back if a page comes up short.
  const needBars = Math.max(2, Math.min(300, Math.ceil((now - lastT) / STEP) + 2));
  while(!done){
    const j=await perpJson(recent ? `${OK_HOST}/api/v5/market/candles?instId=${R.p}&bar=1m&after=${after}&limit=${needBars}`
                                  : `${OK_HOST}/api/v5/market/history-candles?instId=${R.p}&bar=1m&after=${after}&limit=100`, recent ? SLOT.okxRecent : SLOT.okx);
    if(!j || j.code!=="0" || !Array.isArray(j.data)){ err=perpErr(R,j); break; }
    if(!j.data.length) break;
    let oldest=Infinity;
    for(const k of j.data){ const t=+k[0]; if(t<oldest) oldest=t;
      if(t>lastT && t+STEP<=now && k[8]!=="0") got.push([t, unS(k[1],R.s), unS(k[2],R.s), unS(k[3],R.s), unS(k[4],R.s), +k[6]*R.s]); }   // k[8] "0" = not yet confirmed
    if(oldest<=lastT+STEP || !(oldest<after)) done=true; else after=oldest;
  }
  got.sort((a,b)=>a[0]-b[0]);
  const out=[]; for(const b of got) if(!out.length || b[0]>out[out.length-1][0]) out.push(b);
  if(err){ const none=[]; none.err=err; none.scanT=0; return none; }   // NEWEST-first paging: storing what arrived would jump the file past an unfetched hole that is never asked for again (review round 4). Store nothing; the whole gap is asked again next run.
  out.scanT=Math.floor((now-SCAN_MARGIN)/STEP)*STEP;              // the whole stretch lastT..now was answered
  return out;
}
async function fetchMexcPerp(R, lastT, now){
  const out=[]; let cur=Math.floor(lastT/STEP)*STEP+STEP;             // whole minutes: `from` can be an answered point, not a bar time
  out.scanT=0;
  while(cur+STEP<=now){
    const end=Math.min(cur+1999*STEP, now);                          // the venue returns the newest 2000 in a window: keep windows under that
    const j=await perpJson(`${MX_HOST}/api/v1/contract/kline/${R.p}?interval=Min1&start=${Math.floor(cur/1000)}&end=${Math.floor(end/1000)}`, SLOT.mexc);
    const d=j && j.data;
    if(!j || j.success!==true || !d || !Array.isArray(d.time)){ out.err=perpErr(R,j); break; }
    const ix=d.time.map((_,i)=>i).sort((a,b)=>d.time[a]-d.time[b]);       // oldest first, whatever order the venue sends
    for(const i of ix){ const t=d.time[i]*1000, top=out.length?out[out.length-1][0]:lastT;
      if(t>top && t+STEP<=now) out.push([t, unS(d.open[i],R.s), unS(d.high[i],R.s), unS(d.low[i],R.s), unS(d.close[i],R.s), +d.vol[i]*R.m*R.s]); }
    out.scanT=Math.floor(Math.min(end, now-SCAN_MARGIN)/STEP)*STEP;
    cur=end+STEP;
  }
  return out;
}
const PERP_FETCH = { bitget: fetchBitgetPerp, kucoin: fetchKuCoinPerp, okx: fetchOkxPerp, mexc: fetchMexcPerp };
// PERP_SCAN (perp_scan.json in the data folder): per candle file, the time up to which its venue has answered. A
// contract with no bars for a stretch -- a quiet or closed market, a paused listing -- never advances the file's last
// bar, so without this every minute re-requested the whole empty stretch (measured by review: up to 101 requests a
// minute for one instrument). Only the minute lane, which owns the perps, reads or writes it.
async function fetchStock(ticker, lastT, now, key){ if(!key)return []; const fromD=new Date(lastT).toISOString().slice(0,10),toD=new Date(now).toISOString().slice(0,10); let url=`https://api.polygon.io/v2/aggs/ticker/${ticker}/range/${INTERVAL_MIN}/minute/${fromD}/${toD}?adjusted=true&sort=asc&limit=50000&apiKey=${key}`; const out=[]; while(url){let j=null,status=0;for(let a=0;a<6;a++){const r=await fetch(url,{signal:AbortSignal.timeout(60000)});status=r.status;if(status===429){await sleep(20000);continue;}j=await r.json();break;}if(!j||(j.status!=="OK"&&j.status!=="DELAYED")){if(status===401)throw new Error("api-key-invalid");if(status===429)throw new Error("rate-limited");break;}if(j.results)for(const b of j.results)if(b.t>lastT&&b.t<now)out.push([b.t,b.o,b.h,b.l,b.c,b.v]);url=j.next_url?j.next_url+`&apiKey=${key}`:null;if(url)await sleep(13000);} return out; }
async function fetchDuka(id, lastT, now, tries=0){ if(!getHistoricalRates)return []; const tf="m"+INTERVAL_MIN;
  // STALL FIX: dukascopy-node has NO request timeout, so a hung Dukascopy file used to freeze the WHOLE download
  // forever (one stuck file, app idle, no progress until the 3h watchdog). Bound each instrument with a 5-min
  // ceiling we control (Promise.race — independent of the library's internals): a hang loses the race -> rejects ->
  // the existing 2x retry -> then the instrument is skipped (self-heals next run; recompute holds its seed row).
  // 5 min is generous vs a healthy first-run full-year pull (~2 min/instrument observed), so healthy fetches finish
  // well inside it and are never aborted.
  let timer; const ceiling = new Promise((_,rej)=>{ timer=setTimeout(()=>rej(new Error("duka-timeout-"+id)),300000); });
  try{ const d=await Promise.race([ getHistoricalRates({instrument:id,dates:{from:new Date(lastT+STEP),to:new Date(now)},timeframe:tf,format:"array"}), ceiling ]); clearTimeout(timer); return (d||[]).filter(r=>r[0]>lastT&&r[0]<now); }
  catch(e){ clearTimeout(timer); if(tries>=2)throw e; await sleep(800*2**tries); return fetchDuka(id,lastT,now,tries+1); } }

// ---- CROSS-CLASS SYMBOL COLLISION (Nate, 2026-09-11: "fix the cvx collision") -------------------------------
// The data dir is FLAT -- every instrument is <sym>.json -- and the metrics cache is keyed by that same bare
// symbol. So a ticker that appears in TWO classes shares BOTH the candle file and the cache entry, and whichever
// lane wrote last wins. CVX is Convex Finance in crypto and Chevron in stocks; the crypto lane runs every minute
// and the stock lane hourly, so crypto always won. Verified on the live app 2026-09-11: the Stocks CVX row and
// the Crypto CVX row were the IDENTICAL object -- price 2.041, btcCorr 69.6 (a crypto-only field, on a stock),
// span 365d over 525,601 continuous 24/7 bars, which no stock has. The shipped seed carries the same contamination
// (seed usstock CVX = price 2.26, btcCorr 69.6, identical to seed crypto CVX), so there is no clean row to fall
// back to anywhere.
// CVX is the ONLY collision in the pool -- verified across all 348 symbol slots in all seven classes, 347 distinct.
// Only the SECOND claimant is given a new name, so not one existing file moves and nothing is re-downloaded
// except the instrument that never had a file of its own. Crypto keeps cvx.json (a real year of Convex Finance);
// stocks get cvx.us.json, which Polygon fills on the next hourly sweep.
const { dfile, isAliased } = require("./file_alias.js");
const CS = require("./candles.js");
const POOL = (()=>{ try{ return require("./pool.js"); }catch(e){ return null; } })();   // null -> single-threaded, exactly as before   // candle storage: base file + append log (see candles.js for the why)   // shared with main.js, which must resolve chart candles the SAME way
// ---- one instrument: read file -> fetch gap (or full year first run) -> append -> trim -> write.
// Uses the metrics cache to skip the (expensive) full-file read when the last bar is already known, and skips
// the write entirely when the gap fetch returns nothing new (the file is then byte-for-byte unchanged). ----
async function updateInstrument(dataDir, sym, group, key, now, cache, fk, route, scan){
  cache = cache || {};
  fk = fk || sym;                                                     // the file + cache key, which is NOT the symbol for an aliased instrument
  const f = CS.basePath(dataDir, fk);
  const exists = fs.existsSync(f);
  const mt0 = CS.stamp(dataDir, fk);                                                   // covers the LOG too: an append no longer touches the base's mtime
  let store = null, lastT;
  if (exists && cache[fk] && cache[fk].mtime === mt0) { lastT = cache[fk].lastT; }      // last bar known w/o reading the file
  else {
    // A file that EXISTS but could not be READ must never be treated as "no history". The old code fell through
    // with candles=[] and lastT=now-YEAR; fetchCrypto/fetchKuCoin return a PARTIAL result on any non-retryable
    // response, so the write below replaced a full year with a sliver and returned status "ok". A transient
    // EACCES/EBUSY -- the very class writeAtomic already retries on these same files -- was enough to do it, and
    // lastT only ever moves forward, so the destroyed months were never re-fetched.
    // The guard that was supposed to cover this (the "reread-failed" abort below) only protects the CACHE-HIT
    // branch, where candles is genuinely null. This branch leaves it as [], so that check could not fire.
    // Abort instead: the next run re-reads, and an unreadable file stays visible in notFresh rather than being
    // silently rebuilt. It also saves a pointless full-year re-download on a metered connection.
    try { store = CS.read(dataDir, fk, okBar); }                                        // CS.read THROWS when the base exists but cannot be read
    catch(e){ return { status:"err", msg:"read-failed" }; }
    lastT = store.candles.length ? store.candles[store.candles.length-1][0] : (now - (route ? PERP_DAYS*DAY : YEAR));   // genuinely no file -> the trailing year (a perp: 14 days)
  }
  // HEAL A DAMAGED LOG FIRST. readLog stops at the hole, so the line count can never reach the fold threshold on
  // its own -- and doing this before the fetch means a run that returns nothing new still repairs the file.
  let lines = 0, logBad = false, recovered = false;
  if (exists) {
    if (store) { lines = store.logLines; logBad = !!store.logBad; }
    else { try { const L = CS.readLog(dataDir, fk, okBar); lines = L.lines; logBad = !!L.bad; } catch(e){} }
    if (logBad) {
      try { const c = CS.compact(dataDir, fk, sym, INTERVAL_MIN, YEAR, okBar);
            if (c.ok) { lines = 0; logBad = false; recovered = true; } } catch(e){}
    }
  }
  let fresh;
  // A perp resumes from its last bar -- the whole gap, however old (Nate: "catch up") -- but not from before the point
  // its venue has already answered (PERP_SCAN), so an empty stretch is asked about once, not every minute.
  const from = route ? Math.max(lastT, (scan && scan[fk]) || 0) : lastT;
  try {
    fresh = route ? await PERP_FETCH[route.v](route, from, now)
          : group==="crypto" ? (kuActive.has(sym) ? await fetchKuCoin(sym,lastT,now) : await fetchCrypto(sym,lastT,now))
          : group==="stock"  ? await fetchStock(sym.toUpperCase(),lastT,now,key)
          :                     await fetchDuka(DUKA_MAP[sym],lastT,now);
  } catch(e){ return { status:"err", msg:e.message }; }
  const perr = fresh.err || null;                                                           // a perp venue refused: store what arrived, then REPORT it
  // the answered point is recorded ONLY once what arrived is safely on disk (review round 4: recording it before a
  // failed write left a hole the scan said was answered, never fetched again)
  const scanT = fresh.scanT;
  const scanOk = () => { if (route && scan && scanT > ((scan[fk]) || 0)) { scan[fk] = scanT; scan.__dirty = true; } };
  const st = perr ? "err" : "ok";
  fresh = fresh.filter(okBar);                                                              // never store a malformed / zero / NaN bar
  if (!fresh.length) { scanOk(); return { status:st, msg:perr||undefined, added:0, bars:null, recovered:recovered||undefined }; }   // nothing new -> nothing written
  const add = fresh.filter(b => b[0] > lastT).sort((a,b)=>a[0]-b[0]);
  if (!add.length) { scanOk(); return { status:st, msg:perr||undefined, added:0, bars:null, recovered:recovered||undefined }; }
  // FIRST RUN (no base yet): write the base in one go. There is nothing to append to, and a year of bars does not
  // belong in a line-per-bar log.
  if (!exists) {
    let seed = (store ? store.candles : []).concat(add);     // store is non-null here: the cache-hit branch requires exists
    const sc = seed[seed.length-1][0] - YEAR; seed = seed.filter(c => c[0] >= sc);   // the same trailing-year trim compaction applies
    try { CS.writeBase(dataDir, fk, sym, INTERVAL_MIN, seed); }
    catch(e){ return { status:"err", msg:"write-failed: "+(e.code||e.message) }; }
    scanOk();
    return { status:st, msg:perr||undefined, added:add.length, bars:seed.length };
  }
  // APPEND -- the whole point. A few hundred bytes instead of the entire trailing year: this used to rewrite
  // 25-37 MB to add one to three bars, ~22 times an hour, for each of 133 crypto assets.
  try { lines = CS.append(dataDir, fk, add, lines); }
  catch(e){ return { status:"err", msg:"append-failed: "+(e.code||e.message) }; }           // a persistent lock skips THIS instrument, not the run
  scanOk();
  // Fold the log into the base once it holds a day of bars. That fold is also where the trailing-year trim happens
  // -- the old code ran that trim on every single write, which is exactly why every write was a full rewrite.
  let bars = null;
  if (lines >= CS.COMPACT_AFTER_BARS) {                     // the ordinary once-a-day fold (damage was already healed above)
    try { bars = CS.compact(dataDir, fk, sym, INTERVAL_MIN, YEAR, okBar).bars; }
    catch(e){ bars = null; }                                                               // the log is intact; it folds in on a later run
  }
  return { status:st, msg:perr||undefined, added:add.length, bars:bars, recovered:recovered||undefined };
}

// ---- Dukascopy resilient chunked pull. Replaces the old all-or-nothing duka fetch: pull the gap ~a month at a
// time and SAVE after every chunk, so a throttle/timeout keeps everything already fetched and the next run RESUMES
// from the last saved bar instead of re-pulling the whole year from zero (which, under Dukascopy throttling, never
// finished -> the instrument's file stayed empty forever). Paced (gap between chunks + gap between instruments in
// the loop) so a fresh install's ~53 full-year pulls don't fire back-to-back and trip the rate limit. ----
const DUKA_CHUNK_MS = 30 * DAY;          // ~a month per chunk: stays well under the per-fetch ceiling even when slow, and saves often
const DUKA_CHUNK_PAUSE = 1500;            // breathe between chunks (gentler on Dukascopy)
const DUKA_INSTRUMENT_PAUSE = 4000;       // gap between duka instruments (applied in the update loop)
async function updateDuka(dataDir, sym, now, cache, fk){
  const id = DUKA_MAP[sym];
  if (!getHistoricalRates || !id) return { status:"ok", added:0, bars:null };
  fk = fk || sym;
  const f = path.join(dataDir, fk + ".json");
  let candles = [];
  // Same hole as updateInstrument, and worse here because this function writes after EVERY chunk: an unreadable
  // file used to reset candles to [] and lastT to now-YEAR, and the first ~30-day chunk was then written over a
  // full year before any throttle could stop it. Abort on a read failure of an existing file.
  const hadBase = fs.existsSync(f);
  let logLines = 0;
  if (hadBase) { try { let st0 = CS.read(dataDir, fk, okBar);
                       if (st0.logBad) {                                        // heal first, then re-read the repaired file
                         try { CS.compact(dataDir, fk, sym, INTERVAL_MIN, YEAR, okBar); st0 = CS.read(dataDir, fk, okBar); }catch(e){}
                       }
                       candles = st0.candles; logLines = st0.logLines; }
                 catch(e){ return { status:"err", msg:"read-failed" }; } }
  let lastT = candles.length ? candles[candles.length-1][0] : (now - YEAR);   // resume from last saved bar; first run -> a trailing year
  let cur = lastT, added = 0, err = null;
  while (cur < now - STEP) {
    const end = Math.min(cur + DUKA_CHUNK_MS, now);
    let chunk;
    try { chunk = await fetchDuka(id, cur, end); }                   // each chunk keeps fetchDuka's own 5-min ceiling + retries
    catch(e) { err = e.message; break; }                            // throttle/timeout on THIS chunk -> stop, but every chunk before it is already saved
    chunk = (chunk || []).filter(okBar);
    if (chunk.length) {
      // APPEND the chunk. This used to merge the whole year, trim it and rewrite the entire file after EVERY
      // ~30-day chunk, so a first-run full-year pull meant a dozen full rewrites of a 20-30 MB file.
      const add = chunk.filter(b => b[0] > lastT).sort((a,b)=>a[0]-b[0]);
      if (add.length) {
        try {
          if (!hadBase && added === 0) CS.writeBase(dataDir, fk, sym, INTERVAL_MIN, add);   // no base yet -> seed it
          else logLines = CS.append(dataDir, fk, add, logLines);
        } catch(e){ return { status:"err", msg:"write-failed: "+(e.code||e.message), added }; }
        added += add.length;
        lastT = add[add.length-1][0];
        if (logLines >= CS.COMPACT_AFTER_BARS) { try { CS.compact(dataDir, fk, sym, INTERVAL_MIN, YEAR, okBar); logLines = 0; }catch(e){} }
      }
    }
    cur = end - STEP;                                                // advance with a 1-min overlap so a bar sitting exactly on a chunk boundary is never dropped (the Map dedups the overlap); still advances by ~a month, so it can't loop forever
    if (cur < now - STEP) await sleep(DUKA_CHUNK_PAUSE);
  }
  if (err) return { status: added > 0 ? "ok" : "err", msg: err, added };   // kept what we got; next run resumes from lastT
  return { status:"ok", added, bars: null };                              // the total is only known after a fold-in; the caller already tolerates null
}

// ---- metrics cache: skip re-crunching an instrument whose candle file hasn't changed since last time.
// Keyed by file mtime; stores the heavy file-derived metrics + the raw volume buckets. The volume
// direction/% is re-derived from the cached buckets every pass (it depends on the current time), so the
// numbers are IDENTICAL to a from-scratch compute — the cache only skips redundant work, never changes a value. ----
function sleepSync(ms){ try{ Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,ms); }catch(e){} }
// tmp+rename: an interrupted write can never leave a half-written/truncated file. The rename is retried on
// transient Windows locks (Defender/Search-indexer/a reader briefly holds the destination -> EPERM/EACCES/EBUSY).
function writeAtomic(file, data){
  const tmp=file + "." + process.pid + ".tmp"; fs.writeFileSync(tmp, data);
  for(let i=0;;i++){ try{ fs.renameSync(tmp,file); return; }catch(e){ if(i>=5 || !["EPERM","EACCES","EBUSY"].includes(e.code)) throw e; sleepSync(120*(i+1)); } }
}
const CACHE_VER = 117;   // v1.30.38: QUIET_WIN 288->144 (24h->12h) and QUIET_REF 4032->2016 (14d->7d) -- computeMetrics VALUES change, so every cached shard must be discarded or an asset on a slow file serves the old window forever. MEASURED on 96 assets x 90 days x 1.62M 5m buckets, base rate 0.75% for a +30% high within 24h: the 24h/14d/<=1.0 pairing shipped in 1.30.37 ran 72,611 signals at 0.51% precision = 0.68x lift, i.e. BELOW RANDOM, because a 1.0 ceiling against a 14-day norm barely filters. 12h/7d with ceiling 0.4 and surge 10x runs 2,749 signals at 1.64% = 2.19x lift. Recall falls 52%->27%; Nate: "id rather miss out on some if we can get our accuracy up." // v1.30.37: computeMetrics now returns `quiet` (volQuiet: the asset's last 24h of 5m volume over its own 14-day norm) and the Lifer row carries it as `q` -- output SHAPE changed, so every cached shard must be discarded or an asset on a slow-moving file would serve a quiet-less row forever and never be filterable on the new column. // v1.30.35: volSpike now ships b = {20,40,60} (the multiple at three baselines) and dir (the newest closed candle's own direction) for the Volume tab -- output SHAPE changed, so every cached shard must be discarded. Same trap as 1.30.21/1.30.12: a shard is only recomputed when its candle file moves, so without this bump any asset on a dead feed would serve a pre-1.30.35 sp with no b/dir forever and could never appear on the new tab. // v1.30.31: F882 now returns {c5} even when the leg is under the minimum (so a latched row keeps a price to be judged against its own frozen leg) -- output shape changed. // v1.30.29: F882 now carries c5 (the newest CLOSED 5m close) -- output SHAPE changed. // v1.30.28: F882_MIN_LEG 0.05 -> 0.20 (Nate: "lets expand the leg size to 20%") -- that changes computeMetrics' VALUES, not just its shape: every cached shard still holds a 5%-minimum leg, and a shard is only recomputed when its candle file moves, so without this bump an asset with a stalled feed would serve a 5% leg on a 20% board indefinitely. // v1.30.21: computeMetrics now returns F882 (the 0.75d/5m fib retracement leg for the 882 tab) -- output SHAPE changed, so every cached shard must be discarded or an asset whose candle file has stopped updating serves pre-882 metrics forever and never appears on the tab. Measured at the 1.30.21 build: 649 of 689 shards were _ver 110 with no F882 key; 529 of 533 would self-heal within a minute because their files move, but crypto BNC/SIREN/EWT and usstock BIDU (dead venue feeds, 206-284h stale) never would. Exactly the 1.30.12 omission described below. // v1.30.13: emaX is now computed for perp-routed NON-crypto too (the LIVEK gate, Nate 2026-09-26) -- computeMetrics' output shape changed, so a stale cache must be discarded. Missed on the 1.30.12 ship: 213 of 214 self-healed within a minute because perp files change constantly and that invalidates their entries anyway, but usstock BIDU's MEXC feed is DEAD, so its file never changes, its shard stayed a permanent cache hit at _ver 109 with emaX:null, and the fix could never reach it. // v1.30.0: PERPS -- a perp-routed stock/metal/oil/index decides "forming" by the clock and carries Lifer states (liferX); crypto unchanged in shape. //   // v3.33.0: every Lifer per-timeframe state ships sp = {x,n} -- the NEWEST CLOSED candle volume over the mean of the SPIKE_BASE before it, so a 1-2 candle spike is visible as itself instead of being averaged into a 20-bucket mean that cannot represent it. Output shape changed. //  v3.32.0: tfSeries/tfOHLC carry VOLUME in a 6th bucket slot, and each Lifer per-timeframe state ships vol = {r, br, n} - the mean volume of the last 20 closed buckets over the mean of the 100 before them, plus how many of the 20 individually beat that baseline. Output shape changed. // v3.31.1: the Day% CFD fallback now takes a real >=24h reference bounded to a 2-day span (NG was showing a 93h change, FRA40 60h) and blanks when the day cannot be identified. dayPct values change. // v3.31.0: a FLAT 14-bar RSI window no longer reports Stoch RSI %K = 100 (the top of the scale) - it reports nothing, so the cell blanks. 0/0 was being substituted with 100 direction-blind, which read maximally OVERBOUGHT on a straight collapse and on any frozen feed. Stoch values change. Also: Day % on CFDs rejects a session anchor older than 24h. // v3.30.1: the rolling-pnl payload is the MOVE over exactly 1h / 4h of the asset own bars (m1h / m4h), not a reference price for the renderer to divide a live price into - the old shape tied the window length to feed staleness, which a stalled feed made unbounded because recompute reuses cached metrics on mtime alone. Output shape changed. // v3.30.0: rows carry r1h / r4h = the close one hour and four hours back in the asset own 1m series, for the Lifer tab rolling-pnl column -- output shape changed. // v3.29.0: Lifer rows carry x200 (the time price last CROSSED the EMA200 on that timeframe) and rt200 (whether it has since retested and rejected off the 200) -- output shape changed. // v3.28.0: Lifer rows carry e21/e200/lastC at the bar dt stamps, so the renderer can advance the run and the bands from a live closed bar instead of waiting for the next recompute. Output shape changed. // v3.27.0: cross-class symbol collision fixed - the flat data dir and the metrics cache were BOTH keyed on the bare symbol, so crypto CVX (Convex Finance) and stock CVX (Chevron) shared one candle file and one cache entry and the minute-lane crypto write always won. Stocks now read/write cvx.us.json. // v3.26.0: 1m and 3m added to LIFER_TFS (nine timeframes, so every per-timeframe Lifer array changes length) AND the bucketed series is built ONCE per asset and shared by liferCross + liferPatterns instead of sixteen full passes over the candle file - the table rebuild WAS the 3-7 min alert lag, not the download. // v3.25.2: emaX.dt dropped again - it never reached the renderer and cache[sym].lastT already held the same value. EMA output shape is back to v3.24.0. // v3.25.1: the v3.25.0 data-time EMA expiry is REVERTED - it was the complement of a check emaState already guarantees, so it never fired, and it removed the only thing that could expire a row on a frozen feed. Wall clock restored; emaX still carries dt. // v3.25.0: emaX carries dt = the newest 1m bar the asset was computed from, and the EMA payload expiry asks the DATA rather than Date.now() (Nate: "fix the ema tab the same way"). Output shape changed. // v3.24.0: Lifer rows carry dt = the OPEN of the last CLOSED bar on that timeframe, so the live-stoch staleness gate is asked of the DATA rather than of the engine run time (Nate). Output shape changed. // v3.23.0: rows carry stochTail (Wilder state + RSI/raw/%K tails) so the renderer can advance the stoch by one bar on the live price. Output shape changed. // v3.22.0: the stoch SIDE is resolved in the engine with a tie continuing the previous side (Nate) - rows carry stochUp / stochWasUp. Output shape changed. // v3.21.0: stoch rows also carry stochPrev / stochD / stochDPrev so the column can show direction and a %K-through-%D cross. Output shape changed. // v3.20.0: Lifer rows carry stoch = Stoch RSI (14,14,3,3) %K per timeframe, 0-100. Output shape changed. // v3.19.0: the finer scan now covers the FORMING base bar (it was skipped, leaving up to a full bar of staleness); rows carry dist = the gap between EMA21 and EMA200 as a % of price. // v3.18.0: the REJECTION is found on finer candles (1m for 5m/15m/30m, 5m above) against the base timeframe's last confirmed bands - it was up to a full base bar late. Acceptance still lands on the base close. // v3.17.1: a row with NO run (side 0, sitting between the bands) can no longer report a rejection - the touch test was trivially true there. // v3.17.0: the band test reports its OUTCOME - rtOk true = rejected (the close held), false = accepted (the close went through and ended the run). Output shape changed. // v3.16.0: Lifer rows carry rt (RETEST: the wick came back to a band and the close held the side) - output shape changed, so a stale cache is discarded. // v3.15.0: replay 2000->300 (byte-identical on 98/98, ~4.5x faster); a broadening pair can no longer ship as a channel or a flag; most-touches counts CANDLES, not pivots. // v3.14.1: lpPara honours the wick-or-close anchor (it was rebuilding both lines from closes, discarding it on 12 of 14 rows); apexT no longer clamped to the last bar. // v3.14.0: touches use a STRICT tolerance separate from containment - one shared slack was letting lines with 0-1 real touches pass the gate. // v3.13.1: triangles-first is a preference within the structure found, not a walk that hunts for one; LP_MAXP 40 -> 16 (outcome identical at 8/12/16/24/40, cost 35s -> 1.1s). // v3.13.0: touch bug fixed (a close on the far side of the line counted as a touch); FLAG vs CHANNEL by flagpole, from the research; TRIANGLES FIRST, then flags, then channels (Nate). // v3.12.0: ONE line touched by wick AND/OR close in combination; most touches wins, ties to the most recent (Nate); containment on closes. // v3.11.0: boundaries may be fitted on WICKS or CANDLE BODIES (Nate's original rule, never implemented); containment is tested on each line's own basis. // v3.10.0: rows carry h0 (height at the pattern START = the measured move), brkPx and apexBars - output shape changed. // v3.9.0: CONTAINMENT IS REAL - the parallel copy is anchored on a pivot, not on the extreme bar, so containment can actually fail (it never could before: 277,279 of 277,279 parallel candidates held). // v3.8.2: v3.8.1 reverted (scored worse, cost 2.4x); replay bounded to the last 2000 bars, which is what makes the feature affordable. // v3.8.1: 3-and-2 assigned correctly for a channel - 3 touches on the FITTED trendline, 2 on the parallel copy. // v3.8.0: SHOW THE LIVE SHAPE (Nate) - the row reports the structure that can be drawn right now; the 12-candle hold applies only when there is no live shape. // v3.7.0: RIGHT TO LEFT, first structure that can be drawn (Nate); containment span fixed to where both lines exist; channels need 2-and-2, triangles 3-and-2. // v3.6.2: back to the largest holding shape (best of the three selection rules measured); span START remains unsolved. // v3.6.1: keep the BEST-FITTING holding shape (most touches), not the largest or the smallest - size is not the criterion. // v3.6.0: the SMALLEST holding shape wins (the most recent structure), not the largest - my spans ran 2-3x wider than the marks Nate drew. // v3.5.2: the v3.5.1 supersede relaxation is REVERTED - the hypothesis behind it was wrong (it did not recover ETH/DOGE; they are inside their 12-candle post-breakout window, which is Nate’s own rule). // v3.5.1: supersede re-picks the live shape (the stale-shape guard cost ETH and DOGE their triangles). // v3.5.0: A CHANNEL IS A TRENDLINE PLUS A PARALLEL COPY - scored 8 of 9 against the hand-labelled assets vs a ceiling of 3 of 9 for every two-independent-lines variant. // v3.4.3: touches are counted as PRICE approaching the line in distinct episodes (Bulkowski), not pivots lying exactly on it. // v3.4.2: at least ONE boundary must rest on its newest pivot (requiring it of both was over-strict and blanked most assets). // v3.4.1: each boundary must involve the MOST RECENT pivot on its side (Nate) — that is what removes the broadening-wedge white space. // v3.4.0: CONTAINMENT is the definition — two lines that contain price, 3 touches one / 2 the other (the research, restored), grown leftward from the most recent pivots until containment breaks. // v3.3.1: the line is anchored on the most recent pivot and EXTENDS LEFT through older pivots it still touches (Nate). // v3.3.0: THE MOST RECENT STRUCTURE — lines through the two most recent confirmed swing highs / lows. The hull and its edge-preference are deleted; still no width cap. // v3.2.0: NO WIDTH CAP — the LP_WINS window ladder (a de facto 90-bar limit) is gone; the hull grows over every confirmed pivot from the start of the data. // v3.1.3: supersede needs ONE newer point not four; pivot lookup binary-searched (9.6s -> under 1s per asset); apex epsilon; the 12-candle rebuild re-tests its own bar. // v3.1.2: the 50% span-overlap rule is REMOVED — it was mine, unasked, and it was rejecting the visible triangles (ZRO apex 5, ENA apex 13). // v3.1.1: the two boundaries must overlap (no more X shapes from disjoint tape), and NO shape outlives its apex, not just ones labelled triangle. // v3.1.0: boundaries are edges of the hull over the SWING PIVOTS (two real points per line), widest edge; the bar-hull constructions before it were a no-op and a forced wedge. // v3.0.2: the boundary is the hull EDGE nearest the current bar, not the window-endpoint chord (which was a no-op). // v3.0.1: the boundary is the hull chord (first to last hull point), not the widest interior edge. // v3.0.0: boundaries are convex-hull edges (the line no candle pokes through), not the line between two arbitrary pivots. // v2.3.0: Nate's two-outcome rule restored (LP_PARA's null branch gone), the tiebreak prefers the WIDEST shape not the newest, deaths rebuild on the same bar. // v2.2.1: the no-piercing rejection is gone — it was discarding visible patterns. // v2.2.0: the search pairs ANY two recent highs and any two recent lows, not only adjacent ones — that is how a trendline is actually drawn. // v2.1.1: the search no longer returns already-expired shapes, and each line is drawn from its own first pivot. // v2.1.0: a shape must LOOK like its name — diverging wedges are no longer shipped as channels. // v2.0.1: the two lines must cover the SAME stretch of tape (32.5% of patterns had non-overlapping pivot spans). // v2.0.0: back to the research's definition (3-and-2 touches + no piercing while forming); my degeneracy yardstick, rebuild guard and install-bar relabel deleted. // v1.9.6: the drawing clamp is reverted (it blanked 60 rows); est still reports the install bar. // v1.9.5: rows report and draw from the INSTALL bar rather than the pivot bars — that gap was the entire "stale display"; the birth guard added to chase it is reverted. // v1.9.4: after a death the replacement must use four strictly newer points, so stale geometry cannot be resurrected. // v1.9.3: the supersede bar is now re-tested for a breakout instead of being asserted inside. // v1.9.2: the pattern is re-picked whenever four newer points form a shape (Nate) — no longer frozen until a breakout. // v1.9.1: degenerate shapes rejected at birth (box may not exceed the range of the bars that formed it); the live respects-it ending removed. // v1.9.0: the 3-touch and no-piercing rules (mine, unapproved) removed; "price respects it" implemented as the ending for diverging channels. // v1.8.1: Trendy now ships all five candle counts (20/40/60/80/100) per timeframe, and dojis are out of the percent denominator. // v1.8.0: added the Trendy tab payload (red/green candle counts on 5m/10m/15m/30m) — computeMetrics output shape changed. // v1.7.1: a triangle ends at its apex unconditionally (no X-shaped rows), and touches are counted only within each line own span. // v1.7.0: a triangle ends at its apex (Nate); the live crossing rule I added unapproved is removed; containment no longer tested against extrapolated line tails. // v1.6.9: the live crossing test now uses an ATR band, not a fraction of the box height (which made it unfailable on a huge box). // v1.6.8: the crossing/white-space rule is now applied LIVE, not only at birth. // v1.6.7: a shape must now earn the name — Bulkowski's 3-touches-one-line/2-the-other plus no piercing while forming. // v1.6.6: swing pivot is now the STANDARD 2-candles-either-side (5-bar fractal), not 1. // v1.6.5: no width cap and no apex expiry — a pattern ends ONLY by breakout+12 candles or supersede, per Nate. // v1.6.4: Nate's literal triangle/channel rule restored (diverging allowed); the only rejection is a box out of scale with the trailing range, which also lets a widening channel end naturally. // v1.6.3: a shape must now PASS a test (triangle by the 48-candle apex, channel by near-parallel) and the search walks back to the most recent pivot pair that does — "channel" is no longer a leftover label. // v1.6.2: pattern lines are drawn through their OWN two pivots (no shared slope, no window-extreme offset) -- same shape, DIFFERENT VALUES, so a v1.6.1 cache must be discarded. // v1.6.1: 5m and 15m added to LIFER_TFS; inline Lifer chart pane — every per-timeframe Lifer array (st, pat) changes length. // v1.6.0: Lifer rows now carry pat[] (triangle / cloned-parallel channel + breakout state + height) per timeframe -- output SHAPE changed. // v1.5.1: Lifer pnl entry is now the CROSSING CANDLE'S CLOSE, not the EMA level cleared — same shape, DIFFERENT VALUES, so a v1.5.0 cache must be discarded. // v1.5.0: added crypto liferX (price vs EMA21/EMA200 -> long/short/inbetween on 30m/1h/2h/4h/1d) — computeMetrics output shape changed, so a stale cache is discarded and every instrument recomputes. 22:   // v1.4.14: 2h and 3h added to EMA_TFS — every per-timeframe array (st, pct, ref, s916, s1221) changes length. 21:   // v1.4.1: valid life 4x -> 8x the timeframe. 20:   // v1.4.0: 4h added back alongside 8h — seven timeframes, so every per-timeframe array changes length. 19:   // v1.3.9: a valid row now lives 4x its OWN timeframe instead of a flat hour. 18:   // v1.3.9: the 4h column/timeframe is now 8h (Nate: "lets do 8x timeframe. not 4x") — every shipped per-timeframe array changes meaning. 17:   // v1.3.6: emaX ships ref[] (the reference price each % window measures from) so the tab's % columns track the live price instead of freezing between recomputes. 16:   // v1.3.6: emaX now carries the pending/valid state for ALL SIX timeframes (st[]) so the tab's Timeframe Trigger is an instant switch — output shape changed. 15:   // v1.3.3: a valid row now lives 60 min, not 30 — changes which rows are on the list. 14:   // v1.3.2: a 9/16 cross no longer lists when the 12/21 already agrees without having crossed (that is a continuation, not a direction change) — changes which rows exist. 13:   // v1.3.0: emaX carries validPx (the 15m close when the trade went valid) for the EMA tab's status columns. 12:   // v1.3.0: emaX timestamps re-stamped from the 15m bucket OPEN to its CLOSE (a valid row was only living 15 min, not 30) — that changes computeMetrics VALUES, and recompute reuses cached metrics on mtime alone, so a cache written by the pre-fix build would serve the bug straight back. 11: added crypto emaX (9/16 + 12/21 cross state on 5m/15m/30m/1h/2h/3h/4h/8h/D + the 15m pending/valid list) — output shape changed, so a stale cache is discarded and every instrument recomputes.  // bump whenever computeMetrics' output shape OR calc OR per-class base/thr changes OR the CRYPTO pool changes (v1.2.24: added crypto btcCorr = % of 5m bars closing the SAME direction as BTC [size-agnostic, no threshold]; v1.2.23: forex 0.5% dropped → floor/base 1% + 34 EM forex added; v1.2.19: DUAL-GATE move rule — retraceZig per pullback filter [25/50/75/90], floor = class min base, NO speed filter, Oct-10 crash no longer skipped, + per-pf row.pf for the filter/move-size buttons; v1.2.12: crypto 59→100, Binance+KuCoin; v1.2.14: clean() de-stubs the FIRST bar so a listing print can't seed a phantom opening leg, AND avg gap fixed to notIn ÷ #gaps [was the wrong totalMin ÷ count]) -> a stale cache is discarded so every instrument is recomputed (v1.2.0 price/7d/participation; v1.2.5 volume buckets; v1.2.8 in-5%+ & 30d → leg-membership; v1.2.10 forex base 0.25→0.5 + dropped 0.25 col — bump forces upgraders off stale cached VALUES; also stamped as metricsVer so loadData shows the fresh seed first paint)
const CACHE_DIR="_cache";
const cacheDirOf = d => path.join(d, CACHE_DIR);
const shardOf = (d,fk) => path.join(cacheDirOf(d), encodeURIComponent(fk)+".json");
// mark(fk) records that this entry needs writing. Non-enumerable so JSON.stringify of the cache can never see it.
function markDirty(cache, fk){ try{ cache._dirty.add(fk); }catch(e){} }
function newCache(){ const c={_ver:CACHE_VER}; Object.defineProperty(c,"_dirty",{value:new Set(),enumerable:false,writable:true}); return c; }
function loadCache(dataDir){
  const c=newCache();
  const dir=cacheDirOf(dataDir);
  let loaded=0;
  try{
    for(const f of fs.readdirSync(dir)){
      if(!f.endsWith(".json")) continue;
      try{ const e=JSON.parse(fs.readFileSync(path.join(dir,f),"utf8"));
        if(e && e._ver===CACHE_VER && e.fk){ c[e.fk]=e.m ? {mtime:e.mtime,lastT:e.lastT,m:e.m,slowAt:e.slowAt} : null; if(c[e.fk]) loaded++; else delete c[e.fk]; }
      }catch(e2){}                                   // one unreadable shard costs that instrument a recompute, not the run
    }
  }catch(e){}
  if(loaded) return c;
  // MIGRATION: no shard dir yet (or all shards are a stale version) -> seed from the legacy single file, once. Every
  // entry is marked dirty so the first save writes the shards. _cache.json is left on disk untouched.
  try{ const old=JSON.parse(fs.readFileSync(path.join(dataDir,"_cache.json"),"utf8"));
    if(old && old._ver===CACHE_VER){ for(const k in old){ if(k==="_ver") continue; c[k]=old[k]; markDirty(c,k); } }
  }catch(e){}
  return c;
}
function saveCache(dataDir, cache){
  try{
    if(!cache) return;
    // No dirty set = a caller that built the cache itself (recompute is exported). We cannot know what changed, so
    // write everything rather than silently writing nothing.
    const all = !cache._dirty;
    const keys = all ? Object.keys(cache).filter(k=>k!=="_ver") : [...cache._dirty];
    if(!keys.length) return;                                       // nothing changed since the last save -> no write at all
    const dir=cacheDirOf(dataDir);
    try{ fs.mkdirSync(dir,{recursive:true}); }catch(e){}
    const written=[];
    for(const fk of keys){
      const e=cache[fk]; if(!e) continue;
      try{ writeAtomic(shardOf(dataDir,fk), JSON.stringify({_ver:CACHE_VER, fk:fk, mtime:e.mtime, lastT:e.lastT, slowAt:e.slowAt, m:e.m})); written.push(fk); }catch(e2){}
    }
    if(!all) for(const fk of written) cache._dirty.delete(fk);      // a shard that FAILED stays dirty and is retried
  }catch(e){}
}

// Volume SPEEDOMETER position from the cached per-class buckets + current time. volPos in [-1,+1] = the most recent
// COMPLETE bucket ("current" = last update's worth — crypto 5-min, stocks/forex 1-hr) as a PERCENT above/below the
// trailing-24h AVERAGE: 0 = at average (needle straight up); +1 (full right, green) = +100%, i.e. >=2x the average;
// -1 (full left, red) = -100%, i.e. ~zero volume. Clamped to +-100% so a bigger surge just pins it — the renderer
// then shows DH (pinned >= +100%, "one step further") / DL (pinned <= -100%). Only existing buckets count; if the
// latest complete bucket is >4 widths stale (market closed) the gauge blanks. null -> no gauge.
function deriveVol(vb, vbW){
  if(!vb || !vbW) return { volPos:null };
  const nowBk = Math.floor(Date.now()/vbW), span = Math.round(24*3600000/vbW);
  const keys = Object.keys(vb).map(Number).filter(b => b<nowBk && b>=nowBk-span).sort((a,b)=>a-b);   // complete buckets in the trailing 24h (exclude the in-progress current bucket)
  if(keys.length < 3) return { volPos:null };
  const last = keys[keys.length-1];
  if(nowBk - last > 4) return { volPos:null };                                                       // latest complete bucket too old -> market closed -> no gauge
  const vals = keys.map(b=>vb[b]), current = vb[last], avg = vals.reduce((a,b)=>a+b,0)/vals.length;
  if(!(avg>0)) return { volPos:null };
  const volPos = (current-avg)/avg;                                                                   // % above/below the rolling-day average (+1 = 2x avg = full right, -1 = zero = full left)
  return { volPos: +Math.max(-1,Math.min(1,volPos)).toFixed(3) };                                      // clamp to +-100%; renderer flags DH at >=+100% (>=2x avg), DL at <=-100%
}

// ---- daily % change, per-class to match each exchange's own convention: crypto = 24h rolling (Binance's
// priceChangePercent — verified to match within refresh lag); US stocks = vs the prior trading day's 16:00-ET
// REGULAR close (stock data here is extended-hours 4am-8pm ET, so we land the 4pm bar, not the after-hours one);
// forex = vs the prior 22:00-UTC close (Dukascopy's daily close, DST-free); indices/commodities/metals/oil =
// vs the last bar before their most recent daily gap (those CFDs gap ~20:00-22:00Z at their daily settlement). ----
const ET_FMT = new Intl.DateTimeFormat("en-US",{timeZone:"America/New_York",year:"numeric",month:"2-digit",day:"2-digit",hour:"2-digit",minute:"2-digit",hour12:false});
function etParts(t){ const o={}; for(const x of ET_FMT.formatToParts(t)) o[x.type]=x.value; let h=+o.hour; if(h===24)h=0; return { date:o.year+"-"+o.month+"-"+o.day, mins:h*60+(+o.minute) }; }
function dayChangePct(B, key){
  const n=B.length; if(n<2) return null;
  const lastT=B[n-1][0], lastC=B[n-1][4]; let ref=null;
  if(key==="crypto"){ const cut=lastT-DAY; let i=n-1; while(i>0&&B[i][0]>=cut)i--; ref=B[Math.min(i+1,n-1)][1]; }                              // 24h rolling: open of the bar ~24h ago
  else if(key==="usstock"){ const d=etParts(lastT).date; let i=n-1; while(i>=0&&etParts(B[i][0]).date===d)i--; while(i>=0&&etParts(B[i][0]).mins>960)i--; if(i>=0)ref=B[i][4]; }   // prior day's 16:00-ET (960min) regular close
  else if(key==="forex"){ const d=new Date(lastT); let bt=Date.UTC(d.getUTCFullYear(),d.getUTCMonth(),d.getUTCDate(),22,0,0); if(bt>=lastT)bt-=DAY; let i=n-1; while(i>=0&&B[i][0]>bt)i--; if(i>=0)ref=B[i][4]; }   // prior 22:00-UTC close
  // CFDs: the last bar before the most recent daily gap -- but the gap that wins must be recent enough to be a
  // DAILY boundary. With no bound, a missing settlement break in the Dukascopy file let the walk run to the
  // weekend and label a multi-day change as "Day %": measured NG 93.0h, FRA40 60.0h, GASOIL and COFFEE 48.0h.
  // Reject a reference older than 24h and fall through to the 24h rolling below, which is the right answer for
  // exactly that case. (Still unfixed, and reported: a recurring MID-SESSION hole younger than 24h can still win
  // -- SOYBEAN's 47-min hole at 12:44Z beats its real 882-min session break, so its Day % is a ~5.6h change.
  // Separating a settlement gap from a mid-session hole needs a session calendar, which this does not have.)
  else { let i=n-1; while(i>0){ if((B[i][0]-B[i-1][0])/60000>30){ if(lastT-B[i-1][0]<=DAY) ref=B[i-1][4]; break; } i--; }
    if(ref==null){ const cut=lastT-DAY; let j=n-1; while(j>0&&B[j][0]>cut)j--;
      // The reference must be at or before the 24h mark AND close enough to it to BE a day boundary. NG has a
      // 78-hour hole before its final block, so the nearest bar at/before the mark is 93h old -- only ~7h of its
      // last 24h has any data and there is no day change to compute. The one bound chosen here: the boundary must
      // be identifiable to within a day (span <= 2 days), otherwise the cell is BLANK. That also blanks a CFD that
      // has not traded since Friday, which is the honest reading of "Day %" on a closed market.
      ref=(B[j][0]<=cut && lastT-B[j][0]<=2*DAY) ? B[j][4] : null; } }                                                 // a real >=24h reference; no bar that old (asset younger than a day, or a long closure) -> blank rather than a sub-hour change wearing a day label
  if(ref==null||!(ref>0)) return null;
  return +(((lastC-ref)/ref)*100).toFixed(2);
}

// 5-minute close-to-close DIRECTION per 5m bar -> Map(barIdx -> +1/-1/0). Powers the crypto "BTC corr" column: the
// % of 5m bars an alt closes the SAME direction as BTC (size-agnostic, no threshold). 2026-06-23.
function sign5(B){ const BAR=300000, cl=new Map(); for(const k of B) cl.set(Math.floor(k[0]/BAR), k[4]); const sg=new Map(); for(const b of cl.keys()) if(cl.has(b-1)){ const r=cl.get(b)/cl.get(b-1)-1; sg.set(b, r>0?1:(r<0?-1:0)); } return sg; }

// the per-instrument heavy metrics — Nate's dual-gate move rule (retraceZig) computed per pullback filter; the
// non-move fields (price / 7d range / volume / daily %) are unchanged from the prior recompute
// ---- EMA CROSS LIST (v1.3.0, CRYPTO ONLY). Two pairs — 9/16 and 12/21 — read on six timeframes
// (5m/15m/30m/1h/2h/3h/4h/8h/D). The SIGNAL timeframe is chosen in the UI ("Timeframe Trigger"); the state below is
// replayed for every timeframe so switching is instant. Read "15m" in the rest of this note as "the chosen one". ONLY a 15m 9/16 cross LISTS an asset — never the 12/21
// on its own — and only when the 12/21 still DISAGREES (that is what "pending" means: the 9/16 flipped and the
// 12/21 has not yet) or when both pairs cross on the SAME bar (valid at once). PENDING therefore ALWAYS shows the
// two 15m squares disagreeing. It goes VALID when the 12/21 crosses into the 9/16's direction. It drops off an
// HOUR after going valid ("its too late as I missed the cross"). EITHER pair crossing back against the listed
// direction kills the row at ANY stage — a flip-back is a false signal, not a trend change — and a 9/16 flipping
// back into a 12/21 that never moved is removed rather than re-listed. A pending that never resolves sits. The other five timeframes are REFERENCE readouts only — current direction, no state machine.
// CLOSED BARS ONLY (the still-forming bucket is dropped). The whole list state is REPLAYED from the stored 1m
// candles on every recompute, so nothing is persisted between runs and nothing new is downloaded.
const EMA_TFS = [5,15,30,60,120,180,240,480,1440];   // Nate keeps BOTH 4h and 8h, and added 2h + 3h        // the timeframe columns, in minutes; 1440 renders as "D"
const EMA_VALID_MULT = 8;                      // a valid row lives 8x the timeframe it went valid ON — Nate 2026-08-22: "I wanted valid trades to stay 8x the timeframe the trade went valid." => 5m->40m, 15m->2h, 30m->4h, 1h->8h, 2h->16h, 3h->24h, 4h->32h, 8h->64h, D->8d
const emaLifeMs = tfMin => tfMin*60000*EMA_VALID_MULT;
const EMA_BARS = 260;                          // bars kept per timeframe — well past EMA21 warm-up, and enough history to replay the state on any of them (verified: identical state at 1000 bars)

// SMA-seeded EMA — the SAME algorithm as the chart's (desktop/chart.html), so the tab and the chart never disagree.
function emaArr(v,p){ const o=new Array(v.length).fill(null); if(v.length<p) return o;
  let sum=0; for(let i=0;i<p;i++)sum+=v[i]; let e=sum/p; o[p-1]=e; const k=2/(p+1);
  for(let i=p;i<v.length;i++){ e=v[i]*k+e*(1-k); o[i]=e; } return o; }

// 1m -> tfMin buckets (UTC-aligned floor, last close wins), trailing EMA_BARS only, STILL-OPEN bucket DROPPED.
// WHICH BUCKET IS STILL FORMING. Crypto used to store the FORMING 1m bar (t<now) and never fetch it again, so every
// recent 1m close was a mid-minute snapshot: measured on btc/eth, 22-27% of recent bars had a next open more than 2bp
// from the previous close, against ~0% on real exchange bars. Every stoch, EMA and spike reading built on those closes
// inherited it -- independent review showed it moving the 1m Stoch Jackpot trigger. Crypto now stores CLOSED bars only
// (fetchCrypto / fetchKuCoin keep t+STEP<=now), and the newest stored bar's bucket is complete once the wall clock is
// past its end. That needs nowMs, passed ONLY from the crypto compute path. Without it -- stocks, Dukascopy, every
// research script and test -- the original rule stands unchanged: the newest stored bar's bucket is the forming one.
// ...and ONLY when the stored bars actually reach that bucket's final minute. Review of 1.29.3 caught the clock-only
// version publishing a 30m bar holding 28 of its 30 minutes as CLOSED when a coin's download lagged, and the renderer
// then read it as live. A bucket missing its last minute stays forming, exactly as the old engine treated it.
const formingOpen = (lastT, span, nowMs) => { const b=Math.floor(lastT/span)*span; return (nowMs!=null && b+span<=nowMs && lastT===b+span-STEP) ? b+span : b; };
function tfCloses(B, tfMin, maxBars, nowMs){
  const span=tfMin*60000, n=B.length; if(!n) return [];
  const openB=formingOpen(B[n-1][0], span, nowMs);                              // the bucket still forming -> excluded (see formingOpen)
  const cut=openB-(maxBars||EMA_BARS)*span, m=new Map();                         // maxBars defaults to EMA_BARS so every existing caller is unchanged; Lifer passes its own, deeper window
  for(let i=n-1;i>=0;i--){ const t=B[i][0]; if(t<cut)break; const b=Math.floor(t/span)*span; if(b>=openB)continue; if(!m.has(b))m.set(b,B[i][4]); }   // walking BACKWARDS, the first close seen for a bucket is that bucket's LAST 1m close
  return [...m.keys()].sort((a,b)=>a-b).map(b=>({t:b,c:m.get(b)}));
}
function emaDirs(bars,f,sl){ const cl=bars.map(x=>x.c), ef=emaArr(cl,f), es=emaArr(cl,sl);
  return bars.map((_,i)=>(ef[i]==null||es[i]==null)?null:(ef[i]>es[i]?1:-1)); }        // +1 = fast above slow = long, -1 = short

// Rolling % over `mins` — the SAME shape as dayChangePct's crypto branch (latest close vs the OPEN of the bar
// that far back), generalized from 24h to each of the six windows. Nate: "yes match that. rolling."
function pctRoll(B, mins){
  const n=B.length, lastT=B[n-1][0], lastC=B[n-1][4], cut=lastT-mins*60000;
  let i=n-1; while(i>0 && B[i][0]>=cut) i--;
  const ref=B[Math.min(i+1,n-1)][1];
  return (ref>0) ? { pct:+(((lastC/ref)-1)*100).toFixed(2), ref } : { pct:null, ref:null };   // ref is shipped so the tab can recompute the % against the LIVE price
}

// Replay the pending/valid machine on ONE timeframe's closed bars. Run for every timeframe so the tab's
// "Timeframe Trigger" selection is an instant switch. Returns null when the asset is not on that timeframe's list.
function emaState(bars, d916, d1221, tfMin, lastT){
  const LIFE=emaLifeMs(tfMin);                                                                   // this timeframe's valid life
  let onList=false, dir=0, validAt=null, validPx=null, listedAt=null, p916=null, p1221=null;
  for(let i=0;i<bars.length;i++){
    const a=d916[i], b=d1221[i]; if(a==null||b==null) continue;                                  // EMA warm-up: no state, and p916/p1221 stay null so there is no phantom cross on the first evaluable bar
    const t=bars[i].t+tfMin*60000, x916=(p916!==null&&a!==p916), x1221=(p1221!==null&&b!==p1221);  // t = this bar's CLOSE; every duration is measured from when the signal was first knowable
    p916=a; p1221=b;
    if(onList){
      if((x916&&a!==dir)||(x1221&&b!==dir)) onList=false;                                        // ANY flip back against the listed direction kills the row — pending OR valid. A flip-back is a false signal, not a trend change.
      else if(validAt===null && x1221 && b===dir){ validAt=t; validPx=bars[i].c; }                 // the 12/21 CROSSES into the 9/16's direction -> VALID
      else if(validAt!==null && t-validAt>=LIFE) onList=false;                                    // 8x this timeframe after going valid -> gone
    }
    if(!onList && x916 && (b!==a || x1221)){ onList=true; dir=a; listedAt=t; const v=(b===a); validAt=v?t:null; validPx=v?bars[i].c:null; }   // a 9/16 cross lists ONLY while the 12/21 still disagrees (= pending), or when the 12/21 crossed on this same bar (= born valid)
  }
  if(onList && validAt!==null && (lastT-validAt)>=LIFE) onList=false;                    // expire between bar closes too
  return onList ? { dir, validAt, validPx, listedAt } : null;
}

function emaCross(B, nowMs){
  const s916=[], s1221=[], pct=[], ref=[], st=[], lastT=B[B.length-1][0];
  for(const tf of EMA_TFS){
    const bars=tfCloses(B,tf,undefined,nowMs), d916=emaDirs(bars,9,16), d1221=emaDirs(bars,12,21), L=bars.length-1;
    s916.push(L>=0?d916[L]:null); s1221.push(L>=0?d1221[L]:null);
    const pr=pctRoll(B,tf); pct.push(pr.pct); ref.push(pr.ref);
    st.push(emaState(bars,d916,d1221,tf,lastT));
  }
  return { s916, s1221, pct, ref, st };
}

// ROLLING MOVES (Nate, 2026-09-12): "add a pnl column at the end. which is both rolling 1hr and 4hr pnl side by
// side." The engine ships the MOVE, end to end, rather than a reference price for the renderer to divide the live
// price into. That earlier design tied the window length to how stale the feed was, and the staleness could not be
// bounded: a stalled feed never rewrites its candle file, so recompute reuses the cached metrics on mtime alone
// (see the cache-hit branch below) and the value froze -- US sat 76 min behind and did not move. Anchoring the
// window on the asset's OWN newest bar makes it exactly minsBack of that asset's real tape no matter what the feed
// is doing -- EXCEPT where the asset's own tape has a hole straddling the mark, which is the honest limit of this.
// Swept over every anchor of the last 7 days: the window overran its label on 0.141% of asset-minutes (>5 min on
// 0.052%, worst a 121-minute window wearing a 1h label, on UB), and 93 of 133 assets were exactly 60 min at every
// anchor. In the current snapshot the mark-to-bar gap is a median 0.0 min, p90 0.0, MAX 12 min (GRASS).
// It also makes the cache-reuse path harmless AS TO WINDOW LENGTH -- the same bars give the same window -- but it
// does NOT make a stale row fresh: a row served from cache after the app sat closed still reports a real hour, just
// an hour that ended whenever that pass last saw bars. The asset name greys for that (liferLiveOK, in the renderer).
// Binary search, and the reference is the last bar AT OR BEFORE the mark, so a gap resolves backwards to a real bar
// rather than to a fabricated price. Measured across all 133 crypto assets, the bar found sits a median of 0.0 min
// before the mark (p90 0.0; on a symbol that has gone silent the gap is unbounded, which is why the window is
// anchored on its own bars and not on the clock).
function moveBack(B, minsBack){
  const n=B.length; if(!n) return null;
  const t=B[n-1][0]-minsBack*60000;                     // the window is anchored on the asset's OWN newest bar,
  let lo=0, hi=n-1, ans=-1;                             // so it is exactly minsBack of this asset's real tape
  while(lo<=hi){ const m=(lo+hi)>>1; if(B[m][0]<=t){ ans=m; lo=m+1; } else hi=m-1; }
  if(ans<0) return null;
  const ref=B[ans][4], end=B[n-1][4];
  return (ref>0 && end>0) ? (100*(end/ref-1)) : null;    // UNROUNDED: the cell rounds, the SIGN must not be rounded first
}
// VOLUME RISING (Nate, 2026-09-12): "I want a increase in volume metric. which can be a sign of a asset thats bands
// are tight together thans been consolidating and is about to make a big move. especially if it just crossed the
// 200." And the definition, in his words: "if something w/ an average amount of volume suddenly of the course of x
// amount of candles starts averaging more volume thats a sign something is about to happen."
// So: the MEAN of the last VOL_WIN closed buckets against the MEAN of the VOL_BASE buckets before them. A ratio, not
// an absolute level -- Binance and KuCoin do not report volume in the same units, and 42 of the 133 crypto symbols
// are KuCoin-routed, so an absolute threshold would be silently wrong for a third of the pool.
// BREADTH is shipped alongside it, and it is the one thing here Nate did not ask for. A mean alone is satisfied by a
// SINGLE print, which is the opposite of "starts averaging more": measured on his own pool, 19 of the 26 assets
// passing 2x at 1m had a median candle below 1.5x baseline, and six of those had a median of exactly ZERO (APR read
// 17.46x off a median of 0.00). Breadth counts how many of the window's buckets individually beat the baseline, so
// the cell can say "2.4x on 14 of 20" and a one-print spike is visible as "8.1x on 1 of 20". Both numbers ship; the
// screener can test either.
// CLOSED buckets only -- tfSeries already drops the forming one. A half-formed bucket carries half its volume and
// would make every asset read as collapsing.
const VOL_WIN = 20, VOL_BASE = 100;
// THE SPIKE. Nate's tell, in his words: "itll goto little volume to sudden spike ... than the next couple candles a
// big move is happening". volRise below CANNOT see that, by construction: it averages the last 20 buckets, so a
// spike confined to one candle is divided by 20 before it is compared to anything. Run the arithmetic -- a genuine
// 10x single-candle spike reads 1.45x on that ratio, and 1.45 is below the weakest threshold the screener offers.
// A 20x reads 1.95x. Its breadth option is worse than useless here: it demands 12 of the last 20 buckets be above
// baseline, and a 1-2 candle spike gives 1 or 2, so that option rejects his pattern outright.
// So this is the NEWEST CLOSED bucket against the mean of the SPIKE_BASE before it -- no averaging of the spike
// itself, nothing else mixed in. Volume only: he asked for volume and said a bigger candle does not have to happen.
// The one number he did not specify is how far back "normal" reaches; 20 is used because that is already this
// file's window for "recent" and it keeps a single meaning of the word in the app.
const SPIKE_BASE = 20;
// MORE THAN ONE BASELINE (Nate, 2026-10-03: "allow the sweep to answer that").
// ASK THE RIGHT QUESTION. A first pass measured PRECISION -- "given a spike, what price move follows" -- and
// reported no edge. Nate rejected that outright: "if the sweep did not find an edge than youre doing it wrong.
// find a pump that didnt have increased volume." He was right, and the distinction is the whole design: this is a
// SCREEN. It may fire on a hundred spikes that go nowhere and still be exactly right, so long as a pump almost
// never happens WITHOUT one. That is RECALL, and recall is what was actually claimed.
// RECALL ALONE IS NOT A RESULT EITHER -- the trap that caught the first two attempts, recorded so it is not walked
// into a third time. Measuring "99.5% of +5% pumps carried a >=1.5x spike" sounds decisive and means nothing: the
// BASE RATE is 99.36%. Across all 411,550 disjoint 30-minute windows, pump or not, 99.36% contain a >=1.5x
// 1m/base-20 bucket, so LIFT = 1.001. It is closed-form -- 18.61% of 1m buckets clear 1.5x, and the max over 31 of
// them is 1-0.8139^31 = 99.8%. That number measures the WINDOW LENGTH. Always divide by the base rate.
// WHAT SURVIVES THAT TEST (96 Yubit crypto assets listed in Desktop/claude/ASE_pump_recall_assets.txt, 90 days,
// pump = +5% within 30 minutes, lift = P(spike|pump) / P(spike)):
//    config        1.5x    3x     8x    15x
//    1m/base-20    1.00   0.94   0.88   0.93      <- at or BELOW 1.0 everywhere, at every pump size
//    3m/base-60    1.16   1.49   2.42   3.32
//    5m/base-60    1.35   1.97   3.70   5.58      <- rises monotonically with the threshold
// So the discrimination is at a LONGER baseline on a SLOWER timeframe, and 1m/base-20 -- the config this file has
// always shipped as `x` -- carries none. A separate forward test (signal first, move measured only afterwards)
// reproduces the same ordering: 1m/base-20 decays to 0.84 at 30x while 5m/base-60 climbs to 6.40.
// STILL OPEN, and NOT to be presented as settled: absolute precision is low everywhere (0.32% base, 2.08% at
// 5m/base-60 @30x), so this is a WATCHLIST, not a trigger. And 1,245 of those windows have a max multiple of
// exactly 0 -- dead tape, not a quiet pump.
// All three baselines ship so the tab's control is a real switch and the comparison is Nate's to make on live
// tape rather than on this paragraph. x stays base-20 so the existing Lifer column, screener and alert are
// untouched -- verified bit-identical to the previous implementation over 400,000 randomized trials.
const SPIKE_BASES = [20, 40, 60];
// QUIETNESS (Nate, 2026-10-03): "we are looking for very low volume assets that transition into something that
// moves due to the sudden surge of volume... there was low volume over long periods. not high volume thruout the
// recent history than a temporary candle or two dip than spike again."
// volSpike CANNOT express that: it is only `this bucket / the last S buckets`, so an asset busy all day that dips
// two candles and prints normally ranks top of the list. He caught it on the live board -- OPG was running at
// 848% of its own normal volume and still read 4.4x. Nothing in the ratio asks whether the asset has been QUIET.
// So: the recent QUIET_WIN buckets against this asset's OWN longer norm. Below 1 = it has gone quiet.
// MEAN over MEAN deliberately -- an earlier pass compared a mean baseline to a MEDIAN and, because volume is
// right-skewed (the mean sits ~3.2x above the median on this pool), that pushed almost every asset toward "busy"
// before any data was read.
// MEASURED, 96 assets x 90 days x 2.06M 5m buckets, outcome = a +20% high within the next 24h (base rate 2.18%),
// grid: surge baseline {20,60} x surge {3,5,10}x x quiet window {8,12,24,48,72}h x quiet threshold
// {0.05,0.10,0.15,0.25} x 14-day reference.
//    surge 10x / base-20 ALONE (the shipped rule)        21,138 alerts   ~1.0x lift
//    surge 10x / base-20 + quiet 24h <= 0.15                383 alerts   4.70%   2.16x lift
//    surge  5x / base-20 + quiet 24h <= 0.10                335 alerts   4.48%   2.06x
// The optimum is INTERIOR on every axis (0.05 and 48/72h do not dominate), so the grid is not truncated at its
// own edge -- the mistake made twice earlier in this work. The quiet term carries almost all of the lift.
const QUIET_WIN = 144;      // 12h of 5m buckets
const QUIET_REF = 2016;     // the asset's own 7-day norm
function volQuiet(bars5){
  const n = bars5.length;
  if(n < QUIET_REF + 1) return null;                            // not enough history to know what normal is
  const vol = i => { const v = bars5[i][5]; return (typeof v === "number" && isFinite(v) && v > 0) ? v : 0; };
  let w = 0, r = 0;
  for(let i=n-QUIET_WIN;i<n;i++) w += vol(i);
  for(let i=n-QUIET_REF;i<n;i++) r += vol(i);
  const ref = r/QUIET_REF;
  if(!(ref>0)) return null;                                     // a dead tape has no norm to be quiet against
  return +((w/QUIET_WIN)/ref).toFixed(3);                       // < 1 = quieter than its own normal
}
function volSpike(bars){
  const n = bars.length;
  if(n < SPIKE_BASE + 1) return null;                           // no "normal" to compare against yet
  // RAW volume, deliberately NOT volume-per-minute-present. volRise divides by the bar count in the bucket, and
  // copying that here invented spikes out of nothing: a 5m bucket holding ONE minute of ordinary volume divides by 1
  // and reads a clean 5.00x -- a false alarm made entirely of four missing minutes. A spike is "more volume than
  // normal arrived", so it is the volume that is compared. A gappy bucket now reads LOW, which is the safe direction.
  const vol = i => { const v = bars[i][5]; return (typeof v === "number" && isFinite(v) && v > 0) ? v : 0; };
  const cur = vol(n-1);
  const at = w => {                                             // multiple against the mean of the w buckets BEFORE the newest
    if(n < w + 1) return null;
    let b = 0;
    for(let i=n-1-w;i<n-1;i++) b += vol(i);
    const base = b/w;
    if(!(base>0)) return null;                                  // dead tape has no multiple, only a divide by zero
    return +(cur/base).toFixed(2);
  };
  const x = at(SPIKE_BASE);
  if(x == null) return null;
  const b = {};
  for(const w of SPIKE_BASES){ const v = at(w); if(v != null) b[w] = v; }
  // dir: the newest closed candle's own direction, which is the Long/Short the tab shows
  // (Nate: "dump and pump can be designated by a column that says simply short or long").
  return { x, n:SPIKE_BASE, b, dir:(bars[n-1][4] >= bars[n-1][1] ? 1 : -1) };
}
function volRise(bars, tfMin){
  const n = bars.length;
  if(n < VOL_WIN + VOL_BASE) return null;                       // not enough history to have a "normal" to compare against
  // RATE, not total: volume divided by the minutes that actually arrived in the bucket. A bucket that is 80% filled
  // reports its true pace instead of looking 20% quieter, so no bucket has to be discarded and the window stays the
  // newest VOL_WIN -- contiguous, never lagged.
  const rate = i => { const c = bars[i][6]||0; return c>0 ? (bars[i][5]||0)/c : 0; };
  let w=0, b=0, brd=0;
  for(let i=n-VOL_WIN;i<n;i++) w += rate(i);
  for(let i=n-VOL_WIN-VOL_BASE;i<n-VOL_WIN;i++) b += rate(i);
  if(!(b>0)) return null;                                       // a baseline of zero has no ratio -- a dead tape, not a surge
  const base = b/VOL_BASE;
  for(let i=n-VOL_WIN;i<n;i++) if(rate(i) > base) brd++;
  return { r:+((w/VOL_WIN)/base).toFixed(2), br:brd, n:VOL_WIN };
}
// ---- LIFER (crypto only). Nate's rule, literally: on a CLOSED bar of the selected timeframe, the CLOSE is either
// above BOTH the 21 and 200 EMA (long), below BOTH (short), or between them (inbetween). "when it closes in between.
// nothing. its not valid." -> an inbetween close ENDS the run; the next close back on a side starts a NEW one, with a
// new cross time and a new entry. The asset becomes valid ON THE CANDLE CLOSE, but the PnL is tracked from the price
// at which it crossed: the CLOSE of the candle that cleared both EMAs.
// Every timeframe is replayed so the tab's buttons switch instantly. Its OWN bar window: the shipped tfCloses is
// capped at EMA_BARS=260, which leaves only 61 bars after an SMA-seeded EMA200 warm-up.
const LIFER_TFS = [1,3,5,15,30,60,120,240,1440];   // 1m / 3m / 5m / 15m / 30m / 1h / 2h / 4h / 1d -- Nate asked for 30m-1d first, then 5m and 15m, then 1m and 3m for scalping ("for these particular cases using the 200 adds stability to the usually chaotic 1m and 3m timeframes")
const LIFER_BARS = 100000;                     // effectively uncapped: use every stored bar, so EMA200 seeds on the same history at every timeframe

// RETEST -> REJECTED or ACCEPTED (Nate, 2026-09-09): "I just dont want to know if it retested I want to know if
// it rejected or was accepted. a rejection tells a different story than acceptance."
// The break is on CLOSES, exactly as the side already is. The return to the band has to be the WICK: a close back
// at a band is a close BETWEEN the bands, which by his own rule ends the run — which is why liferState takes OHLC.
// The two outcomes sit on opposite sides of the run boundary, and that is why reporting only rejections could
// never show an acceptance:
//   REJECTED — the wick came back to the near band and the close still held the side. The run continues.
//   ACCEPTED — a close went THROUGH: the run that was above (or below) both bands ends on this bar, whether it
//              closed between the bands or all the way to the other side. Nate: "if not it goes thru the bands
//              and switches to the other side."
// The field therefore reports the MOST RECENT test of a band and its outcome, and it deliberately survives the
// run boundary — an acceptance IS the event that produced whatever state the row is in now, so clearing it on the
// new run would delete the only record of it.
// STOCH RSI %K (Nate, 2026-09-09): "I just want the number 0-100 so I can know if its over sold or over bought
// at a glance", and "stochastic rsi", and "the stoch value applies to whatever timeframe is selected."
// Parameters 14/14/3/3 — taken from the TradingView charts he sent earlier today (ZRO 5m and ENA 3m both display
// "Stoch RSI (14, 14, 3, 3)"), so the number in the app matches the number on his screen. One value per Lifer
// timeframe; the row shows whichever timeframe is selected.
function lStoch(cl){
  const n=cl.length, P=14;
  if(n < P*2+6) return null;
  let g=0, l=0;                                                   // Wilder RSI(14)
  for(let i=1;i<=P;i++){ const d=cl[i]-cl[i-1]; if(d>0) g+=d; else l-=d; }
  g/=P; l/=P;
  const rsi=new Array(n).fill(null);
  rsi[P] = l===0 ? 100 : 100 - 100/(1+g/l);
  for(let i=P+1;i<n;i++){
    const d=cl[i]-cl[i-1];
    g=(g*(P-1)+(d>0?d:0))/P;
    l=(l*(P-1)+(d<0?-d:0))/P;
    rsi[i] = l===0 ? 100 : 100 - 100/(1+g/l);
  }
  const raw=new Array(n).fill(null);                              // stochastic OF the RSI, over 14
  for(let i=P*2;i<n;i++){
    let mn=Infinity, mx=-Infinity, ok=true;
    for(let k=i-P+1;k<=i;k++){ const v=rsi[k]; if(v==null){ ok=false; break; } if(v<mn)mn=v; if(v>mx)mx=v; }
    if(!ok) continue;
    // A FLAT RSI WINDOW HAS NO ANSWER, AND 100 IS THE WORST POSSIBLE GUESS. mx===mn is 0/0: every RSI value in
    // the 14-bar window is identical. Substituting 100 -- the top of the scale -- is direction-blind, so it read
    // "maximally overbought" on a straight COLLAPSE (200 strictly decreasing closes hold RSI at 0, which is
    // maximally oversold, and returned %K=100, %D=100). Measured against the honest convention on the real files:
    // BTT 1m 94,236 bars showed >=80 where the truth is <=50, NFT 89,329, LIT 14,211 -- and LIT's live payload
    // read exactly 100. It also corrupted the arrow, the 20/80 highlight and the tail the renderer advances from.
    // Leaving raw[i] null is what the rest of this function already expects: the %K walk below breaks on a null,
    // lStoch returns null, and the cell renders BLANK. On a near-empty 1m tape (BTT is 89.9% zero-volume minutes)
    // that blank is frequent, and it is the truth.
    if(mx===mn) continue;
    raw[i] = 100*(rsi[i]-mn)/(mx-mn);
  }
  // %K = 3-period SMA of the raw stoch RSI. We need the last FOUR %K values so the arrow can show both a
  // direction (this %K vs the previous) and a CROSS of %K through %D (Nate: "whether its moving up or down. or
  // cross up or down") — %D is the 3-period SMA of %K, so it needs three %K values, and detecting a cross needs
  // the previous %D too, hence four.
  const K=[];
  for(let e=n-1;e>=0 && K.length<60;e--){                          // deep enough to walk back through a run of ties
    if(raw[e]==null || raw[e-1]==null || raw[e-2]==null) break;
    K.push((raw[e]+raw[e-1]+raw[e-2])/3);                          // K[0] = newest
  }
  if(K.length<4) return null;
  const r1=v=>+v.toFixed(1);
  const D=[];                                                      // %D = 3-period SMA of %K, same indexing
  for(let i=0;i+2<K.length;i++) D.push((K[i]+K[i+1]+K[i+2])/3);
  // SIDE, WITH A TIE CONTINUING THE PREVIOUS SIDE (Nate: "treat a tie as continuing the previous side").
  // The comparison is made on the ROUNDED values, because those are the numbers the app displays and the ones a
  // tie is defined against. Resolve oldest-to-newest so an inherited side propagates through a run of ties; a
  // tie with nothing before it to inherit from stays unresolved rather than being invented.
  const side=new Array(D.length).fill(0);
  for(let i=D.length-1;i>=0;i--){
    const k=r1(K[i]), d=r1(D[i]);
    side[i] = k>d ? 1 : k<d ? -1 : (i+1<D.length ? side[i+1] : 0);
  }
  if(D.length<2) return null;
  // LIVE TAIL STATE. Wilder RSI is recursive from the first bar, so the renderer cannot recompute a matching
  // value from a short window -- it would produce a different number and disagree with this one. Instead ship
  // exactly what is needed to advance the recursion by ONE bar using the live price as the forming bar's close:
  //   g,l        the Wilder average gain/loss at the last CLOSED bar
  //   c          that bar's close (the base for the next delta)
  //   r13        the previous 13 RSI values, so the new one completes a 14-bar window for the stochastic
  //   raw2       the previous 2 raw stoch-RSI values, so the new one completes the 3-bar SMA that is %K
  //   k2         the previous 2 %K values, so a live %K completes the 3-bar SMA that is %D
  const r13=[]; for(let i=n-1;i>=0 && r13.length<13;i--){ if(rsi[i]==null) break; r13.push(rsi[i]); }
  const raw2=[]; for(let i=n-1;i>=0 && raw2.length<2;i--){ if(raw[i]==null) break; raw2.push(raw[i]); }
  const tail = (r13.length===13 && raw2.length===2 && K.length>=2)
    ? { g, l, c:cl[n-1], r13, raw2, k2:[K[0],K[1]] } : null;
  return { k:r1(K[0]), kPrev:r1(K[1]), d:r1(D[0]), dPrev:r1(D[1]),
           up:(side[0]===0?null:side[0]>0), wasUp:(side[1]===0?null:side[1]>0), tail };
}
function liferState(bars, tfMin, scan, scanMin){                                    // bars: [t,o,h,l,c] on the base tf; scan: the finer bars
  const cl=bars.map(b=>b[4]), e21=emaArr(cl,21), e200=emaArr(cl,200);
  let side=0, at=null, atI=-1, px=null, seen=false, accT=null;                      // side: 1 long, -1 short, 0 inbetween (not valid); accT = when a run was last ENDED by a close through the band
  for(let i=0;i<bars.length;i++){
    if(e21[i]==null||e200[i]==null) continue;                                       // EMA200 warm-up: no state at all
    seen=true;
    const c=cl[i], hi=Math.max(e21[i],e200[i]), lo=Math.min(e21[i],e200[i]);
    const st = (c>hi) ? 1 : (c<lo) ? -1 : 0;                                        // strictly above both / strictly below both / between
    if(st!==side){
      if(side!==0) accT=bars[i][0]+tfMin*60000;                                     // a run was in force and this close went THROUGH: ACCEPTED, on the base tf close (Nate's rule is candle closes)
      if(st===0){ side=0; at=null; atI=-1; px=null; continue; }                      // an inbetween close is NOT valid -> the run ends
      side=st; at=bars[i][0]+tfMin*60000; atI=i; px=c;                               // a NEW run: stamped at this bar's CLOSE, entered AT that close — Nate asked for "pnl since cross", so the entry is the crossing candle's close, not the EMA level it cleared
    }
  }
  // DATA TIME (Nate, 2026-09-10): "fix the asOf staleness thing so it uses data time". asOf is when the ENGINE
  // ran, not how fresh THIS row's bars are. A kline fetch that failed or returned short leaves this asset behind
  // while asOf still says "just now", so the live stoch advance is applied to a bar that closed long ago and the
  // renderer cannot tell. dt is the OPEN of the last CLOSED bar actually used here, on this timeframe, so the
  // gate can be asked of the data instead of the clock. Null when there are no bars at all.
  const dt = bars.length ? bars[bars.length-1][0] : null;
  if(!seen){ const S0=lStoch(cl);
    return { side:null, at:null, px:null, rt:null, rtOk:null, dist:null, dt, e21:null, e200:null, lastC:null, x200:null, rt200:null,
             stoch:S0?S0.k:null, stochPrev:S0?S0.kPrev:null, stochD:S0?S0.d:null, stochDPrev:S0?S0.dPrev:null,
             stochUp:S0?S0.up:null, stochWasUp:S0?S0.wasUp:null, stochTail:S0?S0.tail:null }; }              // never enough history to seed EMA200 on this timeframe -> UNKNOWN, not "inbetween"
  // ---- THE REJECTION, FOUND ON THE FINER CANDLES ------------------------------------------------------------
  // Nate, 2026-09-09: "one it crosses the bands can we track price w/ 1m candles?" and "last confirmed values for
  // the base chart selected. 1m candles is working behind the scenes using that base timeframes bands. do 1m
  // candles for 5m and 15m and 30m. do 5m candles for the rest of the timeframes."
  // Waiting for the BASE bar to close made a rejection up to a full bar late — measured median 139 min on 4h and
  // 1114 min on 1d. The touch and the hold are both visible on the finer candles the moment they happen, tested
  // against the base timeframe's LAST CONFIRMED bands (the ones that were in force when the bar opened, which is
  // also the only band level that is actually known at that moment).
  // Only the REJECTION moves to the finer candles. An acceptance ends the run, and a run ends on a base-tf close
  // ("candle closes") — that is his original rule and it is untouched.
  let rt=null, rtOk=null;
  if(side!==0 && atI>=0 && scan && scan.length){
    let si=0;
    const N=bars.length;
    // ...and one PAST the last closed base bar: tfOHLC drops the still-forming bucket, so without this the finer
    // candles inside the bar that is open RIGHT NOW are never looked at — which is the exact staleness the finer
    // scan exists to remove (up to a full day on 1d). The band there is the last closed bar's, which is what
    // "last confirmed" means at that moment anyway.
    for(let i=atI+1;i<=N;i++){                                                      // every base bar AFTER the one that started the run, plus the forming one
      const j=Math.min(i,N)-1;                                                      // the last CLOSED base bar at that point
      if(e21[j]==null||e200[j]==null) continue;                                     // guard BEFORE its own use: Math.max(null,null) is 0, not null, so a reorder here would silently compare price against zero
      const hi=Math.max(e21[j],e200[j]), lo=Math.min(e21[j],e200[j]);               // LAST CONFIRMED bands
      const t0=(i<N) ? bars[i][0] : bars[N-1][0]+tfMin*60000, t1=t0+tfMin*60000;
      while(si<scan.length && scan[si][0]<t0) si++;
      for(let k=si;k<scan.length && scan[k][0]<t1;k++){
        const b=scan[k];
        const back = (side===1) ? (b[3]<=hi) : (b[2]>=lo);                          // the wick came back to the near band
        if(!back) continue;
        const held = (side===1) ? (b[4]>hi) : (b[4]<lo);                            // ...and this finer candle CLOSED back on the side: REJECTED
        if(held) rt=b[0]+scanMin*60000;                                             // stamped at the finer candle's close — the latest one wins
      }
    }
  }
  // DISTANCE between the two bands (Nate, 2026-09-09): "a column named distance. and its the distance between
  // the 2 emas. when they get tight is when a big move is likely to happen." Taken at the last CONFIRMED base
  // bar, and expressed as a PERCENT OF PRICE — the one choice here, made so a 0.4% squeeze on BTC and on SHIB
  // read the same; a raw price gap would be unrankable across the pool.
  const S=lStoch(cl);
  const stoch = S ? S.k : null, stochPrev = S ? S.kPrev : null, stochD = S ? S.d : null, stochDPrev = S ? S.dPrev : null;
  const stochUp = S ? S.up : null, stochWasUp = S ? S.wasUp : null, stochTail = S ? S.tail : null;
  let dist=null, b21=null, b200=null, lastC=null;
  { const j=bars.length-1;
    if(j>=0 && e21[j]!=null && e200[j]!=null && cl[j]>0){
      dist=+(100*Math.abs(e21[j]-e200[j])/cl[j]).toFixed(3);
      b21=e21[j]; b200=e200[j]; lastC=cl[j];                                        // the band pair AND the close at bar dt -- the renderer's starting point
    } }
  if(rt==null && accT!=null){ rt=accT; rtOk=false; }                                // no rejection since the run began -> the last thing that happened was the acceptance that ended the previous run
  else if(rt!=null) rtOk=true;
  // ---- THE 200 ON ITS OWN (Nate, 2026-09-12): "a column next to it called retest and a simple Y or N. and thats
  // if it had crossed the 200 and retested and rejected off of it. and a column for date and time it had crossed
  // the 200."
  // These are the SAME two rules the run above already uses, applied to ONE line instead of the 21/200 pair:
  //   CROSSED  = a closed base bar whose close sits on the other side of the 200 from the previous closed bar's,
  //              stamped at that bar's CLOSE -- the same convention as the CROSSED column.
  //   REJECTED = after that cross, on the finer scan candles, a wick came back to the 200 and a finer candle then
  //              CLOSED back on the crossing side. Character for character the rejection above, with the 200 as
  //              the line instead of the near band.
  // Reported SEPARATELY from the run's own rt/rtOk, which tests whichever band is nearer -- on most rows that is
  // the 21, not the 200, so the existing retest column does not answer this question.
  // A close exactly ON the 200 is neither side and is skipped rather than counted as a cross.
  let x200=null, x200I=-1, x200Side=0, rt200=false;
  { let pv=0;
    for(let i=0;i<bars.length;i++){
      if(e200[i]==null) continue;
      const s = cl[i]>e200[i] ? 1 : cl[i]<e200[i] ? -1 : 0;
      if(s===0) continue;
      if(pv!==0 && s!==pv){ x200=bars[i][0]+tfMin*60000; x200I=i; x200Side=s; }
      pv=s;
    } }
  if(x200I>=0 && scan && scan.length){
    const N=bars.length;
    let si=0;
    outer:
    for(let i=x200I+1;i<=N;i++){                                                    // every base bar after the cross, plus the forming one
      const j=Math.min(i,N)-1;
      if(e200[j]==null) continue;
      const L=e200[j];                                                              // the LAST CONFIRMED 200, same as above
      const t0=(i<N) ? bars[i][0] : bars[N-1][0]+tfMin*60000, t1=t0+tfMin*60000;
      while(si<scan.length && scan[si][0]<t0) si++;
      for(let k=si;k<scan.length && scan[k][0]<t1;k++){
        const b=scan[k];
        const back = (x200Side===1) ? (b[3]<=L) : (b[2]>=L);                         // the wick came back to the 200
        if(!back) continue;
        if((x200Side===1) ? (b[4]>L) : (b[4]<L)){ rt200=true; break outer; }         // ...and closed back on the crossing side: REJECTED. It is a Y/N, so the first one settles it.
      }
    }
  }
  return { side, at, px, rt, rtOk, dist, dt, e21:b21, e200:b200, lastC, x200, rt200, stoch, stochPrev, stochD, stochDPrev, stochUp, stochWasUp, stochTail };
}
// The finer candles a rejection is hunted on, per base timeframe (Nate): 1m for 5m/15m/30m, 5m for the rest.
// 1m and 3m hunt the rejection on 1m candles. At tf=1 the scan series IS the base series, so the touch and the
// hold are read off the same candle -- which is the literal rule at 1-minute resolution, not a special case.
const LIFER_SCAN = { 1:1, 3:1, 5:1, 15:1, 30:1, 60:5, 120:5, 240:5, 1440:5 };
// ---- ONE PASS, EVERY TIMEFRAME ----------------------------------------------------------------------------
// THE LAG FIX (Nate, 2026-09-11: "fix it"). Measured on the live app: fetching all 133 crypto assets takes 47
// seconds; rebuilding the table takes ~140, which is why a cross waited 3-7 minutes to be published. The cause
// was never the download. computeMetrics called tfOHLC SIXTEEN times per asset -- nine inside liferCross (seven
// timeframes plus two scan series) and seven more inside liferPatterns, which rebuilt the very series liferCross
// had just built -- and each one is a full pass over 525,601 bars. tfOHLC(B,1) is the worst of them: it builds a
// 525,600-entry Map that is a COPY of B.
// This builds each needed timeframe ONCE, in a single pass, and hands the same arrays to both. The bucketing rule
// is tfOHLC's (UTC-aligned floor, last close wins, still-forming bucket dropped), so the numbers do not move --
// verified equal to tfOHLC for every timeframe on every asset in the pool.
// Sequential rather than Map-keyed: candle files are time-ascending (tfOHLC's own openB already assumes it, by
// taking B[n-1][0] as the newest), so a bucket only ever moves forward. If one ever moved BACKWARDS this would
// silently emit a duplicate bucket, so that case falls back to tfOHLC for that timeframe rather than guessing.
function tfSeries(B, tfs, nowMs){
  const out={}, n=B.length;
  if(!n){ for(const tf of tfs) out[tf]=[]; return out; }
  const lastT=B[n-1][0], st=[];
  for(const tf of tfs){ const span=tf*60000; st.push({tf, span, openB:formingOpen(lastT, span, nowMs), arr:[], curB:-1, cur:null, back:false}); }
  for(let i=0;i<n;i++){
    const r=B[i], t=r[0];
    for(let k=0;k<st.length;k++){
      const S=st[k], b=Math.floor(t/S.span)*S.span;
      if(b>=S.openB) continue;
      if(b!==S.curB){
        if(b<S.curB){ S.back=true; continue; }                       // out-of-order bar: this timeframe is not safe to build sequentially
        S.cur=[b,r[1],r[2],r[3],r[4],+r[5]||0,1]; S.arr.push(S.cur); S.curB=b;   // [6] = how many 1m bars landed in this bucket
      } else { if(r[2]>S.cur[2])S.cur[2]=r[2]; if(r[3]<S.cur[3])S.cur[3]=r[3]; S.cur[4]=r[4]; S.cur[5]+=+r[5]||0; S.cur[6]++; }
    }
  }
  for(const S of st) out[S.tf] = S.back ? tfOHLC(B,S.tf,nowMs) : S.arr;    // fail safe, never silently wrong
  return out;
}
const LIFER_PAT_FAST = new Set([1,3,5,15]);   // the timeframes whose pattern column is recomputed EVERY run rather than carried for an hour
const LIFER_ALL_TFS = [...new Set([...LIFER_TFS, ...Object.values(LIFER_SCAN)])].sort((a,b)=>a-b);   // the bases plus the finer scan series
function liferCross(B, S){
  S = S || tfSeries(B, LIFER_ALL_TFS);                               // S optional, so a research script calling liferCross(B) still works
  return LIFER_TFS.map(tf => { const sm=LIFER_SCAN[tf]||1; const st=liferState(S[tf], tf, S[sm], sm); st.vol=volRise(S[tf], tf); st.sp=volSpike(S[tf]); return st; });
}

// ---- LIFER PATTERNS: triangles + parallel channels (Nate's spec, 2026-09-08) -------------------------------
// SWING PIVOT: a candle whose HIGH is above the highs of the LP_N candles either side (low mirrors). Direction is IRRELEVANT --
// a down candle is a swing high if its high is the local peak, and
// two same-side pivots can never be adjacent. Confirmed LP_N bars later, so it never repaints.
// POINTS: the TWO MOST RECENT confirmed swing highs and the TWO MOST RECENT confirmed swing lows.
// CLASSIFY on the INDEPENDENT fit: extend both lines; meet within 48 candles -> TRIANGLE (keep the independent
//   lines), else -> CHANNEL. Nate: "48 candles is just the metric used to decide if its parallel or triangle."
// LINE CONSTRUCTION: each line is drawn through its OWN two pivots. A shared slope can pass through both highs OR
//   both lows, never both, so exact parallelism and the two-highs-two-lows rule cannot both hold; the two-points
//   rule wins.
// ENDINGS: 12 candles after a breakout; superseded; or at the apex, where the lines cross and there is no inside.
//   No width cap and no live validity test — a shape must earn the name at birth (see lpShapeWin).
// SUPERSEDE: on ANY bar price is inside, as soon as a shape forms using a point newer than the incumbent's newest.
//   Nate: "re-pick the pattern whenever newer points form a shape." His earlier "no changing the pattern once its
//   established" gated this on a prior breakout, which made a diverging channel unreplaceable — it can never be
//   broken, so the gate could never open.
//   MEASURED COST, and it is high: a re-pick every 8.2 bars, 12.2% of all bars change the pattern, supersede is
//   51.9% of all terminations, 37.9% of supersedes flip triangle<->channel, and 16.6% of them anchor the
//   "newer" shape on OLDER tape than the one it replaced — because the guard only compares the SECOND endpoint of
//   each hull edge, so one fresher pivot on one line licenses re-anchoring the other. Knock-on: brkT is re-stamped
//   roughly every 6 bars and supersede clears it, so a breakout being watched is erased.
//   (An earlier version of this comment claimed "~14 bars" and "84% die rather than get swapped". Both were wrong:
//   8.2 bars and 48.1%. Do not restore those numbers.)
// BREAKOUT: by WICK, reporting the CURRENT state, not the first one ever. Outside 12 candles -> the pattern is done.
//   Back inside after a break -> FALSE breakout, and the pattern lives on. One candle through BOTH lines is labelled
//   above then changed to below, so below wins (Nate's rule).
const LP_APEX = 48;
const LP_LIFE = 12;
const lpAt = (L,x) => L.p1 + L.m*(x-L.i1);
const lpLine = (i1,p1,i2,p2) => (i1===i2) ? null : {i1,p1,i2,p2,m:(p2-p1)/(i2-i1)};
const LP_N = 2;                                // swing-pivot strength: 2 candles either side — the 5-bar Williams
                                               // Fractal that TradingView (pivothigh/pivotlow) and MetaTrader both
                                               // ship as the standard. 1 either side marks every wiggle; on 5m that
                                               // gave pivots 2-4 bars apart whose lines diverged ~$97/bar on BTC.
function lpPivots(bars){
  const H=[],L=[];
  for(let i=LP_N;i<bars.length-LP_N;i++){
    let hi=true,lo=true;
    for(let k=i-LP_N;k<=i+LP_N;k++){ if(k===i)continue;
      if(bars[k][2]>=bars[i][2])hi=false; if(bars[k][3]<=bars[i][3])lo=false; }
    if(hi)H.push({i,c:i+LP_N});                // confirmed only once LP_N bars have printed after it
    if(lo)L.push({i,c:i+LP_N});
  }
  return {H,L};
}
// A BOUNDARY RESTS ON REAL SWING POINTS. Two failed constructions are recorded here so they are not retried:
//   * hull over every BAR, chord end-to-end  -> hull[0] is always the window start, so it was just "first bar to
//     last bar" and 90% of candles fell outside their own pattern.
//   * hull over every BAR, edge nearest now  -> upper-hull slopes fall left-to-right and lower-hull slopes rise, so
//     the LAST pair is always the most converging one that exists. It returned a triangle on 10 of 10 assets.
// What a chartist actually does: lay the ruler on the SWING POINTS. So build the hull over the confirmed pivots
// (not the bars) and take the edge that spans the most tape. Every edge then rests on exactly two real pivots —
// Nate's "two high points and two low points" — and because the pivot hull is concave, extending that edge leaves
// no pivot poking through it in either direction. Which edge is a preference; that it touches two points is not.
// THE MOST RECENT STRUCTURE. Nate, 2026-09-09: "youre looking for the most recent structure. if youre telling me
// btc's parallel channel goes back a year, that tells me youre not looking for most recent. there was multiple
// structures that played out today alone."
// So the rule is his original one, literally: the upper line runs through the TWO MOST RECENT confirmed swing
// highs, the lower through the TWO MOST RECENT confirmed swing lows. "both flags and triangles require to high
// points and two low points ... when you draw your lines on those two points if they make a flag than its a flag."
// NO WIDTH CAP: nothing bounds how far back those four pivots sit. If the last two highs are 800 bars apart the
// structure is 800 bars wide, and that is just as valid — the size is an OUTPUT, never a limit.
// TWO INVENTIONS OF MINE ARE DELETED HERE, and neither should come back:
//   * LP_WINS = [40,60,90,25,15], first window that yields a shape. A de facto 90-bar cap.
//   * the convex hull over pivots, plus an edge-preference (widest / most recent). A GLOBAL hull can never show
//     a recent structure at all: today's local highs sit BELOW the year's high, so they are interior points and
//     are never hull vertices. Uncapped + widest edge gave BTC 5m one channel spanning 97,181 bars with a box
//     31% of price; every asset read "inside" and nothing could ever break out.
// The hull was only ever there to answer "the top line only hits 1 point" — two named pivots answer that directly.
// THE RESEARCH'S DEFINITION, built properly this time. A channel / flag / triangle is TWO LINES THAT CONTAIN
// PRICE. That is the whole thing, and every version before this one ignored it:
//   Bulkowski: "Price must touch one trendline at least three times and the other trendline at least twice,
//   forming distinct valleys and peaks." and "Price must cross the pattern from side to side, filling it with
//   price movement, not white space."
// I had both rules in this file and deleted them (v1.9.0, v2.2.1) as unapproved additions of mine. They were not
// mine; they are the research Nate asked me to do. Without them a "boundary" is just a segment through two
// pivots that slices straight through the price it is supposed to bound.
// HOW THE SHAPE IS FOUND — Nate: "you start most recent and move left from there ... the line keeps going":
//   start with the most recent pivots, grow the span LEFTWARD one pivot at a time, and keep the LARGEST span that
//   still contains price and still satisfies 3-touches-one-line / 2-the-other. The walk stops when containment
//   breaks, so the size is an OUTPUT and there is no width cap.
// LP_TOL is the one number: how far a wick may sit outside a line, as a fraction of that asset's own median bar
// range over the span. Everything else here is the research's rule, not a knob.
// A CHANNEL IS ONE TRENDLINE PLUS A PARALLEL COPY. This is the piece that was missing all day. Fitting BOTH
// boundaries independently can only ever produce a converging or a broadening shape — a clean parallel channel is
// essentially unreachable that way, which is why every drawing came out as a wedge. The research is explicit:
// you draw the trendline through two higher-lows (or two lower-highs) and drag a PARALLEL line to the other side.
// A triangle is the two-independent-lines case.
// Scored against the nine assets Nate labelled by hand ("btc is a upward flowing channel. so is shib. so is zro.
// so is ena. eth is forming a triangle. sol is a upward channel. doge is a triangle. link is a triangle." plus the
// triangle he drew on DOT):
//   every variant of the two-independent-lines family, 60 of them:  best 3 of 9
//   with the parallel construction added:                           8 of 9 (only LINK blank)
// LP_TOL is the one number: how far price may sit outside a line, as a fraction of that asset's own median bar
// range. HONEST CAVEAT, do not let this get lost: 1.5 is a PEAK, not a plateau — 1.25 and 1.75 both score 6 of 9 —
// and it was chosen on nine hand-labelled assets. The CONSTRUCTION is what is robust (1.25-2.0 all beat the old
// ceiling of 3 of 9); this exact value is fitted and should be re-checked against more of Nate's marks.
const LP_TOL = 1.5;
// A TOUCH AND A CONTAINMENT ARE DIFFERENT TESTS and were sharing one tolerance. At 1.5 median bar-ranges a line
// sitting a bar and a half away from price counted as "touched", so boundaries with 0 or 1 real touches passed
// the gate and got drawn on nothing -- Nate: "btc has lines just floating on it ... zro has a line on no points."
// Touching is now strict: price must actually reach the line. Containment stays at LP_TOL, because a wick poking
// slightly out of a structure is not price leaving it.
const LP_TTOL = 0.2;
const LP_MAXP = 16;                            // pivots per side considered while growing the span. THIS IS A
                                               // WIDTH CAP and Nate said there is none — it is here only to bound
                                               // the search. With "first structure wins" the loop almost always
                                               // exits far earlier, so it binds rarely; at 16 it was binding on
                                               // DOT and XMR, which is why it is raised.
function lpMedRange(bars,a,b){
  const r=[]; for(let i=a;i<=b;i++) r.push(bars[i][2]-bars[i][3]);
  r.sort((x,y)=>x-y); return r.length ? r[r.length>>1] : 0;
}
// A trendline: through TWO pivots, nothing pokes through it, most touches, and it must use the NEWEST pivot —
// Nate: "it needs to involve the most recent in the most recent structure."
// WICKS AND/OR CLOSES, IN COMBINATION. Nate, 2026-09-09: "it can use wicks and or candle closes. it can be a
// combination of. we are looking for touches. the option w/ the most touches while also being most recent is the
// best line."
// So there is no "wick line" and "body line" to choose between (that was my previous build, and it scored 3 of 9).
// ONE line, and a candle touches it if EITHER its wick OR its close lands on it. An anchor point may likewise be a
// wick or a close, so a single boundary can run from a wick on one candle to a close on another.
// Containment is therefore tested on CLOSES: a line that legitimately rests on closes will have wicks above it by
// construction, so a wick outside is not a violation -- a close outside is.
// Selection is his rule verbatim: MOST TOUCHES, ties broken by the MOST RECENT.
const lpWk=(b,i,up)=>up?b[i][2]:b[i][3];               // the wick extreme
const lpCl=(b,i)=>b[i][4];                             // the close
function lpTouch1(b,i,up,v,tol){                       // does candle i touch the line value v?
  return Math.abs(lpWk(b,i,up)-v)<=tol || Math.abs(lpCl(b,i)-v)<=tol;
}
function lpBound(bars,pv,up,tol,ttol){
  const newest=pv[pv.length-1];
  let best=null;
  for(let i=0;i<pv.length;i++) for(let j=i+1;j<pv.length;j++){
    const x1=pv[i], x2=pv[j];
    // each anchor may be that candle's WICK or its CLOSE -- four line candidates per pivot pair
    for(const y1 of [lpWk(bars,x1,up), lpCl(bars,x1)])
    for(const y2 of [lpWk(bars,x2,up), lpCl(bars,x2)]){
      const m=(y2-y1)/(x2-x1);
      let ok=true, tp=0, last=-1;
      for(const x of pv){
        const on=y1+m*(x-x1);
        if(up ? (lpCl(bars,x) > on+tol) : (lpCl(bars,x) < on-tol)){ ok=false; break; }   // a CLOSE through it
      }
      if(!ok) continue;
      // "the option w/ the most touches" is about CANDLES, not pivots (Nate). Counting only pivots capped tp at k
      // and, worse, a line fitted through two pivots always scores 2 on its own two anchors — so the 2-touch
      // minimum was vacuous for the fitted line and in practice only ever gated the parallel copy.
      for(let z=pv[0]; z<=pv[pv.length-1]; z++){
        const on=y1+m*(z-x1);
        if(lpTouch1(bars,z,up,on,ttol)){ tp++; if(z>last) last=z; }
      }
      { const on=y1+m*(newest-x1); if(!lpTouch1(bars,newest,up,on,ttol)) continue; }     // must involve the newest
      // his rule: most touches, ties to the most recent
      if(!best || tp>best.tp || (tp===best.tp && last>best.last)) best={i1:x1,p1:y1,m,tp,last};
    }
  }
  return best;
}
// Distinct touches over the span, counted once per approach (Bulkowski's "distinct valleys and peaks"), where a
// touch is the wick OR the close reaching the line.
function lpTouches(bars,L,a,b,up,tol){
  let n=0, on=false;
  for(let i=a;i<=b;i++){
    // A TOUCH IS A TOUCH. The second clause was `close > v-tol` for an upper line, which counts any candle whose
    // close is ANYWHERE above the line — so a boundary floating above price scored a touch on nearly every bar
    // and won the most-touches selection outright. That is what put lines on no points.
    const v=lpAt(L,i), near=lpTouch1(bars,i,up,v,tol);
    if(near && !on) n++;
    on=near;
  }
  return n;
}
// The parallel copy sits on a real PIVOT, not on the extreme bar -- that is what makes containment a test that can
// actually fail. Offsetting to the extreme BAR over exactly the range containment scans made it unsatisfiable by
// construction: 277,279 of 277,279 parallel candidates held, while independent candidates failed 81.3%. It is also
// the standard construction (TradingView's Parallel Channel takes three points: two set the line, one the offset).
// The copy is offered BOTH ways -- anchored on a pivot's close and on its wick -- because Nate's rule is that a
// line may use either, in combination. Building both boundaries from closes alone (the previous version) threw
// away the wick anchor lpBound had just chosen: it survived on 2 of 14 shipped rows, and 8 of 12 channels had no
// candle wick lying on either boundary at all.
function lpPara(bars,pvU,pvD,m,i1,fit,wick){
  const hiOf=x=>wick?bars[x][2]:lpCl(bars,x), loOf=x=>wick?bars[x][3]:lpCl(bars,x);
  let hiD=-Infinity, loD=Infinity;
  for(const x of pvU){ const v=hiOf(x)-m*(x-i1); if(v>hiD) hiD=v; }
  for(const x of pvD){ const v=loOf(x)-m*(x-i1); if(v<loD) loD=v; }
  if(!(hiD>-Infinity) || !(loD<Infinity)) return null;
  return { U:{i1,p1:hiD,i2:i1+1,p2:hiD+m,m}, D:{i1,p1:loD,i2:i1+1,p2:loD+m,m}, fit };
}
function lpShapeAt(bars,P,ih,il,now){
  if(ih<2 || il<2) return null;                            // two high points AND two low points, or nothing
  let best=null;
  for(let k=2;k<=LP_MAXP;k++){
    if(ih<k || il<k) break;
    const hs=[],ls=[];
    for(let t=ih-k;t<ih;t++) hs.push(P.H[t].i);
    for(let t=il-k;t<il;t++) ls.push(P.L[t].i);
    // The span is where BOTH boundaries exist. Using min() ran the containment scan back past the other line's
    // oldest pivot, so bar `a` was outside by construction and the independent-lines candidate died at offset 0.
    const a=Math.max(hs[0],ls[0]);
    const tol=LP_TOL*lpMedRange(bars,a,now);
    if(!(tol>0)) break;
    const U0=lpBound(bars,hs,true,tol,LP_TTOL*lpMedRange(bars,a,now)), D0=lpBound(bars,ls,false,tol,LP_TTOL*lpMedRange(bars,a,now));
    const cand=[];
    if(U0&&D0) cand.push({U:lpLine(U0.i1,U0.p1,U0.i1+1,U0.p1+U0.m), D:lpLine(D0.i1,D0.p1,D0.i1+1,D0.p1+D0.m)});
    for(const wick of [false,true]){
      if(D0){ const c=lpPara(bars,hs,ls,D0.m,D0.i1,'D',wick); if(c) cand.push(c); }   // trendline off the LOWS
      if(U0){ const c=lpPara(bars,hs,ls,U0.m,U0.i1,'U',wick); if(c) cand.push(c); }   // trendline off the HIGHS
    }
    let got=null, heldAny=false;
    for(const c of cand){
      if(!c.U||!c.D) continue;
      const ttol=LP_TTOL*lpMedRange(bars,a,now);
      const tu=lpTouches(bars,c.U,a,now,true,ttol), td=lpTouches(bars,c.D,a,now,false,ttol);
      // Bulkowski's 3-and-2 applies to a channel too, just assigned to the right lines: THREE touches on the
      // FITTED trendline, TWO on the parallel copy. Relaxing it to 2-and-2 let any two-pivot box qualify, so the
      // right-to-left search stopped at the first degenerate shape and every span came out ~0.4x the structures
      // Nate marked by hand.
      // Tried and reverted (v3.8.1): 3 touches on the FITTED trendline and 2 on the copy. It is the more
      // faithful reading of Bulkowski, but measured it scored 4 of 9 against Nate's labels vs 5 of 9 for
      // 2-and-2, and cost 13.1s per asset vs 5.5s. Two on each line for a channel, 3-and-2 for a triangle.
      if(c.fit ? (tu<2 || td<2) : (Math.max(tu,td)<3 || Math.min(tu,td)<2)) continue;
      let held=true;                                                          // CONTAINMENT: price fills the shape
      // containment on CLOSES: a line may legitimately rest on closes, in which case wicks sit outside it by
      // construction, so only a CLOSE through a boundary counts as price leaving the shape.
      for(let i=a;i<=now;i++) if(lpCl(bars,i)>lpAt(c.U,i)+tol || lpCl(bars,i)<lpAt(c.D,i)-tol){ held=false; break; }
      if(!held) continue;
      heldAny=true;                                    // ANY candidate holding is enough; never reset it
      const gap=lpAt(c.U,now)-lpAt(c.D,now); if(gap<=0) continue;
      const dm=c.U.m-c.D.m;
      let apex=null; if(dm<0){ const d=gap/(-dm); if(d>0) apex=d; }
      const isTri=(apex!=null && apex<=LP_APEX);
      // A SHAPE MUST LOOK LIKE ITS NAME. The independent-lines candidate is the TRIANGLE construction; when its
      // two lines diverge it is a broadening formation, and shipping that as a "channel" or a "flag" is the exact
      // defect this file fixed once in v2.1.0 and then lost. 5 of 14 rows were broadening, two of them labelled
      // flags, which the research forbids outright. A parallel candidate cannot broaden (its lines share a
      // slope), so channels and flags now come only from that construction.
      if(!c.fit && !isTri) continue;
      // "if a triangle appears inside a parallel channel its now a triangle because it supersedes the channel"
      if(!got || (isTri && got.type!=='triangle'))
        got={U:c.U,D:c.D,est:now,apex,type:isTri?'triangle':'channel',touches:tu+td,
             H:[Math.min(c.U.i1,a),hs[hs.length-1]], L:[Math.min(c.D.i1,a),ls[ls.length-1]]};
    }
    // RIGHT TO LEFT, FIRST STRUCTURE YOU CAN DRAW. Nate, 2026-09-09: "you work right to left. the first
    // structure that you can build right to left. you dont keep the largest. you dont keep the smallest. or the
    // most touches. you build right to left. first structure that you can draw."
    // k grows from the most recent pivots outward, so the first k that yields a shape IS that structure. This
    // also deletes three inventions of mine at once (largest-wins, smallest-wins, most-touches-wins) and the
    // asymmetric `if(k>2) break` early abort that was blanking LINK.
    // TRIED AND REVERTED: `if(!heldAny) break;` — stopping the walk the first time nothing held containment.
    // Containment is NOT monotonic in k: a wider span is fitted to different pivots, so it produces different
    // lines and can hold where a narrower one failed. Breaking on the first failure left 3-6 of 9 assets with no
    // pattern at all and dropped the label score from 5/9 to 2/9 at every tolerance tested.
    if(!got) continue;
    // TRIANGLES FIRST (Nate, 2026-09-09): "look for triangles first. than flags. than parallel channels. channels
    // will almost always be present. triangles are the most useful and what you seem to struggle w/ the most."
    // The outward walk therefore keeps looking for a triangle even after a parallel shape has been found, and
    // falls back to that shape only if no triangle exists in the walk. Taking the first shape of EITHER kind let
    // a k=2 channel pre-empt a triangle a few pivots further out.
    // TRIANGLES FIRST (Nate): "look for triangles first. than flags. than parallel channels. channels will
    // almost always be present. triangles are the most useful."
    // The walk keeps going for a triangle even after a parallel shape has been found, and falls back to that
    // shape only if no triangle turns up. Measured on the four assets Nate labelled as triangles, a valid one
    // that passes every gate exists at ETH k=3 (apex 23), LINK k=4 (apex 8) and DOT k=6 (apex 8) — all of them
    // beyond the k=2 parallel shape the walk used to stop on.
    // This over-fired once before, labelling BTC/ZRO/SOL triangles. That was with the loose shared tolerance and
    // LP_MAXP 40; with strict touches (LP_TTOL) and LP_MAXP 16 it is re-tested, not assumed.
    // SMALLEST AND MOST RECENT WINS (Nate, 2026-09-09): "its ok to have multiple structures on a chart. if its a
    // triangle inside a channel or flag thats fine. we go w/ the smallest most recent for labeling." k grows from
    // the most recent pivots outward, so the first k that yields a valid structure IS the smallest most recent
    // one. Triangles-first is a preference WITHIN that structure (the candidate loop above already applies it);
    // walking past the smallest structure to hunt a triangle further out is the opposite of this rule, and it
    // also cost 7.1s per asset against 1.0s.
    best=got; break;
  }
  return best;
}
function lpBuild(bars,P,ih,il,now,strict){
  const sh=lpShapeAt(bars,P,ih,il,now);
  if(!sh) return null;
  // "if a triangle appears inside a parallel channel its now a triangle because it supersedes the channel" (Nate).
  if(strict && !(Math.max(sh.H[1],sh.L[1]) > Math.max(strict[0],strict[1]))) return null;
  return sh;
}
const LP_REPLAY = 300;                        // bars of history replayed. The shape never reaches further back
                                               // than LP_MAXP pivots, and the status only needs LP_LIFE bars, so
                                               // replaying 105,120 bars to report the last one was pure waste —
                                               // it is what made a pool pass take ~29 minutes against a 60s tick.
                                               // Verified to reproduce the full-replay row exactly (see below).
function lpReplay(allBars){
  const bars = allBars.length>LP_REPLAY ? allBars.slice(-LP_REPLAY) : allBars;
  const P=lpPivots(bars), n=bars.length-1;
  let ih=0, il=0;                                        // how many pivots have CONFIRMED as of bar i
  let pat=null, st='inside', brkAt=null, out=false, outFor=0;
  for(let i=0;i<=n;i++){
    while(ih<P.H.length && P.H[ih].c<=i) ih++;
    while(il<P.L.length && P.L[il].c<=i) il++;
    // SHOW THE LIVE SHAPE (Nate, 2026-09-09: "show live shape."). Every bar, the structure is rebuilt right to
    // left and whatever can be drawn NOW is what the row reports. Previously the row kept the shape price had
    // broken OUT of for 12 candles, so the tab showed a dead pattern for up to 12 bars while a new one was
    // plainly on the chart — that alone accounted for three of the assets Nate labelled by hand (eth, doge, dot).
    // His 12-candle rule still applies, but only where it was always the fallback: when there is NO live shape
    // to draw, the last one is held for 12 more candles and then dropped.
    const live=lpBuild(bars,P,ih,il,i);
    if(live){
      pat=live; brkAt=null; out=false; outFor=0;
      // the establishing bar is evaluated for a breakout like any other, so the row never reports a stale
      // 'inside' while that bar's own wick is already outside the lines it was just given
    }
    if(!pat) continue;
    // A shape cannot outlive its own apex, whatever it is CALLED (Nate: "yes a triangle ends at its apex").
    if(pat.apex!=null && i > pat.est+pat.apex-1e-9){ pat=null; st='inside'; brkAt=null; out=false; outFor=0; continue; }
    const up=bars[i][2]>lpAt(pat.U,i), dn=bars[i][3]<lpAt(pat.D,i);
    if(up||dn){
      const side=(up&&dn)?'below':(up?'above':'below');   // one candle through BOTH lines is labelled below
      if(!out){ st=side; brkAt=i; out=true; outFor=1; }
      else { outFor++; if(side!==st){ st=side; brkAt=i; } }
      // 12 candles outside with no live shape to replace it -> the pattern is done.
      if(outFor>LP_LIFE){ pat=null; st='inside'; brkAt=null; out=false; outFor=0; }
    } else {
      if(out){ st='false'; out=false; outFor=0; }         // back inside after a break -> FALSE breakout
      else st='inside';
    }
  }
  if(!pat) return null;
  const w=lpAt(pat.U,n)-lpAt(pat.D,n);
  // MEASURED MOVE (the research): the target is the pattern's height projected from the breakout, and the height
  // that matters is the one at the pattern's WIDEST point — its start. `height` is the gap at the CURRENT bar,
  // which for a triangle near its apex is almost zero, so it cannot be used for this.
  const b0=Math.min(pat.H[0],pat.L[0]);
  const h0=lpAt(pat.U,b0)-lpAt(pat.D,b0);
  // FLAG vs CHANNEL — the research is explicit and it is not a judgement call:
  //   a FLAG is a short consolidation that follows a sharp near-vertical move (the FLAGPOLE) and slopes AGAINST
  //   it; a CHANNEL is a longer structure with repeated touches and no pole behind it.
  //   It also changes the target: a flag projects the POLE's height from the breakout, a channel its own height.
  // No free parameters: the pole is the net move over the same number of bars immediately BEFORE the structure,
  // it must be taller than the structure itself, and the structure must lean against it.
  let kind=pat.type, pole=null;
  if(pat.type==='channel'){
    const len=n-b0, pa=Math.max(0,b0-len);
    if(len>=3 && pa<b0){
      const mv=bars[b0][4]-bars[pa][4], slope=(pat.U.m+pat.D.m)/2;
      if(Math.abs(mv)>h0 && slope*mv<0){ kind='flag'; pole=Math.abs(mv); }
    }
  }
  return { type:kind, pole:pole, status:st, brkT:brkAt==null?null:bars[brkAt][0],
           est:bars[pat.est]?bars[pat.est][0]:null, height:(w>0?w:null),
           // the measured move: a flag projects the POLE, a triangle or channel its own height at its widest
           h0:(kind==='flag'&&pole>0)?pole:(h0>0?h0:null),
           brkPx:(brkAt==null?null:(st==='above'?lpAt(pat.U,brkAt):lpAt(pat.D,brkAt))),
           // the apex is in the FUTURE, so there is no bar for it: extrapolate the timestamp instead. The old
           // Math.min(n,...) clamp made this the LAST BAR on 61 of 61 rows.
           apexT:(pat.apex!=null&&bars.length>1)?bars[n][0]+Math.round(pat.apex)*(bars[n][0]-bars[n-1][0]):null,
           apexBars:(pat.apex!=null?Math.round(pat.est+pat.apex-n):null),
           u:[bars[pat.U.i1][0], pat.U.p1, pat.U.m], d:[bars[pat.D.i1][0], pat.D.p1, pat.D.m],
           t0:bars[Math.min(pat.H[0],pat.L[0])][0] };   // draw from the OLDEST PIVOT so the shape shows the points it was fitted to
}
// 1m -> tfMin OHLC buckets (the still-forming bucket dropped); the OHLC twin of tfCloses.
function tfOHLC(B,tfMin,nowMs){
  const span=tfMin*60000, n=B.length; if(!n) return [];
  const openB=formingOpen(B[n-1][0], span, nowMs), m=new Map();
  for(let i=0;i<n;i++){ const b=Math.floor(B[i][0]/span)*span; if(b>=openB) continue;
    let x=m.get(b); if(!x) m.set(b,[b,B[i][1],B[i][2],B[i][3],B[i][4],+B[i][5]||0,1]);
    else { if(B[i][2]>x[2])x[2]=B[i][2]; if(B[i][3]<x[3])x[3]=B[i][3]; x[4]=B[i][4]; x[5]+=+B[i][5]||0; x[6]++; } }
  return [...m.keys()].sort((a,b)=>a-b).map(b=>m.get(b));
}
// ---- TRENDY: red/green candle counts (Nate, 2026-09-08) --------------------------------------------------
// "we are just counting red or green candles at different timeframes. well use the most recent 100 candles per
// timeframe. timeframes 5m 10m 15m 30m." Per timeframe: the PERCENT of the last 100 candles that are green (the
// number is green above 50%, red below), how many of the SAME colour in a row right now, and how far price moved
// across those 100 candles. Green = close > open, red = close < open; a doji is neither and counts to neither.
const TRENDY_TFS = [5,10,15,30];
const TRENDY_NS  = [20,40,60,80,100];                             // Nate: "buttons up top so I can select 20, 40, 60, 80, and 100 candles per timeframe"
const TRENDY_MAXN = 100;
function trendyStats(B, nowMs){
  // Only the tail matters, so slice before bucketing: the full 1m array is ~525k bars and four passes over it cost
  // ~131ms per asset for data we throw away.
  const need = TRENDY_MAXN * TRENDY_TFS[TRENDY_TFS.length-1] * 3;   // generous margin for missing buckets
  const tail = B.length > need ? B.slice(-need) : B;
  return TRENDY_TFS.map(tf => {
    const bars = tfOHLC(tail, tf, nowMs);
    return TRENDY_NS.map(N => {
      if(bars.length < N) return null;
      const w = bars.slice(-N);
      let g=0, r=0;
      for(const b of w){ if(b[4]>b[1]) g++; else if(b[4]<b[1]) r++; }
      // "the percent of candles that is RED OR GREEN" — a doji is neither, so it is not in the denominator.
      // Counting it in made the remainder read as red and turned 93 of 528 cells red when green actually led.
      const pct = (g+r) ? +(100*g/(g+r)).toFixed(0) : null;
      const last = w[w.length-1];
      const dir = last[4]>last[1] ? 1 : last[4]<last[1] ? -1 : 0;
      let run = 0;
      if(dir!==0){ for(let i=w.length-1;i>=0;i--){ const d = w[i][4]>w[i][1] ? 1 : w[i][4]<w[i][1] ? -1 : 0; if(d!==dir) break; run++; } }
      const pnl = w[0][1]>0 ? +(((last[4]/w[0][1])-1)*100).toFixed(2) : null;   // first candle's OPEN to the last candle's CLOSE
      return { pct, run, dir, pnl };
    });
  });
}
function liferPatterns(B, S){
  S = S || tfSeries(B, LIFER_ALL_TFS);                                 // shares the series liferCross already built instead of rebuilding all of them
  return LIFER_TFS.map(tf => { const bars=S[tf]; return bars.length<10 ? null : lpReplay(bars); });
}

function computeMetrics(B, C, btcSign, prior){
  const Cc=clean(B), n=Cc.length;
  const floor=C.thr[0]/100;                                                    // class MIN BASE = the noise floor (5% crypto/stocks, 3% metals...)
  const totalMin=Math.round((B[B.length-1][0]-B[0][0])/60000);
  const lastTm=Cc[n-1][0], lastC=Cc[n-1][4], cut30=lastTm-30*DAY;
  const baseIdx=C.thr.indexOf(C.base);
  // ---- MOVES, per PULLBACK FILTER. The dual-gate retraceZig (floor = class min base, NO speed filter) replaces
  // oaPivots/oaFiltered/moveSplit. For each pf, at EVERY move size on the ladder: count, total-move %, in-move/not-in
  // minutes, avg gap, 30d participation. The Oct-10 market-wide crash is NOT skipped — under the dual-gate it's a
  // bounded real move, exactly as the chart counts it (same Cc + retraceZig + floor as viz/chart.html -> numbers match).
  // the SLOW half, carried forward when recompute says this instrument already has one
  const reuse = prior && prior.pf ? prior : null;
  const pf = reuse ? reuse.pf : {};
  if(!reuse) for(const P of PFS){
    const piv=retraceZig(Cc, P/100, floor);
    const sw=[]; for(let k=1;k<piv.length;k++){ const a=piv[k-1], b=piv[k]; sw.push({aI:a.i,bI:b.i,abs:Math.abs(b.p-a.p)/a.p}); }   // swing %s between consecutive pivots
    const counts=[], byBase=[];
    for(const t of C.thr){
      const thr=t/100; const inArr=new Array(n).fill(false); let cnt=0, tot=0;
      for(const s of sw){ if(s.abs>=thr){ cnt++; tot+=s.abs; for(let i=s.aI;i<=s.bI;i++)inArr[i]=true; } }                          // a swing is a COUNTED MOVE if >= this size; mark its minutes "in"
      let inMove=0,gaps=0,pIn=true,fIn=0,fTot=0;                                                                                   // one pass: in-move mins, # not-in stretches (gaps), 30d participation
      for(let j=0;j<n;j++){ const on=inArr[j]; if(on)inMove++; if(!on&&pIn)gaps++; pIn=on; if(Cc[j][0]>=cut30){ fTot++; if(on)fIn++; } }
      const notIn=n-inMove;
      counts.push(cnt);
      byBase.push({ tpct:+(tot*100).toFixed(1), inMoveMin:inMove, notInMin:notIn, avgGapMin:gaps?Math.round(notIn/gaps):0, flexPct:fTot?Math.round(100*fIn/fTot):0 });
    }
    let mvDir=0,mvMag=0,mvDur=null;                                                                                                // current in-progress move at the DEFAULT base, off this pf's last pivot
    if(piv.length){ const ref=piv[piv.length-1].p, disp=(lastC-ref)/ref; if(Math.abs(disp)>=C.base/100){ mvDir=disp>0?1:-1; mvMag=+(Math.abs(disp)*100).toFixed(1); mvDur=lastTm-piv[piv.length-1].t; } }
    const bb=byBase[baseIdx];
    pf[P]={ counts, byBase, mvDir, mvMag, mvDur, tpct:bb.tpct, avgGapMin:bb.avgGapMin, inMoveMin:bb.inMoveMin, notInMin:bb.notInMin, flexPct:bb.flexPct };
  }
  // ---- pf-INDEPENDENT fields (price / 7d range / volume / daily %) — verbatim from the prior logic ----
  const price=Cc[n-1][4];
  const cut7=lastTm-7*DAY; let hi7=-Infinity,lo7=Infinity,wStart=n;
  for(let i=n-1;i>=0 && Cc[i][0]>=cut7;i--){ if(Cc[i][2]>hi7)hi7=Cc[i][2]; if(Cc[i][3]<lo7)lo7=Cc[i][3]; wStart=i; }                // rolling 7-day high/low
  const range7=lo7>0?Math.round(100*(hi7-lo7)/lo7):0, wOpen=Cc[wStart][4];
  const pctDir=((hi7+lo7)/2>=wOpen)?1:-1, woDir=(price>=wOpen)?1:-1;                                                               // % color = range-mid vs weekly open; wo arrow = price vs weekly open
  const volType=(C.key==="crypto"||C.key==="usstock")?"volume":(["forex","metal","oil","indices"].includes(C.key)?"ticks":null);
  let vb=null, vbW=null;
  if(volType){ vbW=(C.key==="crypto")?5*60000:60*60000; const cut=B[B.length-1][0]-30*3600000; vb={}; for(const r of B){ if(r[0]<cut)continue; const b=Math.floor(r[0]/vbW); vb[b]=(vb[b]||0)+r[5]; } }   // crypto 5-min / stocks+forex 1-hr buckets, ~30h
  const dayPct=dayChangePct(Cc, C.key);                                                                                           // daily % change, per-class convention
  // crypto "BTC corr": % of 5m bars closing the SAME direction as BTC, size ignored. btcSign passed by recompute
  // (null for BTC itself + non-crypto). Trailing-year value is stable bar-to-bar, so caching it with the asset's
  // metrics is fine — it refreshes whenever the asset's own file updates (every 5m for active crypto).
  // A perp-routed stock / metal / oil / index (C.perp) is stored closed-bars-only from a 24/7 venue exactly like crypto,
  // so it decides "forming" by the clock too, and it gets the Lifer per-timeframe states -- the Stoch Jackpot lists
  // "crypto or tokenized stock or metals". EMA, Trendy, patterns and the rolling moves stay crypto-only.
  const LIVEK = (C.key==="crypto") || !!C.perp;
  const NOWB = LIVEK ? Date.now() : undefined;   // crypto is stored closed-bars-only, so it decides "forming" by the clock (formingOpen)
  // Nate, 2026-09-26: "still says no ema data on some of them. emas are easy to calculate. fix that." emaCross()
  // itself is genuinely class-agnostic (pure candle-array math, no crypto assumption anywhere in it) -- the ONLY
  // thing that made this crypto-only was this gate. LIVEK (crypto or perp-routed, same universe Lifer/Jackpot
  // already use) is exactly right: emaCross needs 24/7 continuous closed-bar data to decide "still forming"
  // correctly (formingOpen), which is true for crypto AND for a perp-routed stock/metal/oil/index, but NOT for
  // that instrument's own cash-market data (real market-hours gaps would misread as forming/broken bars).
  const emaX = LIVEK ? emaCross(B, NOWB) : null;                                                              // EMA cross list (crypto + perp-routed non-crypto) — see emaCross above
  const liferSer = LIVEK ? tfSeries(B, LIFER_ALL_TFS, NOWB) : null;   // ONE bucketing pass per asset, shared by the cross and the patterns
  const liferX = liferSer ? liferCross(B, liferSer) : null;
  const quiet = (liferSer && liferSer[5]) ? volQuiet(liferSer[5]) : null;   // asset-level, not per-timeframe: "has this asset been quiet" is one fact about the asset
  // PATTERNS: slow on the high timeframes, FAST on the ones Nate trades. Carrying the whole column for an hour
  // left the fast timeframes reading a pattern and a breakout status from up to 60 minutes ago while every other
  // column on the same row was ~2 min fresh, with nothing on screen to say so -- measured against a live table at
  // 25 min of age: 9 of 12 assets had the wrong breakout STATUS at 1m, 8 of 12 at 3m, 6 of 12 at 5m, 2 of 12 at
  // 15m, and 0 at 30m and above. So 1m/3m/5m/15m are recomputed every run (~222 ms/asset measured on btc, about
  // +30 s on a 133-asset pass) and 30m upward keep the hourly shelf life, where the measured error was zero.
  const liferP = (C.key!=="crypto") ? null : (reuse && reuse.liferP && liferSer)
    ? LIFER_TFS.map((tf,i)=> LIFER_PAT_FAST.has(tf) ? (liferSer[tf].length<10 ? null : lpReplay(liferSer[tf])) : reuse.liferP[i])
    : (liferSer ? liferPatterns(B, liferSer) : null);
  const trendy = (C.key==="crypto") ? trendyStats(B, NOWB) : null;                                                        // Trendy tab: red/green candle counts on 5m/10m/15m/30m                                                       // Lifer patterns: triangle / cloned-parallel channel per timeframe                                                          // Lifer: price-vs-EMA21/EMA200 state on 30m/1h/2h/4h/1d (crypto only) — see liferCross above
  let btcCorr = reuse ? reuse.btcCorr : null;
  if(!reuse && btcSign){ const a=sign5(B); let same=0,tot=0; for(const [b,s] of a){ if(s===0)continue; const bs=btcSign.get(b); if(bs===undefined||bs===0)continue; tot++; if(s===bs)same++; } btcCorr = tot? +(same/tot*100).toFixed(1) : null; }
  // ---- 882: the fib retracement leg (Nate, 2026-09-30). Same universe as Lifer/EMA (LIVEK: crypto + perp-routed),
  // because it needs 24/7 closed bars to bucket 5m correctly. PARAMETERS ARE NATE'S, not defaults I picked:
  //   5m bars           -- "no not daily. 5m. this needs more accuracy"
  //   >= 20% leg         -- "since I want this to be big moves lets do min 5% between range top and range bottom"
  //   0.75-day window   -- his choice; measured against the TOWNS chart he sent (the ONLY window that reproduces
  //                        the anchors he drew by hand; 0.5d finds nothing, 1d+ drifts to older, wider legs).
  // retrace=0.96 is FORCED, not chosen: retraceZig re-anchors the moment its own retracement test passes, and that
  // test is the SAME quantity this tab displays -- so any value below .941 makes the top of his band structurally
  // unreachable (measured: 1 of 303 rows ever passed .941 at 0.90, vs 16 of 428 at 0.96). floor is near-inert here
  // because the window is short enough to hold one impulse, so it sits at 0.001 and the 20% gate is applied below.
  const F882 = LIVEK ? (()=>{
    const win = B.slice(Math.max(0, B.length - F882_WIN_1M));
    const b5 = tfOHLC(clean(win), 5, NOWB);
    if(b5.length < 20) return null;
    const piv = retraceZig(b5, F882_RETRACE, F882_FLOOR);
    if(piv.length < 2) return null;
    const A = piv[piv.length-2], Z = piv[piv.length-1];          // A = the leg's ORIGIN (fib 1.0), Z = its END (fib 0)
    const span = Math.abs(Z.p - A.p) / Math.min(A.p, Z.p);
    const lvl = f => Z.p + (A.p - Z.p) * f;
    // c5 = the newest CLOSED 5-minute close (tfOHLC already drops the forming bucket). The 882 tab judges a row IN
    // or OUT on this, not on live ticks: tick-judged membership churned 9.3 entries per asset with a 1-minute median
    // row life, because one stray tick across .786 evicted and the next re-latched (Nate: "assets are popping on and
    // off the list constantly"). On closed 5m bars at the 20% minimum, a 48h replay over all 533 assets gave 19 entries
    // from 7 assets -- 2.7 per asset THAT ENTERED, 0.036 across the whole board -- with a 10-minute median row life
    // and a 39-minute mean.
    // c5 IS RETURNED EVEN WHEN THE LEG NO LONGER QUALIFIES, and that is the point: a row already on the tab is judged
    // against its own LATCHED leg, so all it needs from here is a price. Returning null below the gate instead
    // deleted live rows that had crossed neither level -- measured on crypto MEW, whose leg dipped 21.5% -> 19.5%
    // and whose row was dropped at retracement 0.827 while still approaching .882; it REACHED .882 sixteen minutes
    // later. Nate's rule is "it stays until it passed either .786 or .941", so only those two levels may evict.
    const c5 = b5[b5.length-1][4];
    if(!(span >= F882_MIN_LEG)) return { c5 };                   // no NEW row can start here, but an existing one stays judgeable
    return { a:A.p, at:A.t, z:Z.p, zt:Z.t, dir:(Z.p>A.p?1:-1), span:+(span*100).toFixed(2), c5,
             l786:lvl(0.786), l882:lvl(0.882), l941:lvl(0.941) };
  })() : null;
  const d=pf[DEFAULT_PF];                                                                                                         // hoist default-pf values so the seed + the as-yet-unwired renderer keep working unchanged
  // The rolling moves, crypto only. 12h and 1d are here because the screener needs the three windows Nate named:
  // "moved 5 percent in the last day. or 12 hours. or 4 hours". Each is one binary search over bars already in hand.
  const m1h = (C.key==="crypto") ? moveBack(B,60) : null, m4h = (C.key==="crypto") ? moveBack(B,240) : null;
  const m12h = (C.key==="crypto") ? moveBack(B,720) : null, m1d = (C.key==="crypto") ? moveBack(B,1440) : null;
  return { span:Math.round(totalMin/1440), start:B[0][0], price, hi7, lo7, range7, pctDir, woDir, dayPct, vb, vbW, pf, btcCorr, emaX, liferP, trendy, liferX, quiet, F882, m1h, m4h, m12h, m1d,   // start = first candle ts -> M/YY shown by "short history"
    counts:d.counts, tpct:d.tpct, avgGapMin:d.avgGapMin, inMoveMin:d.inMoveMin, notInMin:d.notInMin, mvDir:d.mvDir, mvMag:d.mvMag, mvDur:d.mvDur, flexPct:d.flexPct, byBase:d.byBase };
}

// ---- market-cap RANK MOVEMENT (CoinGecko; separate from candle data). Current global rank = 1 markets call (cheap,
// every sweep). 30-day-ago rank = each top-200 coin's mcap 30d ago via /coins/{id}/history, by the coin's OWN CoinGecko
// id (reliable — no symbol-collision mapping; CoinPaprika's mapping was rejected). That pull is paced (~12s/call to
// dodge CoinGecko's ~5/min keyless cap) and run ONCE A DAY (the 30d-ago reference date only rolls daily). Stored in
// mcap.json { rank30Date, rank30:{SYM:rank}, ranks:{SYM:{rank,rank30,move}} }; move = rank30 - rankNow (+ = climbed). ----
const CG = "https://api.coingecko.com/api/v3";
async function cgJSON(p, tries=0){ try{ const r=await fetch(CG+p,{headers:{Accept:"application/json"},signal:AbortSignal.timeout(30000)}); if(r.status===429){ if(tries>=5)return null; await sleep(15000); return cgJSON(p,tries+1);} if(!r.ok)return null; return await r.json(); }catch(e){ if(tries>=5)return null; await sleep(3000); return cgJSON(p,tries+1); } }
function dmyUTC(ms){ const d=new Date(ms); return String(d.getUTCDate()).padStart(2,"0")+"-"+String(d.getUTCMonth()+1).padStart(2,"0")+"-"+d.getUTCFullYear(); }
async function updateMcap(dataDir, syms){
  const f=path.join(dataDir,"mcap.json");
  let store={}; try{ store=JSON.parse(fs.readFileSync(f,"utf8")); }catch(e){}
  // ONCE A DAY, not once an hour (Nate, 2026-09-26: "fix them all" / "are we asking for more data than we need").
  // This ran on every hourly sweep: 44,115 bytes gzipped, 24x a day, for three fields (symbol, market_cap_rank, id)
  // whose ranking rolls daily -- the same cadence the rank30Date gate below already keys off. DISCLOSED: the sticky
  // top-100 auto-add below reads the same response, so it now also evaluates once a day instead of hourly.
  const mkDay=dmyUTC(Date.now());
  if(store.marketsDate===mkDay && store.ranks) return store;
  const mk=await cgJSON("/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=250&page=1");
  if(!Array.isArray(mk)) return store.ranks?store:null;                                    // CoinGecko down -> keep last-known ranks
  const now={}; for(const c of mk){ if(c&&c.symbol) now[c.symbol.toUpperCase()]={rank:c.market_cap_rank}; }
  const assemble=(r30)=>{ const out={}; for(const sym of syms){ const S=sym.toUpperCase(); const n=now[S]; if(!n)continue;
    const x=(r30&&r30[S]!=null)?r30[S]:null; out[S]={ rank:(n.rank!=null?n.rank:null), rank30:x, move:(x!=null&&n.rank!=null)?(x-n.rank):null }; } return out; };   // +move = climbed up the rankings over 30d
  store.rank30 = store.rank30 || {};
  store.ranks = assemble(store.rank30); store.asOf=Date.now(); store.marketsDate=mkDay;
  try{ writeAtomic(f, JSON.stringify(store)); }catch(e){}                                    // write CURRENT ranks immediately (move uses cached 30d-ago ranking if present)
  // STICKY auto-add: any coin now in the global top-100 that isn't already in our pool, isn't excluded, and is
  // tradeable (Binance or KuCoin) -> add it to the membership. Next sweep's buildClasses picks it up + downloads it.
  try{
    const mem=loadMembership(dataDir); const have=new Set(mem.coins.map(s=>s.toLowerCase()));
    const mcAdded=[];   // applied to a FRESH load below, not to this object -- minutes pass before the write
    const taken=new Set();                                                                   // every symbol the NON-crypto classes already own
    for(const C of buildClasses(dataDir)) if(C.key!=="crypto") for(const t of (C.syms||[])) taken.add(String(t).toLowerCase());
    for(const c of mk.slice(0,165)){ const sl=(c.symbol||"").toLowerCase();   // scan to ~the pool floor (#164): a tradeable+non-excluded coin this high that isn't a member just climbed into the qualifying top-100
      if(!sl || have.has(sl) || poolExcluded(sl) || !/^[a-z0-9]+$/.test(sl)) continue;
      if(!onYubit(sl)) continue;                                                             // NOT LISTED ON YUBIT -> not tradeable by hand -> never goes on the board
      if(taken.has(sl)) continue;                                                            // a tradfi class already owns this ticker: adding it would collide on the flat data dir
      const src=await tradeSrc(sl.toUpperCase()); if(src){ mem.coins.push(sl); mcAdded.push([sl,src]); have.add(sl); }   // grow-only; never removes
    }
    // Same re-read as the KuCoin re-test, for the same reason and in the other direction: this loop paces a probe per
    // candidate, so minutes pass between the load above and this write. The crypto lane can have demoted a symbol in
    // that window, and writing the stale `mem` would silently put it back on KuCoin.
    if(mcAdded.length){
      const memW=loadMembership(dataDir);
      for(const [sl,src] of mcAdded){ if(!memW.coins.includes(sl)) memW.coins.push(sl); if(src==="KUCOIN") memW.src[sl]="KUCOIN"; }
      saveMembership(dataDir, memW);
    }
  }catch(e){}
  const dateNeeded=dmyUTC(Date.now()-30*DAY);
  if(store.rank30Date!==dateNeeded){                                                         // the 30d-ago date rolled -> rebuild that ranking (ONCE A DAY, ~40 min paced)
    const caps=[];
    for(const c of mk.slice(0,200)){ const h=await cgJSON("/coins/"+c.id+"/history?date="+dateNeeded+"&localization=false"); await sleep(12000);
      const mc=h&&h.market_data&&h.market_data.market_cap&&h.market_data.market_cap.usd; if(mc>0) caps.push({sym:c.symbol.toUpperCase(),cap:mc}); }
    if(caps.length>=50){ const r30={}; caps.sort((a,b)=>b.cap-a.cap); caps.forEach((x,i)=>r30[x.sym]=i+1);   // only commit if the pull mostly succeeded
      store.rank30=r30; store.rank30Date=dateNeeded; store.ranks=assemble(r30); store.asOf=Date.now(); try{ writeAtomic(f, JSON.stringify(store)); }catch(e){} }
  }
  return store;
}

// ---- recompute the table from all instrument files -> engine_data.json (cache-accelerated; same numbers) ----
// ⚠ INVARIANT THE TWO-LANE SCHEDULER DEPENDS ON: recompute() is UNFILTERED — it rebuilds every class from the
// candle files on disk, every call. That is what makes it safe for the crypto lane and the tradfi lane to publish
// engine_data.json concurrently: whichever writes last publishes a COMPLETE table built from the freshest files on
// disk, so last-writer-wins can never drop a class. Adding an `only` filter here would silently break that.
// The ONLY place a carried EMA payload is aged. Both the carry branch and the re-read-before-write call it, because
// a payload that skips it republishes a Valid row that can never die.
function emaExpire(E){
  if(!E) return E;
  const now=Date.now();
  // XROWS TOO (Nate, 2026-09-26: "fix them all"). This mapped only E.rows, so the wall-clock expiry -- which the
  // carry-path comment below calls the only thing that can expire a row on a FROZEN FEED -- never ran on the
  // perp-routed non-crypto xrows added in 1.30.12. Those xrows are the Zones tab's ONLY EMA source, and a dead
  // venue feed (usstock BIDU's is dead) would have served a stale cross forever.
  const age = rows => rows.map(r => (!r || !Array.isArray(r.st)) ? r
    : Object.assign({}, r, { st: r.st.map((x,i) => (x && !(x.validAt!=null && now-x.validAt>=emaLifeMs(EMA_TFS[i]))) ? x : null) }));
  const out = {};
  if(Array.isArray(E.rows))  out.rows  = age(E.rows);
  if(Array.isArray(E.xrows)) out.xrows = age(E.xrows);
  return Object.assign({}, E, out);
}
const SLOW_MS = 3600000;   // the shelf life of the slow half = the app's existing hourly sweep cadence (main.js)
// ...but STAGGERED, per instrument. Every entry is stamped in the same instant by a cold start or a CACHE_VER bump,
// so an hour later they all expire on ONE 60 s tick. Measured in the live cache: 106 of 349 entries land in a single
// minute, and the slow half costs 553 ms/asset (50% of a cold compute), so that tick carries +59 s serial / +22 s
// pooled -- on a tick that is 60 s long. So give each instrument its own shelf life in [SLOW_MS/2, SLOW_MS], derived
// from its file key: deterministic, so the pool pre-pass and the inline path below always agree on the same answer
// for the same asset (they must -- disagreeing would make the pooled table differ from the single-threaded one), and
// stable across runs so a restart cannot re-cluster them. The CEILING stays SLOW_MS: the slow half is never staler
// than the hourly sweep it is pinned to. Only the floor moves, which spreads the expiries across ~30 ticks.
function slowMsOf(fk){
  let h=2166136261;                                                   // FNV-1a: no dependency, same answer everywhere
  for(let i=0;i<fk.length;i++){ h^=fk.charCodeAt(i); h=Math.imul(h,16777619); }
  return (SLOW_MS>>1) + ((h>>>0) % (SLOW_MS>>1));
}
// refreshing = the set of class KEYS this run actually downloaded (null = all of them, the old behaviour). A class
// outside it is carried from the published table rather than recomputed -- see the note above.
// opts.perpOwner === false: this run does NOT download the perp-routed instruments (the hourly lane). Their files change
// every minute in the other lane, so recomputing them here was pure waste (a cache miss for all of them on every
// rebuild); their rows and Jackpot rows are carried from the published table instead.
function recompute(dataDir, classes, cache, refreshing, opts){
  const perpOwner = !(opts && opts.perpOwner === false);
  cache = cache || {};
  const NOW = Date.now();
  // A class we can't refresh keeps its LAST-KNOWN row instead of blanking. BASE = the BUNDLED seed (ships inside
  // the app, always readable — this is what makes a not-yet-downloaded class, e.g. keyless stocks, never blank even
  // if no engine_data.json was ever written to disk). Then OVERLAY the on-disk table so freshly-computed values
  // win. Used ONLY for instruments we would otherwise DROP (missing/short/corrupt file) — never alters a freshly
  // computed row, so a fully-downloaded app is byte-identical to before.
  let prev={};
  try{ const sj=JSON.parse(fs.readFileSync(path.join(__dirname,"seed_engine_data.json"),"utf8")); if(sj&&sj.classes)for(const k in sj.classes){prev[k]={};for(const r of (sj.classes[k].rows||[]))prev[k][r.a]=r;} }catch(e){}
  let prevTop=null;   // the whole published table, kept so a non-refreshed class can carry its side payloads too
  try{ const pj=JSON.parse(fs.readFileSync(path.join(dataDir,"engine_data.json"),"utf8")); if(pj&&pj.classes&&pj.metricsVer===CACHE_VER){ prevTop=pj; for(const k in pj.classes){ if(!prev[k])prev[k]={}; for(const r of (pj.classes[k].rows||[]))prev[k][r.a]=r;} } }catch(e){}   // overlay on-disk ONLY if it was computed with the SAME calc version; a stale-calc on-disk table is ignored so un-refreshable instruments (e.g. keyless stocks) fall back to the correct-calc seed, not the old values
  // ---- PARALLEL PRE-PASS: compute every cache MISS across worker threads, then let the loop below hit cache ----
  // BTC's 5m direction series, read at most ONCE per recompute and shared by the pool pre-pass and the class loop.
  // It used to be computed inside the class loop, which was fine until the pre-pass needed to know whether the read
  // SUCCEEDED (see the btcOk note below) -- the pre-pass runs first, so the answer has to exist before the loop.
  // Lazy, so a recompute with no crypto class never pays for the read. Memoised on FIRST CALL including the null
  // result, because "the parent could not read btc.json" is the very fact the pre-pass and the loop must agree on.
  // _btcKey is derived from the SERIES ACTUALLY USED (bar count + last bar time), not from a file stamp. A stamp
  // taken before the read names the generation on disk at that instant, while the content the parent goes on to use
  // may be the next one -- a measured ~300 ms window (stamp 0.2-0.35 ms, then read+sign5 298-330 ms) in which a
  // worker holding the older map would match the key and serve it while the parent used the newer. Re-ordering the
  // stamp only mirrors the hole. A key computed from the bars themselves cannot disagree with the bars themselves.
  let _btcSign=null, _btcKey="0:0", _btcDone=false;
  const btcSignOnce = () => {
    if(_btcDone) return _btcSign;
    _btcDone=true;
    try{ if(fs.existsSync(CS.basePath(dataDir,"btc"))){
      const bc=CS.read(dataDir,"btc",okBar).candles;   // base+log: reading the base alone left the BTC reference up to a full day behind every asset it is correlated against
      if(Array.isArray(bc) && bc.length){ _btcSign=sign5(bc)||null; _btcKey=bc.length+":"+bc[bc.length-1][0]; }
    } }catch(e){ _btcSign=null; _btcKey="0:0"; }
    return _btcSign;
  };
  const btcKeyOnce = () => { btcSignOnce(); return _btcKey; };
  if(POOL){
    const tasks=[], meta=[];
    // The worker derives btcSign itself (a ~105k-entry map, far too big to clone per task) and keeps it, so it has
    // to be told WHICH btc it should be holding. Without this the worker serves whatever btc.json it happened to
    // read first while the parent re-reads it on every recompute -- and afterClass calls recompute several times in
    // one run, so a btc.json that changes or briefly cannot be read mid-run made the pooled table differ from the
    // inline one (measured: sol/doge/shib got a stale numeric btcCorr where inline produced null). btcKey is the
    // generation, btcOk is whether the PARENT could read it, and between them the worker's answer is always the
    // same generation the inline path would have used.
    // Both inside the crypto guard: a recompute with no crypto class must not pay for the btc read at all.
    const anyCrypto = classes.some(C => C.key==="crypto");
    const btcOk = !!(anyCrypto && btcSignOnce());
    const btcKey = anyCrypto ? btcKeyOnce() : "0:0";
    for(const C of classes){
      if(refreshing && refreshing.size && !refreshing.has(C.key)) continue;        // carried class: nothing to compute
      for(const sym of C.syms){
        const fk=dfile(C.key, sym);
        if(!fs.existsSync(path.join(dataDir, fk+".json"))) continue;
        if(!perpOwner && C.key!=="crypto" && perpOf(C.key, sym)) continue;             // owned by the minute lane
        const mt=CS.stamp(dataDir, fk);
        const ent=cache[fk];
        if(ent && ent.mtime===mt) continue;                                        // already fresh
        const slowFresh = !!(ent && ent.m && ent.m.pf && ent.slowAt && (NOW - ent.slowAt) < slowMsOf(fk));
        tasks.push({ dataDir, fk, C:{key:C.key, thr:C.thr, base:C.base, perp:!!perpOf(C.key, sym)}, priorM: slowFresh ? ent.m : null, needBtc: !slowFresh, btcKey, btcOk });
        meta.push({ fk, mt, slowAt: slowFresh ? ent.slowAt : NOW });
      }
    }
    if(tasks.length){
      let res=null; try{ res=POOL.run(tasks); }catch(e){ res=null; }
      if(res) for(let i=0;i<meta.length;i++){
        const r=res[i]; if(!r || !r.m) continue;                                    // not done -> the loop computes it inline
        cache[meta[i].fk]={mtime:meta[i].mt, lastT:r.lastT, m:r.m, slowAt:meta[i].slowAt};
        markDirty(cache, meta[i].fk);
      }
    }
  }
  const carried=new Set();                 // class keys this run carried rather than computed -- see the re-read before the write
  const carriedSyms=Object.create(null);   // and exactly WHICH symbols were carried, so the re-read cannot touch the rest
  let mcap={}; try{ mcap=(JSON.parse(fs.readFileSync(path.join(dataDir,"mcap.json"),"utf8")).ranks)||{}; }catch(e){}   // market-cap rank+30d-move per symbol (crypto only), attached to rows below
  const xLifer=[];   // Lifer states of the perp-routed NON-crypto instruments -> DATA.lifer.xrows (Stoch Jackpot only; the Lifer tab stays crypto)
  // 882 legs for EVERY asset in one flat list, each row carrying its OWN cls+sym. Deliberately NOT the
  // rows/xrows split the EMA and Lifer payloads use: that split is keyed by bare symbol on one side and xKey()
  // on the other, which is exactly what served Convex Finance's numbers under Chevron's row. A new tab has no
  // legacy to preserve, so it gets an unambiguous key from the start.
  const f882=[];
  const pubF882=(cls,sym)=>{ const F=prevTop&&prevTop.f882; return F&&Array.isArray(F.rows) ? F.rows.find(r=>r.cls===cls&&r.sym===sym) : null; };
  const xEma=[];     // EMA-cross states of the perp-routed NON-crypto instruments -> DATA.ema.xrows (Zones confluence; the EMA tab stays crypto), same shape/lifecycle as xLifer
  const perpCarried=Object.create(null);   // class -> symbols whose rows this (non-owner) run took from the published table
  const pubXrow=a=>{ const L=prevTop&&prevTop.lifer; return L&&Array.isArray(L.xrows) ? L.xrows.find(r=>r.a===a) : null; };
  const pubXEma=a=>{ const E=prevTop&&prevTop.ema; return E&&Array.isArray(E.xrows) ? E.xrows.find(r=>r.a===a) : null; };
  const out = { perpErrors: PERP_ERRS!=null ? PERP_ERRS.slice(0,50) : ((prevTop && prevTop.perpErrors) || []), window:"1y", volWin:3, metricsVer:CACHE_VER, classOrder:classes.map(c=>c.key), classes:{}, notFresh:{}, noSource:{} };   // metricsVer stamps the calc version so loadData (first paint) + this overlay can detect a stale-calc on-disk table after an upgrade   // notFresh: per-class list of instruments still on SEED data (no fresh local candle file) -> drives the "not yet downloaded" status so a stalled class can't masquerade as current
  for (const C of classes){
    const baseIdx=C.thr.indexOf(C.base);
    const volType=(C.key==="crypto"||C.key==="usstock")?"volume":(["forex","metal","oil","indices"].includes(C.key)?"ticks":null);
    const btcSign = (C.key==="crypto") ? btcSignOnce() : null;   // crypto only: BTC's 5m direction series, computed ONCE per recompute (see btcSignOnce) so each alt's row.btcCorr = % of 5m bars it closes the SAME way as BTC
    const rows=[]; const stale=[];
    // NO DATA SOURCE vs NOT YET DOWNLOADED. symsOf() drops a Dukascopy-class symbol that has no DUKA_MAP id from the
    // download entirely -- silently, with no error and no retry -- so GASOLINE and WHEAT would sit in notFresh as
    // "pending" FOREVER, which reads as "still coming" when nothing is coming. Nate asked for them on the board
    // anyway; this is what makes their absence legible instead of a permanent false pending.
    // Two different questions, because the two source kinds fail differently.
    //   DUKA  -- structural. No DUKA_MAP id means symsOf drops it from the download entirely; it can never arrive.
    //   CRYPTO / STOCK -- observational, and it has to be. A delisted Polygon ticker returns j.status!=="OK", the
    //     loop breaks, fetchStock returns [], and updateInstrument reports {status:"ok", added:0}: indistinguishable
    //     from a healthy symbol with nothing new, by return value alone. So the evidence is an ACTUAL ATTEMPT that
    //     produced nothing AND left no file behind (NO_BARS, filled by the download loop). A symbol the sweep has
    //     not reached yet is never in that set, which is what stops a newly-added instrument being mislabelled --
    //     the first version of this test used "has no file while its class-mates do" and, measured against the live
    //     data dir, would have condemned 84 stocks and 4 coins that were merely still queued.
    // A lane that did not ATTEMPT a symbol this run (the other lane owns it) keeps the published verdict, so the two
    // lanes stop alternating "no source" and "pending" for the same instrument.
    const prevNoSrc = new Set((prevTop && prevTop.noSource && prevTop.noSource[C.key]) || []);
    const obs = s => NO_BARS.has(C.key+":"+s) || (!ATTEMPTED.has(C.key+":"+s) && prevNoSrc.has(s.toUpperCase()));
    const noSrc = (C.group==="duka")
      ? C.syms.filter(s => perpOf(C.key, s) ? obs(s) : (!DUKA_MAP[s] && !BINANCE_IN_DUKA[s])).map(s=>s.toUpperCase())
      : C.syms.filter(obs).map(s=>s.toUpperCase());
    const noSrcSet = new Set(noSrc);
    const emaList=[];                                                                             // crypto only: the assets currently pending or valid on the 15m EMA cross
    const liferList=[], trendyList=[];                                                                           // crypto only: every asset, with its long/short/inbetween state on each Lifer timeframe
    const keep=A=>{
      // An ALIASED instrument has no trustworthy last-known row: every prior row for it -- seed or on-disk -- was
      // computed from the OTHER class's candle file. Carrying one forward is what kept Convex Finance on the
      // Stocks tab, so it is reported not-fresh and left OUT of the table until its OWN file computes.
      if(isAliased(C.key, A)){ stale.push(A); return; }
      const p=prev[C.key]&&prev[C.key][A]; if(p){ rows.push(p); stale.push(A); } };   // preserve last-known row; record it as NOT FRESH (no local candle file -> never freshly downloaded this build)
    // A class this run did not download: carry its published rows instead of re-deriving them from candles. Guarded
    // on prevTop so a first run (or a stale-calc table) still computes everything.
    const carry = !!(refreshing && refreshing.size && !refreshing.has(C.key) && prevTop && prevTop.classes && prevTop.classes[C.key]);
    // Rows for a carried class come from the PUBLISHED table only -- never from prev, which is the bundled seed
    // overlaid by it. A seed row published as current is how Convex Finance reached the Stocks tab.
    if(carry) carried.add(C.key);
    const pub = carry ? Object.create(null) : null;
    if(carry) for(const r of (prevTop.classes[C.key].rows||[])) pub[r.a]=r;
    for (const sym of C.syms){
      const A=sym.toUpperCase();
      const fk=dfile(C.key, sym);
      const f=path.join(dataDir, fk+".json");
      // An aliased instrument is refused here exactly as keep() refuses it: its only prior rows were computed from
      // the OTHER class's candle file, so carrying one re-creates the cross-class bug.
      if(carry && !isAliased(C.key, A)){ const pr=pub[A]; if(pr){ rows.push(pr); (carriedSyms[C.key]||(carriedSyms[C.key]=new Set())).add(A);
        const cf=pubF882(C.key, A); if(cf) f882.push(cf);   // carried class keeps its 882 legs too, or they drop off the tab every publish
        continue; } }
      if(!perpOwner && C.key!=="crypto" && perpOf(C.key, sym) && prevTop && prevTop.classes && prevTop.classes[C.key]){
        const pr=(prevTop.classes[C.key].rows||[]).find(r=>r.a===A);
        if(pr){ rows.push(pr); (perpCarried[C.key]||(perpCarried[C.key]=new Set())).add(A);
                const xr=pubXrow(xKey(C.key, sym)); if(xr) xLifer.push(xr);
                const xe=pubXEma(xKey(C.key, sym)); if(xe) xEma.push(xe);
                const xf=pubF882(C.key, A); if(xf) f882.push(xf); continue; }
      }
      if(!fs.existsSync(f)){ keep(A); continue; }                                     // keep() itself refuses to carry an aliased instrument (above)
      const mt=CS.stamp(dataDir, fk);   // MUST cover the append log. An append no longer changes the base's mtime, so a
                                        // base-only key would hit forever and freeze every number in the app.
      let m;
      if(cache[fk] && cache[fk].mtime===mt){ m=cache[fk].m; }                            // file unchanged -> reuse metrics
      else { let B; try{ B=CS.read(dataDir, fk, okBar).candles; }catch(e){ keep(A); continue; } B=Array.isArray(B)?B:null; if(!B||B.length<50){ keep(A); continue; }
        // The crypto lane runs every minute and every fetch rewrites every file, so this branch is taken for all
        // 133 assets on every single run -- which is why a 5-minute cross waited on a full year of ZigZag move
        // counts being recounted. Carry the SLOW half forward and recompute only the fast one. SLOW_MS is the
        // app's OWN hourly sweep cadence (main.js), not a new number: the slow half refreshes on the same clock
        // the slow lane already runs on.
        const ent=cache[fk];
        const slowFresh = !!(ent && ent.m && ent.m.pf && ent.slowAt && (NOW - ent.slowAt) < slowMsOf(fk));
        m=computeMetrics(B, perpOf(C.key, sym) ? Object.assign({}, C, {perp:true}) : C, (btcSign && sym.toLowerCase()!=="btc")?btcSign:null, slowFresh ? ent.m : null);
        cache[fk]={mtime:mt, lastT:B[B.length-1][0], m, slowAt: slowFresh ? ent.slowAt : NOW}; markDirty(cache,fk); }
      const v=deriveVol(m.vb, m.vbW);
      const _pp=prev[C.key]&&prev[C.key][A];                                                       // last-known row (seed overlaid by prior on-disk @ same calc ver)
      const volPos=(v.volPos!=null)?v.volPos:((_pp&&_pp.volPos!=null)?_pp.volPos:null);            // HOLD the last-known gauge value when this refresh is stale/null (coin's data >~25min old) instead of blanking; snaps to a fresh value the moment one computes. Nate 2026-06-18: "dont let it go away, the volume gauge is still relevant."
      const row={a:A,span:m.span,start:m.start,counts:m.counts,tpct:m.tpct,avgGapMin:m.avgGapMin,inMoveMin:m.inMoveMin,notInMin:m.notInMin,mvDir:m.mvDir,mvMag:m.mvMag,mvDur:m.mvDur,price:m.price,hi7:m.hi7,lo7:m.lo7,range7:m.range7,pctDir:m.pctDir,woDir:m.woDir,flexPct:m.flexPct,dayPct:m.dayPct,pf:m.pf,volPos:volPos,btcCorr:m.btcCorr};   // pf = per-pullback-filter {25,50,75,90} move data for the table's filter + move-size buttons
      if(C.key==="crypto" && mcap[A]){ row.mcRank=mcap[A].rank; row.mcRank30=mcap[A].rank30; row.mcMove=mcap[A].move; }   // current global rank + 30d ▲/▼ (crypto only)
      if(m.emaX){                                                             // kept OFF the row so the shipped seed never carries a stale live signal
        // WALL-CLOCK EXPIRY, per timeframe, and it must stay wall-clock. emaState (line ~317) already expires the
        // state against lastT, the asset's own data time, so re-asking THAT question here is a tautology that can
        // never fire -- 0 of N, for any N, by construction, not by measurement. This pass exists for the case
        // data time cannot cover: the metrics cache is keyed on file mtime, so a row can be served from a run that
        // happened long ago, and a symbol whose feed stops never rewrites its file at all -- its data time freezes
        // and a valid row would sit on the list forever. Only a value that ADVANCES can expire that row.
        const st=m.emaX.st.map((x,i) => (x && !(x.validAt!=null && Date.now()-x.validAt>=emaLifeMs(EMA_TFS[i]))) ? x : null);
        // EVERY CRYPTO ASSET IS ON THE PAYLOAD (Nate, 2026-09-12: "all cypto assets should be added."). It used to
        // require st.some(Boolean) -- a state on at least one of the nine timeframes -- which withheld the whole row,
        // reference readouts included: around 127-129 of 133 at any moment on 2026-09-12, and only 28-68 of those
        // reached the screen on any one trigger. The pending/valid state is still exactly as computed, per timeframe; an asset with no live
        // cross now simply shows blank status cells beside its 9/16 - 12/21 directions and its % windows.
        if(C.key==="crypto"){
          emaList.push({a:A, price:m.price, st, s916:m.emaX.s916, s1221:m.emaX.s1221, pct:m.emaX.pct, ref:m.emaX.ref});
        } else if(perpOf(C.key, sym)){
          // Nate, 2026-09-26: "fix that" (no EMA data for non-crypto). Same xLifer/Jackpot pattern: keyed by
          // xKey to dodge cross-class symbol collisions (e.g. crypto CVX vs stock CVX), tagged with cls/sym so the
          // merge below can tell which class computed it. The EMA TAB itself stays crypto-only (unchanged, per
          // emaList above) -- this is additive, only reaches the Zones confluence check via DATA.ema.xrows.
          xEma.push({a:xKey(C.key, sym), cls:C.key, sym:A, price:m.price, st, s916:m.emaX.s916, s1221:m.emaX.s1221, pct:m.emaX.pct, ref:m.emaX.ref});
        }
      }
      if(C.key==="crypto" && m.liferX) liferList.push({a:A, price:m.price, st:m.liferX, q:m.quiet!=null?m.quiet:null, pat:m.liferP||null, m1h:m.m1h, m4h:m.m4h, m12h:m.m12h, m1d:m.m1d});
      if(m.F882) f882.push(Object.assign({cls:C.key, sym:A, price:m.price}, m.F882));   // 882 tab: one flat row per asset, keyed cls+sym
      if(C.key!=="crypto" && m.liferX && perpOf(C.key, sym)) xLifer.push({a:xKey(C.key, sym), cls:C.key, sym:A, price:m.price, st:m.liferX});
      if(C.key==="crypto" && m.trendy) trendyList.push({a:A, tf:m.trendy});           // no expiry and no filtering — a Lifer row is a STATE, so every crypto asset is always on the list

      rows.push(row);
    }
    rows.sort((x,y)=>y.counts[baseIdx]-x.counts[baseIdx]);
    out.classes[C.key]={label:C.label,thresholds:C.thr,base:C.base,volType,rows};
    // CARRIED CLASS: the three crypto side payloads come from the published table, not from the (empty) lists.
    if(C.key==="crypto" && carry){
      // The wall-clock expiry lives in the compute branch, so re-apply it here or a Valid row served from a carried
      // payload never dies -- which is the one thing that comment says the pass exists to prevent.
      if(prevTop.ema) out.ema=emaExpire(prevTop.ema);
      if(prevTop.lifer)  out.lifer=prevTop.lifer;
      if(prevTop.trendy) out.trendy=prevTop.trendy;
    }
    if(C.key==="crypto" && !carry) out.ema={tfs:EMA_TFS, lifeMs:EMA_TFS.map(emaLifeMs), asOf:Date.now(), rows:emaList};   // the EMA tab's whole payload, rebuilt from scratch every recompute
    if(C.key==="crypto" && !carry) out.lifer={tfs:LIFER_TFS, asOf:Date.now(), rows:liferList, ku:[...kuActive]};   // ku = the symbols this engine ROUTED to KuCoin, so the renderer's live kline overlay streams them from KuCoin.
    // It does NOT mean "absent from Binance", and must not be read that way. NOTHING ever removes an entry: both
    // writers only add -- the tradeSrc auto-add in updateMcap above, and loadMembership's seed. (It shrinks in exactly
    // one situation: a lost or corrupt membership.json, or one with an empty coins array, resets src to the 25 seeds
    // and drops every auto-added entry; a file carrying coins but no src empties it outright.) And tradeSrc treats ANY
    // Binance failure as a miss -- its probe sits in an empty catch with a 20s timeout -- so a timed-out or
    // rate-limited request AT THE MOMENT A COIN IS FIRST ADDED routes a Binance-listed symbol here for good. Only a
    // first add is exposed: the loop skips anything already in `have`, so an existing member is never re-probed.
    // Over a third of the current entries came in through that auto-add rather than the seed. So treat this as "fetch
    // its candles from KuCoin", never as "Binance does not have it": excluding these from the renderer's live PRICE
    // subscription on that assumption was tried and reverted (see startLive in index.html). The separate 1m KLINE split
    // in lkStart still stands and should -- that one routes by where an asset's candles actually come from.
    if(C.key==="crypto" && !carry) out.trendy={tfs:TRENDY_TFS, ns:TRENDY_NS, asOf:Date.now(), rows:trendyList};      // the Trendy tab payload                        // the Lifer tab's whole payload, rebuilt from scratch every recompute
    // A carried class keeps the published not-fresh list; without this the "N pending" banner blanked and a class
    // still sitting on seed data read as fully downloaded.
    if(carry){ if(prevTop.notFresh && prevTop.notFresh[C.key]) out.notFresh[C.key]=prevTop.notFresh[C.key]; }
    const stillPending = stale.filter(a => !noSrcSet.has(a));    // a symbol with no feed is not pending, it is unavailable
    if(noSrc.length) (out.noSource||(out.noSource={}))[C.key]=noSrc;
    if(stillPending.length) out.notFresh[C.key]=stillPending;                                                 // this class has instruments still on seed data
  }
  // RE-READ BEFORE WRITING: see the note above. Only classes this run CARRIED are refreshed from the current table;
  // anything this run computed stays as computed.
  if(carried.size || Object.keys(perpCarried).length){
    try{
      const nowTbl=JSON.parse(fs.readFileSync(path.join(dataDir,"engine_data.json"),"utf8"));
      if(nowTbl && nowTbl.metricsVer===CACHE_VER && nowTbl.classes){
        for(const k of carried){
          const mine=out.classes[k] && Array.isArray(out.classes[k].rows) ? out.classes[k].rows : null;
          const fresh=nowTbl.classes[k] && Array.isArray(nowTbl.classes[k].rows) ? nowTbl.classes[k].rows : null;
          const csy=carriedSyms[k];
          if(mine && fresh && csy){
            const byA=Object.create(null); for(const r of fresh) byA[r.a]=r;
            // PER SYMBOL: only a symbol this run carried may be refreshed from disk, and only if the alias guard
            // allows it. A row this run COMPUTED is never replaced, and disk can never add a symbol of its own.
            out.classes[k]=Object.assign({}, out.classes[k], { rows: mine.map(r =>
              (csy.has(r.a) && byA[r.a] && !isAliased(k, r.a)) ? byA[r.a] : r) });
          }
          // Updated when the fresh table has one; NEVER deleted. Dropping it let a class with instruments still on
          // seed data read as fully downloaded -- the same banner lie this change already had to fix once.
          if(nowTbl.notFresh && nowTbl.notFresh[k]) out.notFresh[k]=nowTbl.notFresh[k];
          if(k==="crypto"){                                   // the three crypto side payloads travel with the class
            if(nowTbl.ema)    out.ema=emaExpire(nowTbl.ema);   // must age here too, or the re-read undoes the expiry above
            if(nowTbl.lifer)  out.lifer=nowTbl.lifer;
            if(nowTbl.trendy) out.trendy=nowTbl.trendy;
          }
        }
        // the perp rows a non-owner run carried: take them from the CURRENT table too, row and Jackpot row alike
        for(const k in perpCarried){
          const mine=out.classes[k] && out.classes[k].rows, fresh=nowTbl.classes[k] && nowTbl.classes[k].rows;
          if(!mine || !Array.isArray(fresh)) continue;
          const byA=Object.create(null); for(const r of fresh) byA[r.a]=r;
          out.classes[k]=Object.assign({}, out.classes[k], { rows: mine.map(r => (perpCarried[k].has(r.a) && byA[r.a]) ? byA[r.a] : r) });
          const nx=(nowTbl.lifer && Array.isArray(nowTbl.lifer.xrows)) ? nowTbl.lifer.xrows : [];
          for(let i=0;i<xLifer.length;i++){ const x=xLifer[i]; if(x.cls!==k || !perpCarried[k].has(x.sym)) continue;
            const f2=nx.find(r=>r.a===x.a); if(f2) xLifer[i]=f2; }
          const nxe=(nowTbl.ema && Array.isArray(nowTbl.ema.xrows)) ? nowTbl.ema.xrows : [];
          for(let i=0;i<xEma.length;i++){ const x=xEma[i]; if(x.cls!==k || !perpCarried[k].has(x.sym)) continue;
            const f2=nxe.find(r=>r.a===x.a); if(f2) xEma[i]=f2; }
        }
      }
    }catch(e){}                                               // unreadable/absent table -> keep what we built
  }
  // THE JACKPOT'S PERP ROWS (lifer.xrows) and the ROUTE MAP (lifer.perp) ride on the crypto lifer payload, whichever
  // lane built it: a class this recompute COMPUTED replaces its rows, a class it carried keeps the published ones. (It
  // used to rebuild them only when crypto itself was recomputed, so with crypto switched off every stock and metal
  // Jackpot row froze.) Keys are unique: a tradfi symbol that collides with a crypto one uses its file alias (CVX.US).
  const computedNonCrypto=new Set(classes.filter(C=>C.key!=="crypto" && !carried.has(C.key)).map(C=>C.key));
  if(out.lifer){
    const kept=(Array.isArray(out.lifer.xrows)?out.lifer.xrows:[]).filter(r=>!computedNonCrypto.has(r.cls) && perpOf(r.cls, r.sym||r.a));
    const xrows=kept.concat(xLifer);
    const pm={};
    for(const r of (out.lifer.rows||[])){ const q=perpOf("crypto", r.a); if(q) pm[r.a]=Object.assign({cls:"crypto", sym:r.a}, q); }
    for(const r of xrows){ const q=perpOf(r.cls, r.sym||r.a); if(q) pm[r.a]=Object.assign({cls:r.cls, sym:r.sym||r.a}, q); }
    out.lifer=Object.assign({}, out.lifer, { xrows, perp:pm });
  }
  // EMA's PERP ROWS (ema.xrows) ride on the crypto ema payload exactly the same way lifer.xrows does above (same
  // computed/carried logic, same reasoning) -- built for the Zones confluence check (Nate, 2026-09-26), NOT the
  // EMA tab itself, which stays crypto-only by design (emaList above never changed).
  if(out.ema){
    const kept=(Array.isArray(out.ema.xrows)?out.ema.xrows:[]).filter(r=>!computedNonCrypto.has(r.cls) && perpOf(r.cls, r.sym||r.a));
    const xrows=kept.concat(xEma);
    out.ema=Object.assign({}, out.ema, { xrows });
  }
  // ZONES (new, additive): attaches the externally-computed range-extreme/flip-zone payload. Built by
  // trend-solution's own research/_run_universe.mjs pipeline, NOT this engine -- the zone spec needs a 90-day
  // daily lookback, and PERP_DAYS (14d, above) is all this engine retains for non-crypto perp-routed instruments,
  // so it cannot be computed live here. Read-only, best-effort: any failure must never break the existing
  // recompute, so it is wrapped the same defensive way as the writeAtomic call right below it.
  // 882 payload: rebuilt every recompute from whatever this run computed plus whatever it carried (above), so a
  // class that did not download this cycle keeps its legs rather than dropping off the tab for a minute.
  out.f882 = { asOf: Date.now(), rows: f882 };
  // CLASS TAGGING (Nate, 2026-09-26: "fix them all"). The payload carries only a bare SYMBOL per asset, which is
  // ambiguous for a symbol two classes both use -- and that ambiguity was serving the WRONG ASSET's live price,
  // stoch and chart on the Zones tab (crypto CVX = Convex Finance ~$2 vs usstock CVX = Chevron ~$205).
  // The class is recoverable EXACTLY, from the pipeline's own construction rather than by guessing: the crypto
  // fetcher (research/_fetch_universe_daily.mjs) builds its universe as the Yubit perps list MINUS every symbol in
  // ANY yubit_classes.json list, so a symbol that appears in that file is NEVER fetched as crypto -- meaning
  // data/fresh/<sym>-1d.json (what the detector reads) is that tradfi class's asset, full stop. Anything the
  // classes file does not list came in as crypto. So: first class here that lists the symbol wins, else crypto.
  try {
    const zp = path.join(__dirname, "zones_cache.json");
    if (fs.existsSync(zp)) {
      const z = JSON.parse(fs.readFileSync(zp, "utf8"));
      let cls = {};
      try { cls = JSON.parse(fs.readFileSync(path.join(__dirname, "yubit_classes.json"), "utf8")); } catch (e) { cls = {}; }
      const owner = new Map();
      for (const k of Object.keys(cls)) for (const row of (cls[k] || [])) {
        const s = String(row.sym || row).toUpperCase();
        if (!owner.has(s)) owner.set(s, k);                         // first class that lists it owns it
      }
      if (Array.isArray(z.results)) z.results = z.results.map(r => Object.assign({}, r, { cls: owner.get(String(r.sym).toUpperCase()) || "crypto" }));
      if (Array.isArray(z.live)) z.live = z.live.map(r => Object.assign({}, r, { cls: owner.get(String(r.sym).toUpperCase()) || "crypto" }));
      out.zones = z;
    }
  } catch (e) {}
  try { writeAtomic(path.join(dataDir,"engine_data.json"), JSON.stringify(out)); } catch(e){}   // a transient lock just skips this table write; the next class's recompute rewrites it
  return out;
}

// ---- sticky pool membership (userData/membership.json). Starts = the shipped CRYPTO seed; GROWS when a coin climbs
// into the top-100 (auto-add in updateMcap); only MANUAL removal subtracts -> a living list that never shrinks itself. ----
function saveMembership(dataDir, m){ try{ writeAtomic(path.join(dataDir,"membership.json"), JSON.stringify(m)); }catch(e){} }
function loadMembership(dataDir){
  let m=null; try{ m=JSON.parse(fs.readFileSync(path.join(dataDir,"membership.json"),"utf8")); }catch(e){}
  if(!m || !Array.isArray(m.coins) || !m.coins.length){ m={ coins:CRYPTO.filter(onYubit), src:{}, removed:[] }; for(const s of KUCOIN_CRYPTO) m.src[s]="KUCOIN"; saveMembership(dataDir,m); }   // first run -> seed from the shipped pool
  if(!m.src) m.src={}; if(!Array.isArray(m.removed)) m.removed=[];
  // Nate 2026-09-22 "add yubit perps": every crypto perp in perp_routes.json is a member (grow-only, a manual removal still wins)
  let grew=false; for(const k of perpRoutes().keys()){ if(!k.startsWith("crypto:")) continue; const s=k.slice(7);
    if(!m.coins.includes(s) && !m.removed.includes(s)){ m.coins.push(s); grew=true; } }
  if(grew) saveMembership(dataDir,m);
  return m;
}
// which source can serve this symbol's history: Binance first, then KuCoin (both verified to serve real 1m). null = neither -> not added.
// Does Binance serve this symbol at all? Split out of tradeSrc so the daily KuCoin re-test in update() asks the
// SAME question the original routing decision asked, rather than a second copy of it that could drift from it.
async function binHas(SYM){
  try{ const r=await fetch(HOST+"/api/v3/klines?symbol="+SYM+"USDT&interval=1m&limit=2",{headers:{Accept:"application/json"},signal:AbortSignal.timeout(20000)}); if(r.ok){ const a=await r.json(); return Array.isArray(a)&&a.length>0; } }catch(e){}
  return false;
}
async function tradeSrc(SYM){
  if(await binHas(SYM)) return "BIN";
  try{ const r=await fetch(KU_HOST+"/api/v1/market/candles?type=1min&symbol="+SYM+"-USDT",{headers:{"User-Agent":"Mozilla/5.0"},signal:AbortSignal.timeout(20000)}); if(r.ok){ const j=await r.json(); if(j&&j.code==="200000"&&Array.isArray(j.data)&&j.data.length) return "KUCOIN"; } }catch(e){}
  return null;
}

// ---- build the class list (US stocks + Dukascopy ids from the bundled yubit_classes.json) ----
function buildClasses(dataDir){
  let cls={}; try{ cls = JSON.parse(fs.readFileSync(path.join(__dirname,"yubit_classes.json"),"utf8")); }catch(e){ cls={}; }   // missing/corrupt config -> crypto (hard-coded) still works; tradfi classes just come back empty
  const tf = k => (cls[k]||[]).map(x=>(x.sym||x).toLowerCase());
  const mem = dataDir ? loadMembership(dataDir) : null;
  const cryptoSyms = mem ? mem.coins.filter(s=>!mem.removed.includes(s)) : CRYPTO;   // STICKY: live membership minus manual removals (falls back to the static seed if no dataDir)
  const out = [
    { key:"crypto", label:"Crypto", syms:cryptoSyms, group:"crypto", thr:FULL, base:5 },
    { key:"usstock", label:"Stocks", syms:tf("usstock"), group:"stock", thr:FULL, base:5 },
    { key:"forex", label:"Forex", syms:tf("forex"), group:"duka", thr:[1,1.5,2,2.5,3], base:1 },   // 2026-06-19: dropped 0.5% (Nate: too small for forex) → floor/base now 1%; was [0.5,1,1.5,2,2.5,3] base:0.5
    { key:"indices", label:"Indices", syms:tf("indices"), group:"duka", thr:[1,2,3,4,5,6,7,8,9,10,11,12,15,20], base:1 },
    { key:"commodities", label:"Commodities", syms:tf("commodities"), group:"duka", thr:[3,4,5,6,7,8,9,10,11,12,15,20], base:3 },
    { key:"metal", label:"Metals", syms:tf("metal"), group:"duka", thr:[3,4,5,6,7,8,9,10,11,12,15,20], base:3 },
    { key:"oil", label:"Oil", syms:tf("oil"), group:"duka", thr:[3,4,5,6,7,8,9,10,11,12,15,20], base:3 },
  ];
  assertNoCollision(out);
  return out;
}
// The data dir is FLAT and the metrics cache is keyed by the same name, so two classes resolving to the SAME file
// name share a candle file AND a cache entry, and the lane that writes last wins. That is how the Stocks tab spent
// months showing Convex Finance under CVX: nothing anywhere noticed. This is the check that would have caught it
// on the first run instead.
// It THROWS. engine.js only ever runs inside the utilityProcess (engine_host.js), whose catch posts {type:'error'}
// -> main.js finish({ok:false,error}) -> the renderer's "Update problem" panel, which is the app's existing loud
// path. The window is NOT bricked: the renderer reads engine_data.json from disk, so the last good table stays on
// screen with the collision named on top of it. Serving knowingly-wrong data silently is the worse failure.
// The fix when it fires is one line: add the second claimant to FILE_ALIAS in file_alias.js.
function assertNoCollision(classes){
  const owner=new Map(), bad=[]; let sameClass=false, crossClass=false;
  for(const C of classes) for(const sym of (C.syms||[])){
    const f=dfile(C.key, sym), prev=owner.get(f);
    if(prev){ const same=(prev===C.key+":"+sym); if(same) sameClass=true; else crossClass=true;
      bad.push(f+".json <- "+prev+" AND "+C.key+":"+sym+(same ? " (the SAME class lists it twice)" : "")); }
    else owner.set(f, C.key+":"+sym);
  }
  if(bad.length){
    // The remedy depends on WHICH kind it is. FILE_ALIAS is keyed class:symbol, so it cannot separate two entries
    // that share one class -- telling someone to alias a duplicate sends them somewhere that cannot work.
    const fix = sameClass && crossClass ? "Fix: remove the duplicate listing(s) from yubit_classes.json, and add the cross-class claimant to FILE_ALIAS in file_alias.js."
              : sameClass               ? "Fix: remove the duplicate listing from yubit_classes.json (FILE_ALIAS is keyed class:symbol, so it cannot separate two entries in the same class)."
              :                           "Fix: add the second claimant to FILE_ALIAS in file_alias.js.";
    throw new Error("SYMBOL COLLISION: "+bad.length+" file name(s) claimed twice -- "+bad.join("; ")
      +". Both would share one candle file and one cache entry, so the last lane to write wins and one tab shows the other's asset. "+fix);
  }
}

// ---- the one call main.js uses. PROGRESSIVE: recomputes + reports after each class finishes, so the
// table fills in class-by-class (crypto first) instead of waiting for the whole download. ----
// CRYPTO download concurrency. Binance & KuCoin are INDEPENDENT servers (separate rate limits), so their pools run
// at the same time; each pool is capped so we never hammer one source (and binanceKlines/kucoinCandles already retry
// on 429 with backoff, so an occasional throttle self-heals rather than failing). Was 1-coin-at-a-time (serial) =
// the slow first download Nate flagged. Stocks (Polygon ~5/min) & Dukascopy (throttle-prone) stay serial + paced.
const CRYPTO_BIN_CONC = 4, CRYPTO_KU_CONC = 3;
const PERP_CONC = { bitget:6, kucoin:3, okx:3, mexc:3 };   // perp pools per class; the real ceiling is the shared per-venue pace (SLOT)
async function runPool(items, conc, fn){ let i=0; const n=Math.max(1,Math.min(conc, items.length)); const w=Array.from({length:n},async()=>{ while(i<items.length){ const x=items[i++]; await fn(x); } }); await Promise.all(w); }   // fixed worker pool over a shared cursor: at most `conc` fn() in flight
async function update({ dataDir, polygonKey, enabled, only, onProgress, onClassDone }){
  if(!fs.existsSync(dataDir)) fs.mkdirSync(dataDir,{recursive:true});
  const mem0 = loadMembership(dataDir); kuActive = new Set(Object.keys(mem0.src||{}));   // refresh the KuCoin routing set from the live membership (seed + anything auto-added)
  const classes = buildClasses(dataDir);
  const cache = loadCache(dataDir);               // metrics cache -> skip re-reading/re-crunching unchanged instruments
  // WHICH LANE. A perp-routed instrument, whatever its class, runs in the minute lane (the one that includes crypto):
  // its venue is keyless and fast, and the Stoch Jackpot needs its 1m. Everything unrouted keeps its class's lane. A
  // class toggle still switches its routed instruments off too.
  const fastLane = !only || only.includes("crypto");
  const laneHas = (C, s) => perpOf(C.key, s) ? fastLane : (!only || only.includes(C.group));
  const symsOf = C => C.syms.filter(s => (!enabled || enabled[C.group] !== false) && laneHas(C, s)
                                        && (perpOf(C.key, s) || C.group!=="duka" || DUKA_MAP[s] || BINANCE_IN_DUKA[s]));
  const isOn = C => symsOf(C).length > 0;
  // ---- DAILY KUCOIN RE-TEST -------------------------------------------------------------------------------
  // kuActive is grow-only (see the note in updateMcap: "grow-only; never removes"), so a symbol that landed on
  // KuCoin because of ONE transient Binance failure -- a timeout, a 451, a rate limit -- stayed there permanently.
  // That is an ALERT problem, not just a tidiness one: KuCoin's 1m feed goes quiet for minutes at a time (measured
  // on the live pool: 15 of 42 routed symbols produced no bar at all in a run; staleness p90 862 s, max 2,122 s),
  // and the Lifer alert gate needs the newest CONFIRMED bar to be less than one timeframe old. A row fed by a venue
  // that can go 35 minutes without a bar can never fire a 1m or 3m alert, however fast the engine gets.
  // So once a day, ask Binance again about every KuCoin-routed symbol and move back the ones it can serve.
  // DIRECTION IS DELIBERATELY ONE-WAY. This only ever demotes TOWARD Binance; it never promotes anything to KuCoin
  // and never drops a symbol. A coin Binance does not serve stays exactly where it is, so the genuinely KuCoin-only
  // members are untouched. Nothing is deleted: mem.src is a ROUTING table, the candle file is the same file either
  // way, and the history already downloaded is not read or rewritten here.
  // COST on a metered line: one two-candle probe per routed symbol, once a day -- 42 requests, ~60 kB. It is gated
  // on the crypto group being on AND on this run actually touching crypto, so a stock/duka sweep never pays it.
  const cryptoOn = classes.some(C => C.group==="crypto" && isOn(C));   // isOn, not a second copy of its logic
  const kuRetestDay = dmyUTC(Date.now());
  let kuMoved = [];
  if (cryptoOn && mem0.kuRetestDate !== kuRetestDay && kuActive.size){
    const routed = [...kuActive].filter(sl => !perpOf("crypto", sl));   // a perp-routed symbol never reads the kuActive branch in updateInstrument, so probing it cannot change any fetch -- pure waste on a metered line
    await runPool(routed, CRYPTO_BIN_CONC, async sl => { if (await binHas(String(sl).toUpperCase())) kuMoved.push(sl); });
    // RE-READ BEFORE WRITING. saveMembership is a blind whole-file last-writer-wins, and updateMcap in the OTHER
    // lane loads this same file, then spends minutes on paced tradeSrc probes, then saves. main.js fires both lanes
    // back-to-back at launch, so that window overlaps this one almost every time. Writing mem0 (loaded seconds ago)
    // would revert whatever the other lane added -- its auto-added coins -- or have this demotion reverted in turn.
    // Re-reading and applying only OUR delta makes the two writers compose instead of clobbering.
    const memW = loadMembership(dataDir);
    for (const sl of kuMoved) delete memW.src[sl];
    memW.kuRetestDate = kuRetestDay;                       // stamped even when nothing moved, so it runs once a day
    saveMembership(dataDir, memW);
    mem0.src = memW.src; mem0.kuRetestDate = memW.kuRetestDate;
    kuActive = new Set(Object.keys(memW.src||{}));         // the fetch split below reads this, so refresh it HERE
  }
  const total = classes.reduce((n,C)=> n + (isOn(C) ? symsOf(C).length : 0), 0);
  const now = Date.now();
  const log = { time:new Date(now).toISOString(), sources:{crypto:{ok:0,err:0},stock:{ok:0,err:0,skipped:0},duka:{ok:0,err:0}}, errors:[] };
  // Carried in the run log for anything that wants it. NOT on screen: onDone in the renderer reads only d.error, so
  // nothing displays d.log today. Saying "routing changes are not silent" here would be false -- the only way to see
  // a move right now is to diff membership.json, which is also the only way to catch a bad one.
  if (kuMoved.length) log.kuToBinance = kuMoved.slice();
  let done=0, stockKeyBad=false;
  // CONCURRENCY SAFETY: recompute() and every file write (writeAtomic) are fully SYNCHRONOUS — there is no `await`
  // anywhere inside them — so on Node's single thread they run to completion without yielding. The source streams
  // below only ever yield at their fetch/sleep `await`s, so a recompute or a write can NEVER interleave mid-flight
  // with another stream's. That makes the three concurrent streams race-free on engine_data.json / _cache.json /
  // the shared in-memory cache (each stream also only writes its own disjoint per-symbol keys + candle files).
  // The class keys this run is actually downloading. Passed to recompute so classes outside it are carried from the
  // published table instead of being recomputed from candles -- the fast lane and the slow lane stop duplicating each
  // other's work. null when the run is unrestricted, which keeps the original all-classes behaviour exactly.
  const refreshing = only ? new Set(classes.filter(isOn).map(C=>C.key)) : null;
  PERP_ERRS = fastLane ? [] : null;
  VENUE_FAILS = {}; VENUE_DOWN = new Set();                 // a venue that was down last run is asked again
  const scanFile = path.join(dataDir, "perp_scan.json");
  let scan = {}; if (fastLane) { try { scan = JSON.parse(fs.readFileSync(scanFile, "utf8")) || {}; } catch(e){ scan = {}; } }
  const afterClass = (C) => { recompute(dataDir, classes, cache, refreshing, { perpOwner: fastLane }); saveCache(dataDir, cache); if(onClassDone)onClassDone(C.key, C.label, done, total); };   // progressive fill: recompute as each class finishes
  const doOne = async (C, sym) => {                                  // one instrument: fetch gap -> append -> log -> progress (extracted so crypto can run it in parallel pools)
    if (C.group==="stock" && !perpOf(C.key, sym) && (!polygonKey || stockKeyBad)){ log.sources.stock.skipped++; done++; if(onProgress)onProgress(done,total,sym.toUpperCase()); return; }
    const fk = dfile(C.key, sym);                                    // an aliased instrument reads and writes its OWN file, not the other class's
    const route = perpOf(C.key, sym);
    const r = route                ? await updateInstrument(dataDir, sym, C.group, polygonKey, now, cache, fk, route, scan)
            : BINANCE_IN_DUKA[sym] ? await updateInstrument(dataDir, sym, "crypto", polygonKey, now, cache, fk)   // gold token in the metals tab, fetched from Binance not Duka
            : C.group==="duka"     ? await updateDuka(dataDir, sym, now, cache, fk)
            :                        await updateInstrument(dataDir, sym, C.group, polygonKey, now, cache, fk);
    // NO DATA AT ALL: the fetch came back with nothing AND nothing is on disk for it. For a healthy instrument
    // "added 0" just means no new bars since last time -- which is why the file check is the other half.
    if (C.group!=="duka" || route){
      const k = C.key+":"+sym; ATTEMPTED.add(k);
      if (!r.added && !fs.existsSync(CS.basePath(dataDir, fk))) NO_BARS.add(k); else NO_BARS.delete(k);
    }
    if (route && r.status!=="ok" && PERP_ERRS) PERP_ERRS.push(C.key+" "+sym.toUpperCase()+": "+r.msg);
    if (r.status==="ok") log.sources[C.group].ok++;
    else { log.sources[C.group].err++; log.errors.push(sym+": "+r.msg); if(C.group==="stock" && /api-key-invalid/.test(r.msg||"")) stockKeyBad=true; }   // bad key -> stop hammering the remaining stocks this run
    done++;
    if (onProgress) onProgress(done, total, sym.toUpperCase());
  };
  const runClass = async (C) => {
    if (!isOn(C)) return;
    const all = symsOf(C), perps = all.filter(s => perpOf(C.key, s)), rest = all.filter(s => !perpOf(C.key, s));
    const perpRun = Promise.all(Object.keys(PERP_CONC).map(v => runPool(perps.filter(s => perpOf(C.key, s).v===v), PERP_CONC[v], s=>doOne(C,s))));
    if (!rest.length){ await perpRun; }
    else if (C.group==="crypto"){                                          // CRYPTO: Binance pool ‖ KuCoin pool run AT THE SAME TIME (independent sources), each capped -> big speed-up vs the old 1-coin-at-a-time loop
      await Promise.all([ perpRun,
                          runPool(rest.filter(s=>!kuActive.has(s)), CRYPTO_BIN_CONC, s=>doOne(C,s)),
                          runPool(rest.filter(s=> kuActive.has(s)), CRYPTO_KU_CONC,  s=>doOne(C,s)) ]);
    } else {                                                          // stocks (Polygon ~5/min) + Dukascopy (throttle-prone) MUST stay serial + paced
      await perpRun;
      for (const sym of rest){
        await doOne(C, sym);
        if (C.group==="stock") await sleep(12500);                   // massive free tier ~5/min
        else if (C.group==="duka" && !BINANCE_IN_DUKA[sym]) await sleep(DUKA_INSTRUMENT_PAUSE);   // pace Dukascopy (Binance-sourced golds skip this)
      }
    }
    // THE MINUTE LANE'S PERP-ONLY CLASSES (stocks, metals, oil, indices here) do not rebuild the table on their own
    // finish. recompute() is synchronous, and each class finishing used to trigger a full rebuild that STALLED every
    // other stream's downloads while it ran -- measured on a copy of the data: 92-112 s a minute-lane run, the
    // downloads alone not done until 67-73 s. The final recompute after all streams covers these classes.
    if (only && !only.includes(C.group) && !rest.length) return;
    afterClass(C);
  };
  // Run the THREE source streams CONCURRENTLY. CRYPTO now also fans out INTERNALLY (Binance pool ‖ KuCoin pool, each
  // capped) so its first download is no longer 1-coin-at-a-time; stocks (Polygon ~5/min) & Dukascopy stay serial +
  // paced. The streams overlap so total wall-clock ~= the SLOWEST source. A keyed hourly run stays ~stock-bound, under
  // the hour, so the hourly tick isn't overrun/skipped (main.js busy guard).
  const stream = async (grp) => { for (const C of classes) if (C.group===grp) await runClass(C); };   // duka stream walks forex->indices->commodities->metal->oil in order
  const cryptoSyms = (classes.find(c=>c.key==="crypto")||{}).syms || [];
  const cryptoOnly = !!(only && only.length===1 && only[0]==='crypto');
  const mcapTask = (!cryptoOnly) ? updateMcap(dataDir, cryptoSyms).catch(()=>null) : Promise.resolve();   // runs on the full sweep, which now passes ['stock','duka'] rather than null — keying off `!only` here would have silently killed the daily mcap refresh AND the sticky auto-add   // refresh mcap ranks on FULL sweeps only (not the 5-min crypto-only ticks); the once-a-day 30d-ago pull lives inside updateMcap
  await Promise.all([ stream("crypto"), stream("stock"), stream("duka"), mcapTask ]);
  if (fastLane && scan.__dirty){ delete scan.__dirty; try{ writeAtomic(scanFile, JSON.stringify(scan)); }catch(e){} }
  recompute(dataDir, classes, cache, refreshing, { perpOwner: fastLane }); saveCache(dataDir, cache);   // final pass: guarantee the complete table once every stream has finished
  // Release the workers. Each holds its last asset's parsed bars and bucket series (~0.5 GB apiece) and the tradfi
  // lane runs for minutes to an hour after its last compute, so leaving them resident pins memory for nothing.
  try{ if(POOL && POOL.shutdown) POOL.shutdown(); }catch(e){}
  return log;
}

module.exports = { perpOf, xKey, fetchBitgetPerp, fetchKuCoinPerp, fetchOkxPerp, fetchMexcPerp, PERP_DAYS, update, recompute, buildClasses, computeMetrics, INTERVAL_MIN, updateInstrument, updateDuka, updateMcap, sign5, okBar, emaCross, EMA_TFS, liferCross, LIFER_TFS, LIFER_ALL_TFS, liferPatterns, tfOHLC, tfSeries, trendyStats, TRENDY_TFS, TRENDY_NS };   // emaCross + EMA_TFS exported for the research/verification scripts (same pattern as sign5/okBar)
