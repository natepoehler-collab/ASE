// Cross-class symbol collisions. The data dir is FLAT (<name>.json) and the metrics cache is keyed by the same
// name, so a ticker that appears in TWO classes would share both and the last writer would win. CVX is Convex
// Finance in crypto and Chevron in stocks. Only the SECOND claimant is renamed, so no existing file moves.
// This lives in its own module because BOTH engine.js and main.js must resolve it identically -- main.js serves
// the chart windows their candles, and resolving it in only one of the two is how a stock chart ends up drawing
// a crypto asset. Requiring engine.js from main.js would drag in the whole download stack, hence a tiny module.
const FILE_ALIAS = { "usstock:cvx":"cvx.us" };
// PERP ROUTES WHOSE OLD FILE IS A DIFFERENT ASSET (perp_routes.json fresh:true). Nate: "move to perps" -- but EWT's
// candle file holds a coin at 0.29 while the EWT perp trades at 109 (likewise BNC, SIREN). Appending the perp's bars
// would splice two assets into one series, so the perp gets its OWN file, <sym>.perp, started 14 days back, and the old
// file is never read or written again (nothing is deleted). Being an alias, its old rows are never carried forward either
// (see isAliased in engine.js recompute): they describe the other asset.
// FAIL LOUD (Nate, 2026-09-26: "fix them all"). This was catch(e){}: a corrupt or unreadable perp_routes.json
// produced ZERO aliases, silently, and every fresh:true perp then read AND WROTE the old file -- which the comment
// above says is a DIFFERENT ASSET (EWT 0.29 vs 109). Splicing two assets into one series is exactly the failure
// this module exists to prevent, so a missing/broken routes file must stop the run, the same way engine.js's
// assertNoCollision throws rather than serving knowingly-wrong data. A file that is absent by design (no routes
// configured at all) is still fine: only a PARSE/READ failure of an existing file throws.
const _rp = require("path").join(__dirname, "perp_routes.json");
if (require("fs").existsSync(_rp)) {
  let _j;
  try { _j = JSON.parse(require("fs").readFileSync(_rp, "utf8")); }
  catch (e) { throw new Error("perp_routes.json exists but could not be read/parsed (" + e.message + "). Refusing to run with ZERO file aliases: every fresh-routed perp would read and write the OLD file, which is a different asset."); }
  for (const r of (_j.routes || [])) if (r && r.fresh && r.cls && r.sym)
    FILE_ALIAS[String(r.cls) + ":" + String(r.sym).toLowerCase()] = String(r.sym).toLowerCase() + ".perp";
}
const dfile = (classKey, sym) => FILE_ALIAS[String(classKey||"")+":"+String(sym||"").toLowerCase()] || String(sym||"").toLowerCase();
const isAliased = (classKey, sym) => !!FILE_ALIAS[String(classKey||"")+":"+String(sym||"").toLowerCase()];
module.exports = { FILE_ALIAS, dfile, isAliased };
