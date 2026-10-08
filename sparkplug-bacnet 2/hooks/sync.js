/**
 * sync — Scheduled hook (every minute). Loops for ~50 s, copying Sparkplug values
 * into the approved BACnet local objects every few seconds (Settings → interval).
 */
const E = require('../lib/engine');

module.exports = async () => {
  try { return JSON.stringify(await E.runScheduled()); }
  catch (e) { return JSON.stringify({ error: e.message }); }
};
