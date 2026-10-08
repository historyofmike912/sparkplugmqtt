/**
 * mqtt — Scheduled hook (every minute). Subscribes to the configured broker for ~50 s and writes
 * each topic's value to a point on the app's MQTT layer. Does nothing until enabled in Settings.
 */
const I = require('../lib/ingest');

module.exports = async () => {
  try { return JSON.stringify(await I.runIngest()); }
  catch (e) { return JSON.stringify({ error: e.message }); }
};
