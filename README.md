# sparkplug-bacnet — MQTT & Sparkplug to BACnet

An Abound Normalizer application that publishes building and process data as **BACnet local objects** (Analog, Binary and Multi-State Values) so WebCTRL or any BACnet client can discover and read them. It takes data from two kinds of source:

- **Plain MQTT** (one value per topic, for example `site/area/equipment/point/Value`): the app's own `mqtt` hook subscribes to the broker and creates a point per topic on its `mqtt-ingest` layer.
- **Sparkplug B**: the platform's built-in Sparkplug Consumer creates the points (layer `hpl:sparkplug:1`).

```
MQTT broker (plain topics) → mqtt hook → points (mqtt-ingest) ┐
Sparkplug B broker → Sparkplug Consumer → points (hpl:sparkplug:1) ┴→ sync hook → BACnet local objects → WebCTRL
```

Everything runs inside the Abound Normalizer and has no npm dependencies, so it installs on sites without internet access. Data flows one way; nothing is written to the source.

## What's in here

| Path | What it does |
|---|---|
| `app.json`, `package.json` | App manifest. `app.json` may only contain fields the platform knows (`name`, `description`, `minNfVersion`); an unknown field such as `version` makes the install fail with `STATUS_INSTALL_ERROR`. |
| `hooks-update/*.json` | Hook definitions, registered automatically at install. Flat format: `id`, `name`, `entryPoint`, `points`, `mode`, `schedule`, `invokeTimeout`. |
| `hooks/mqtt.js` | **Scheduled**, every minute. Subscribes for ~50 s, adds new topics as points, writes changed values. Idle until enabled in Settings. |
| `hooks/sync.js` | **Scheduled**, every minute. Copies values into the published BACnet objects every few seconds; sets Status_Flags FAULT when a tag stops reporting. |
| `hooks/api.js` | **On Request**, behind the app page. |
| `lib/mqtt.js` | Built-in MQTT 3.1.1 client (mqtt:// and mqtts://, username/password, custom CA) |
| `lib/ingest.js` | Topic → point creation, payload parsing, value writes |
| `lib/engine.js` | Mapping rules, BACnet object calls, sync cycle |
| `static/index.html` | The app page (Mapping, Activity, Settings) |
| `data/` (runtime) | `mapping.json` (approved mapping), `mqtt.json`, status files. Installed apps live in `/var/nf/apps/<app>/app/`, so this is `/var/nf/apps/sparkplug-bacnet/app/data/`. |

## Plain MQTT setup

On **Settings → MQTT broker**: turn it on, enter the broker address (`mqtt://host:1883` or `mqtts://host:8883`; a bare IP means `mqtt://` on 1883). For a broker cluster, list every node separated by commas (`10.0.0.1, 10.0.0.2`): the app connects to one at a time, fails over to the next, and Test connection reports which nodes answer. the read-only username and password, the topics (for example `nvidia/sjc22/#`) and a client ID no other client uses. Click **Test connection** first: it connects for a few seconds and shows sample topics, the raw payload, and how the app reads it. Save, and within two minutes every topic appears as a point and **Find tags** lists them.

Payloads understood automatically: bare values (`23.4`, `true`, `5`), JSON scalars, and JSON objects with a `value` / `Value` / `v` / `presentValue` field and an optional `ts` / `timestamp`. For other JSON layouts set **JSON value field** (for example `data.reading`). **Trim from names** (default `/Value`) shortens object names.

**Objects per BACnet device** (default 250) spreads new tags across local device offsets when a device fills up. Confirm the platform's local-object limit and configure the additional local devices before publishing more than one device's worth.

**Multi-state values that count from 0** (for example 0 = Moving, 1 = Closed, 2 = Open): BACnet states start at 1, so set the row's **Offset** to `1`; the app then publishes value + 1.

## Requirements

- Abound Normalizer 3.10 or later, with the Sparkplug Consumer connected to the broker
- BACnet/IP configured on the platform (device instance, interface)
- The platform's data folder (`/var/nf`) on persistent storage. The mapping is stored inside it.

## Install

**From a Git repository (recommended):**

1. Push this folder to a Git repository the platform can reach.
2. In the console, open **Applications → + Create**. Name `sparkplug-bacnet`, and enter the repository URL. For a private repository, provide an access token with read access.
3. Open the app's **Hooks** tab and confirm `sync` (Scheduled) and `api` (On Request) are listed.

**From a local copy (no Git server reachable):** on a Linux install `/var/nf` on the host is the platform's data folder, so the repository can be placed there directly:

```bash
sudo mkdir -p /var/nf/local-src
sudo cp -r sparkplug-bacnet /var/nf/local-src/
cd /var/nf/local-src/sparkplug-bacnet && sudo git init -q && sudo git add -A && sudo git commit -qm "sparkplug-bacnet 1.0.0"
cd /opt/nf && sudo docker compose exec -u root nf chown -R daemon:daemon /var/nf/local-src/sparkplug-bacnet
```

Then **Applications → + Create** with the Git URL `file:///var/nf/local-src/sparkplug-bacnet`.

### If the hooks aren't listed after install

Register them through the API from the server (replace 8080 if your console port differs):

```bash
curl -s -X POST http://localhost:8080/api/v1/apps/sparkplug-bacnet/hooks -H 'Content-Type: application/json' \
  -d '{"id":"api","name":"api","entryPoint":"/hooks/api.js","points":{"noPoints":true},"mode":"MODE_ON_REQUEST","invokeTimeout":"60s"}'
curl -s -X POST http://localhost:8080/api/v1/apps/sparkplug-bacnet/hooks -H 'Content-Type: application/json' \
  -d '{"id":"sync","name":"sync","entryPoint":"/hooks/sync.js","points":{"noPoints":true},"schedule":{"rrule":"DTSTART:20260101T000000Z\nRRULE:FREQ=MINUTELY;INTERVAL=1"},"mode":"MODE_SCHEDULED","invokeTimeout":"80s"}'
```

Or create them on the Hooks tab with the same name, entry point and mode.

## Use

Open the app from **Applications**.

1. **Settings tab first.** Set the not-reporting timeout to at least three times the PLC's slowest publish interval. If the edge node reports by exception, choose "Do nothing" for stale tags. If the console port is not 8080, set the API address (for example `http://localhost:8081`).
2. **Find tags.** Every tag the Sparkplug Consumer has received is added as a draft (**Needs approval**) with a proposed object type, instance, name and units. Node-level metrics, strings and Templates (UDTs) are listed as **Not published** with the reason.
3. **Review the drafts** against the approved point list:
   - Change the object type (AV, BV, MSV), instance and object name to match the site standard.
   - Set units for analog values, and scale/offset for raw PLC counts.
   - Use **Split bits** on integer tags that pack alarms into a word; each bit becomes its own BV.
   - To reuse an existing gateway's numbering, use **Import CSV** with `path`, `object_type`, `instance` and `object_name` columns.
   - Click **Save changes**.
4. **Approve and publish.** Enter your name for the change record. The objects are created on the BACnet device and the sync hook starts updating them.
5. Check **BACnet → Local Objects** in the console, then discover the device in WebCTRL.

Rules the app enforces:

- Nothing is published until it is approved.
- Instance numbers are assigned once and never renumbered. New tags get the next free number.
- Changing a published row (type, instance, name, units, scaling) removes its object and returns it to **Needs approval**, so BACnet never shows an object that doesn't match the approved mapping.
- Duplicate instance numbers or names block approval.

**Activity** shows the last sync, tags not reporting, and the event log (approvals, objects created or removed, errors).

## Update the app

Push the new version to the repository, then use the app's update / pull-from-Git option in the console. The mapping in `data/` is not in Git, so updates keep it. Deleting and reinstalling the app removes `data/`: restore it from a managed backup, or export the mapping as CSV first and import it afterwards.

For a local copy, replace the files under `/var/nf/local-src/sparkplug-bacnet`, commit, update the app, then restart it:

```bash
curl -s -X PATCH http://localhost:8080/api/v1/apps/sparkplug-bacnet/restart
```

## Back up

The platform's built-in managed backups include installed applications, so the app and its approved mapping (`data/mapping.json`) are backed up with the rest of the site. After go-live and after each upgrade, restore a recent backup to a test instance once and check the mapping page still shows the published rows.

**Export CSV** on the Mapping tab is still useful as a readable record of the approved mapping for project files, and for moving a mapping to another site. A manual `nfcli.py backup` file does not include applications.

## Troubleshooting

| Symptom | Check |
|---|---|
| Page says it can't start the api hook | The `api` hook is missing on the Hooks tab: register it (above). |
| "Can't reach the platform API" | The console port is not 8080: set the API address on the Settings tab. If console authentication is on, add an API token. |
| "Sync not running" | The `sync` hook is missing or disabled on the Hooks tab, or its Runs show errors. |
| Find tags returns nothing | The Sparkplug Consumer has no points yet: check its connection, and ask for a rebirth from the edge node. |
| Many tags "Not reporting" | The timeout is shorter than the PLC publish interval, or the edge node reports by exception. |
| Objects exist but WebCTRL doesn't see them | BACnet reachability (subnet, BBMD, UDP 47808), not the app. |

## Development

```bash
npm test                      # offline unit tests
# with the lab kit running (broker, PLC simulator, dev/mock_normalizer.py on :8080):
node test/e2e.js              # hooks end to end
node test/devserver.js 8090   # serve the page at http://localhost:8090/api/v1/apps/static/sparkplug-bacnet/
```

Open `static/index.html?demo=1` to preview the page with sample data.
