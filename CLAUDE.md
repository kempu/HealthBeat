# HealthBeat

iOS app that collects Apple HealthKit data + raw GPS + geofence check-ins and pushes every batch to **two backends in parallel**:

1. **MySQL** — direct TCP write via the in-tree `MySQLService`. The original backend. Schema lives in `Sources/HealthBeat/Services/SchemaService.swift`.
2. **EA** (`Executive Assistant`) — HTTPS POSTs to `/api/v1/healthbeat/*` on the user's EA server, authenticated with a per-integration sync key. Schema is mirrored as `hb_*` tables in EA's own database.

Both destinations receive identical, typed batches via the `BackendWriter` protocol. A `MultiBackendWriter` fans out to all enabled writers. MySQL writes happen first (inline SQL in `SyncService`); EA writes happen immediately after via the `eaWriter` property. If either throws, the offending pass is logged as failed and cursors are NOT advanced, so the next pass retries cleanly.

## ⚠️ NON-NEGOTIABLE: every dataset must flow to BOTH backends

**Adding a new HealthKit type, a new on-device entity, or any new field that gets written to MySQL? You MUST also add it to the EA path. No exceptions.** EA is not a "nice to have" mirror — it's the user's only path to view, edit, and query their data outside the iPhone app. A dataset that exists only in MySQL is invisible to the EA web UI, the EA iOS app, and the EA agent's `healthbeat` domain tool.

When this rule is violated, the symptom is silent: the live app keeps working, MySQL keeps filling, but the user's EA `/health-data` view is incomplete, the agent's `tool_invoke("healthbeat", …)` actions return partial truths, and the **dump-based full sync** skips the new data because the dump export/drain (`DumpExport.swift` / `DumpDrain.swift`) doesn't know the table — so a user catching up a newly-enabled (or reset) destination silently misses it. By the time anyone notices, the schema drift has weeks of gap to reconcile manually.

### When adding a new dataset, the checklist is

For a new table (e.g. you're adding `health_mindfulness_sessions`):

1. **MySQL schema** — `Services/SchemaService.swift`: add the `CREATE TABLE` + bump `currentSchemaVersion` + add the migration block. Same shape as the existing tables (UUID primary key, `start_date` / `end_date`, datetime UTC, JSON metadata, `synced_at`).
2. **EA schema** — add a `hb_<table>` migration on the EA side at `web/database/migrations/YYYY_MM_DD_NNNNNN_<name>.php` mirroring the column list one-to-one. EA tables use `Schema::create('hb_…', …)`.
3. **EA ingest column map** — `web/app/Services/HealthBeat/HealthBeatIngestService.php` `COLUMN_MAP`: add the new table with its allowed column list. Add date/JSON column names to `DATETIME_COLUMNS` / `JSON_COLUMNS` if needed.
4. **EA route + controller method** — `web/routes/api.php` (inside the `v1/healthbeat` group): `Route::post('/<endpoint>', [HealthBeatController::class, 'put<…>']);`. Add the controller method in `web/app/Http/Controllers/Api/V1/HealthBeatController.php` calling `$this->ingest->upsertUuidBatch(...)` (or the right shape for the table).
5. **Typed row** — `Sources/HealthBeat/Models/BackendRows.swift`: add an `HB<…>Row: Codable, Sendable` matching the column list exactly (snake_case field names).
6. **BackendWriter protocol method** — `Sources/HealthBeat/Services/BackendWriter.swift`:
   - Add `func write<…>(_ rows: [HB<…>Row]) async throws` to the protocol.
   - Implement in `EABackendWriter` (one-line POST through `service.postRecords`).
   - Implement in `MultiBackendWriter` (`try await fanout { try await $0.write<…>(rows) }`).
7. **SyncService fan-out** — at every site that writes the new table to MySQL, build the typed rows in parallel and call `try await eaWriter?.write<…>(rows)` AFTER the `mysql.execute(...)` succeeds. Same pattern as `syncQuantityType` / `syncCategoryType` / etc. Never wrap this in a separate try/catch — let errors propagate so the offending pass is logged as failed and cursors don't advance.
8. **Reconciliation** — if the new table is UUID-keyed AND its rows can be deleted in Apple Health (so HealthKit may have fewer UUIDs than EA on a re-query), call `reconcileEverywhere(...)` at the end of the sync method exactly like the existing methods do. This is the only path that removes stale rows from EA when HealthKit drops them.
9. **Dump full-sync coverage** — the full sync (the only way to (re)establish a destination's baseline) ships data through an encrypted local dump. Add the new table to `DumpStore.DumpTable` in `Sources/HealthBeat/Services/DumpExport.swift` (with its `sqlTable` / `eaTable` / `fileName`) so `DumpFileBackendWriter` captures it on export, and add the matching `INSERT` header + row decoder in `Sources/HealthBeat/Services/DumpDrain.swift` so the MySQL drain replays it. **Without this, a user catching up a newly-enabled or reset destination silently misses the table.**
10. **EA query path** — `web/app/Services/HealthBeat/HealthBeatQueryService.php` if there's a reasonable read query for the new data (overview KPI, list, range query). Surface it from `web/app/Livewire/HealthData.php` and the EA Health Data web/iOS views if it's user-facing.
11. **Agent skill** — if the data is something users will ask the agent about, add an action to `web/app/Agent/Tools/Ea/HealthBeatTool.php` and update the table map + worked examples in `web/database/seeds/bundled_skills/ea-healthbeat/SKILL.md`.

For a new column on an existing table:

1. Add it to `SchemaService.swift` migration block + bump `currentSchemaVersion`.
2. Add it to the matching EA `hb_*` migration.
3. Add it to `COLUMN_MAP` in `HealthBeatIngestService.php`.
4. Add it to the matching `HB<…>Row` struct in `BackendRows.swift`.
5. Populate it in `SyncService` where the row is constructed (search for the `HB<…>Row(…)` literal).
6. Add the column to the per-table row decoder in `DumpDrain.swift` so the dump-based full sync carries it to MySQL.

If you skip any of these, the column lives in MySQL but is invisible everywhere else — including future agents trying to use the data the user collected.

## Backend flow summary

```
HealthKit / CoreLocation
        │
        ▼
  SyncService / LocationService
        │   builds typed HB*Row batches
        ▼
   ┌─────────────────────────┐
   │  MySQL: INSERT IGNORE   │   (existing)
   └─────────────────────────┘
        │
        ▼
   ┌─────────────────────────────┐
   │  eaWriter?.writeX(rows)     │   (mirrors to EA, no-op when disabled)
   │   → EABackendWriter         │
   │   → POST /api/v1/healthbeat │
   └─────────────────────────────┘
        │
        ▼
   (cursor advances only if BOTH succeed)
```

Reconciliation (deletes) runs at end of each per-type sync:

```
reconcileEverywhere(table, type, since, until, validUUIDs)
   → reconcileStaleRecords (MySQL DELETE)
   → eaWriter.reconcileSlice (POST /api/v1/healthbeat/reconcile)
```

## File map

- `Sources/HealthBeat/Models/MySQLConfig.swift` — direct-MySQL connection config
- `Sources/HealthBeat/Models/EAConfig.swift` — EA URL + sync key + enabled toggle
- `Sources/HealthBeat/Models/BackendRows.swift` — typed batch row structs (the contract between the writers and SyncService)
- `Sources/HealthBeat/Services/MySQLService.swift` — raw MySQL TCP client
- `Sources/HealthBeat/Services/EAService.swift` — HTTPS client for the EA ingest API (incl. reconcile + bidirectional pulls)
- `Sources/HealthBeat/Services/BackendWriter.swift` — `BackendWriter` protocol + `EABackendWriter` + `MultiBackendWriter`
- `Sources/HealthBeat/Services/SyncService.swift` — orchestrator (HealthKit → MySQL + EA fan-out)
- `Sources/HealthBeat/Services/LocationService.swift` — CoreLocation → MySQL + EA fan-out (with retry queue for transient EA outages)
- `Sources/HealthBeat/Services/GeofenceSyncService.swift` — bidirectional sync for geofences + place categories (MySQL ↔ device ↔ EA)
- `Sources/HealthBeat/Services/DumpExport.swift` — encrypted local dump export (HealthKit → dump files) + `DumpStore` run/coordination state
- `Sources/HealthBeat/Services/DumpDrain.swift` — drains a local dump into MySQL (truncate + replace, resumable per table)
- `Sources/HealthBeat/Services/DumpUpload.swift` — `EADumpUploader`: background-URLSession upload of the dump to EA in ~16 MiB **resumable chunks** (`…/bulk-import/{run}/{table}/chunk/{i}`, server reassembles into `{table}.enc`) + key delivery; progress counts chunks so a dropped slice only re-sends that slice
- `Sources/HealthBeat/Services/DumpCrypto.swift` — AES-256-GCM streaming dump file format + Keychain key storage
- `Sources/HealthBeat/Services/SchemaService.swift` — MySQL schema DDL + migration runner
- `Sources/HealthBeat/Services/BackgroundSyncManager.swift` — observer-query setup, BGProcessingTask handler
- `Sources/HealthBeat/Services/iCloudSyncService.swift` — KV-store sync of configs + per-destination baseline state across devices
- `Sources/HealthBeat/Views/Settings/MySQLSettingsView.swift` — MySQL host/credentials UI + Reset Database (clears the MySQL baseline)
- `Sources/HealthBeat/Views/Settings/EASettingsView.swift` — EA URL/sync key UI + connection test + "Re-sync EA from scratch" (clears the EA baseline)

## Live-sync vs full sync (per-destination baselines)

- **Full sync** (`SyncService.runFullSync`, dump-based) — establishes a destination's *baseline*: a one-time screen-on export of all HealthKit data to an encrypted local dump, then background delivery (EA upload + on-device MySQL drain, both truncate + replace). Each destination tracks its own baseline in `SyncState` (`mysqlBaselineDone`/`eaBaselineDone`, with `…At` timestamps for iCloud LWW). A full sync delivers **only to destinations that still need a baseline** — an already-baselined, current destination is left untouched.
- **Live sync** (`SyncService.runIncrementalSync` / `runTargetedSync`, observer-query driven) — every new HealthKit sample is mirrored within ~2s, but **only to destinations that already have a baseline**. A configured-but-un-baselined destination (enabled after the other, or whose data was reset) is skipped and surfaced on the dashboard as "needs a Full Sync". Cursors track per-type progress in `SyncState`.
- **Catch-up flow** — when a destination is enabled later, or its data is wiped (MySQL via Settings → Reset Database; EA via Settings → Re-sync EA from scratch), its baseline is cleared and the dashboard prompts a Full Sync that re-exports only to that destination. The healthy destination keeps syncing incrementally throughout.

## Sync triggers

- HKObserverQuery — debounced 2s; runs on every HealthKit change while the app is foregrounded or backgrounded.
- App foreground entry — 60s cooldown.
- BGAppRefreshTask — periodic, OS-scheduled.
- "Sync Now" button in the dashboard — manual.
- Siri Shortcut intents — `SyncHealthDataIntent`, `FullSyncHealthDataIntent`.

Every entry point calls `service.attachEAIfConfigured()` so the EA destination is picked up automatically when the user has it enabled.

## Anti-patterns to avoid

- **Writing to MySQL without writing to EA.** See the checklist above.
- **Wrapping the EA write in a separate try/catch that swallows the error.** Let it throw — `SyncState.errorMessage` will surface the failure and the cursor won't advance.
- **Hardcoded timeouts in front of `SyncService` calls.** The orchestrator decides duration based on data volume; a 30s/60s cap will kill legitimate large historical pulls mid-flight.
- **Reading from MySQL when EA's `hb_*` table can serve the same query.** EA queries are HTTP and reach the user's other clients too (web, agent); MySQL queries don't.
- **Adding a new HealthKit type and only wiring the MySQL INSERT.** EA will silently miss that data forever. See the checklist.
