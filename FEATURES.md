# smartmet-engine-observation — Feature List

A structured inventory of capabilities provided by the observation
engine. Use as a checklist when drafting release notes. When new
functionality is added, append the new entry under the matching
section (and bump the *Last updated* line at the bottom).

`smartmet-engine-observation` (output: `observation.so`) is the
SmartMet Server engine that gives plugins shared access to weather
**station observations** — surface, upper-air, lightning,
mobile/external, and magnetometer data. It supports multiple database
backends (PostgreSQL, Oracle via delfoi, SpatiaLite/SQLite), keeps a
multi-level cache (in-memory + disk), and resolves stations by
multiple identifier types.

---

## 1. Engine surface for plugins

- **`Engine::values(Settings)`** — primary query entry point;
  returns `TS::TimeSeriesVectorPtr`.
- **`Engine::values(Settings, areaPolygon)`** — variant scoped to an
  area.
- **`Engine::getStations(stations, settings)`** — resolve stations
  matching a query.
- **`Engine::getStationsByArea`**, **`getStationsByBoundingBox`** —
  spatial lookup variants.
- **`Engine::getFlashCount(start, end, area)`** — count lightning
  flashes in a region/time window.
- **`Engine::reloadStations()`** — force station-info reload.
- **`Engine::getProducerInfo(producer)`** — formatted table of
  configured producers.
- **`Engine::getMeasurandInfo()`** — map of producer-specific
  measurand metadata.

## 2. Two-mode engine

- **`EngineImpl`** — full implementation: loads config, initialises
  drivers + caches, runs queries.
- **`DisabledEngine`** — null implementation returning empty results
  when the engine is disabled in server config.
- **`Engine::create()`** factory picks which to instantiate.

## 3. Supported station / observation types

- **Synoptic / climatological surface observations.**
- **Road-weather stations (RWSID).**
- **Lightning observations** — separate `FlashMemoryCache`,
  `FlashDataItem`.
- **Magnetometer data** — `MagnetometerDataItem` and dedicated
  driver path.
- **External / mobile observations** — `MobileExternalDataItem`,
  `ExternalAndMobileProducerConfig`, `ExternalAndMobileDBInfo`.
- **Radiosoundings** — schema registered in the DB registry.

## 4. Station identification

- **FMISID** — Finnish Meteorological Institute station id.
- **WMO** — World Meteorological Organization id.
- **LPNN** — Finnish radar/sounding network id.
- **RWSID** — Road-weather station id.
- **WSI** — WIGOS station identifier.
- **GeoID** — GeoNames id.
- **By name** — case-insensitive station-name lookup.
- **By coordinate** — nearest-station via spatial index.

## 5. Spatial indexing

- **`StationInfo`** — central station registry rebuilt from a
  serialized file (`stations.txt`).
- **NearTree-backed nearest-station search.**
- **`stations.sqlite`** — SpatiaLite index for spatial joins.
- **`StationGroups`** — group station sets together (synoptic,
  road, ...).

## 6. Database driver layer

All database access goes through `DatabaseDriverInterface`. Concrete
drivers:

- **`PostgreSQLDatabaseDriver`** — with `FmiData` and `MobileData`
  specialisations.
- **`SpatiaLiteDatabaseDriver`** — SQLite + SpatiaLite for
  lightweight setups.
- **Oracle drivers** — loaded dynamically at runtime via
  `DatabaseDriverProxy` / `DatabaseDriverFactory` from
  `smartmet-library-delfoi`.
- **`DummyDatabaseDriver`** — testing / disabled mode.
- **`DatabaseDriverContainer`** — keeps the set of active drivers.
- **`DatabaseDriverProxy`** — delegating proxy used to switch
  drivers transparently.
- **Connection pooling** via `Fmi::Pool`.
- **`DBRegistry`** + **`DBRegistryConfig`** — table-schema registry
  loaded from `cnf/db_registry/` (20+ table definitions).

## 7. Multi-level caching

Two levels, both `ObservationCache`-derived:

- **Disk cache** — `PostgreSQLCache` or `SpatiaLiteCache`; survives
  restarts.
- **In-memory caches**:
  - **`ObservationMemoryCache`** — surface / generic observations.
  - **`FlashMemoryCache`** — lightning data.
- **`DummyCache`** — no-op variant for disabled mode.
- **`ObservationCacheProxy`** — dispatcher between caches.
- **Cache fill / refresh** — periodic background updates from the
  source database.

## 8. Query model

- **`Settings`** — request parameters (locations, time range,
  parameters, station type, time-zone, time-step, missing-value
  handling, etc.).
- **`QueryBase`** / **`QueryResult`** — abstract query types
  consumed by drivers.
- **`DBQueryUtils`** — SQL building helpers.
- **`DataItem`**, **`FlashDataItem`**, **`MobileExternalDataItem`**,
  **`MagnetometerDataItem`** — typed data rows returned from drivers.
- **`DataWithQuality`** — value + quality flag pair.
- **`AsDouble`** — uniform conversion helper for mixed-type DB
  values.

## 9. Parameter mapping

- **`ParameterMap`** — maps human-readable parameter names
  (`Temperature`, `WindSpeedMS`, ...) to backend measurand IDs and
  column names per station type.
- **`EngineParameters`** — config-loaded parameter inventory.
- **`MeasurandInfo`** — per-producer measurand catalogue.
- **`StationtypeConfig`** — per-station-type parameter aliases.

## 10. Concurrency

- **`Fmi::AtomicSharedPtr<StationInfo>`** — lock-free station-info
  updates; readers never block during reloads.
- **Connection-pool backed drivers** — `Fmi::Pool` per driver.
- **Background reload** — station info and caches refresh on
  configurable intervals.

## 11. Aviation / OGC support

- **`FEConformanceClassBase`** — Feature-Encoding helper for OGC
  Filter Encoding conformance, used by the WFS plugin.

## 12. Configuration

libconfig file (`observation.conf`) with SmartMet extensions:

- **Database connection** — per-driver parameters.
- **Station types** — each station type's defaults, parameter
  aliases, and producer mappings.
- **Parameter aliases** — human names ↔ measurand columns.
- **Cache settings** — sizes, refresh intervals, disk paths.
- **DB registry path** — directory of `*.conf` files describing
  table schemas.
- **`cnf/db_registry/`** — 20+ schema descriptors for observations,
  radiosoundings, stations, measurands, etc.

## 13. Documentation

Under `docs/`:

- **`Configuring-Observation-Engine.md`** — configuration guide.
- **`Configuring-Observation-Parameters.md`** — parameter mapping
  guide.
- **`How-To-Add-Weather-Stations.md`** — station ingestion guide.

## 14. Tooling & deployment

- **`docker/`** — Dockerfile + `etc/` config skeleton + PostGIS
  initdb script (`initdb-postgis.sh`) for setting up an observation
  database container.

## 15. Testing

- **Catch2-based unit tests** under `test/`:
  - `ObservationMemoryCacheTest.cpp`
  - `StationInfoTest.cpp`
- **Test prerequisites**: `../observation.so` and
  `/usr/share/smartmet/engines/geonames.so` installed.
- **Per-test run**:
  `cd test && make ObservationMemoryCacheTest && ./ObservationMemoryCacheTest`.
- **`make test-headers`** — verifies each header compiles on its
  own (catches missing includes).

## 16. Build & integration

- **Output**: `observation.so`.
- **Loaded at**: `$(prefix)/share/smartmet/engines/observation.so`.
- **Build**: `make`.
- **Format**: `make format` runs clang-format.
- **Install**: `make install`.
- **RPM**: `make rpm`.
- **pkg-config requirements**: `gdal`, `configpp`, `sqlite3`,
  `spatialite`.
- **SmartMet libraries**: `smartmet-library-timeseries`,
  `smartmet-library-spine`, `smartmet-library-macgyver`,
  `smartmet-library-locus`, `smartmet-library-gis` (+ optional
  `smartmet-library-delfoi` for Oracle).
- **External libraries**: Boost (thread, iostreams, locale,
  serialization).
- **Unresolved-symbol check** — link step exempts
  `SmartMet::Engine::Geonames::*` because those are resolved at
  runtime when both engines load.
- **CI**: CircleCI on RHEL 8 / RHEL 10 with the
  `fmidev/smartmet-cibase-{8,10}` Docker images.

---

*Last updated: 2026-06-01.*
