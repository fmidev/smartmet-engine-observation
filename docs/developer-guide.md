# Observation engine developer guide

This guide is for developers who change `smartmet-engine-observation`, or write plugins
that use it. It explains how a request travels from `values(settings)` to a database or a
cache, how tables are routed to database drivers, how the caches are filled and trimmed,
how stations are chosen, and the rules for keeping the plugin interface compatible.

Related documents:

* [CLAUDE.md](../CLAUDE.md): architecture summary and source groups.
* [FEATURES.md](../FEATURES.md): the feature inventory.
* [Configuring-Observation-Engine.md](Configuring-Observation-Engine.md),
  [Configuring-Observation-Parameters.md](Configuring-Observation-Parameters.md),
  [How-To-Add-Weather-Stations.md](How-To-Add-Weather-Stations.md): the configuration
  reference.
* The spine [developer guide](https://github.com/fmidev/smartmet-library-spine/blob/master/docs/developer-guide.md)
  (engine lifecycle, binary compatibility).

## Contents

1. [What the engine does](#1-what-the-engine-does)
2. [Building and testing](#2-building-and-testing)
3. [Layers](#3-layers)
4. [A request](#4-a-request)
5. [Database drivers and table routing](#5-database-drivers-and-table-routing)
6. [Caches](#6-caches)
7. [Stations](#7-stations)
8. [Station types, parameters and the DB registry](#8-station-types-parameters-and-the-db-registry)
9. [The plugin API](#9-the-plugin-api)
10. [Binary compatibility](#10-binary-compatibility)
11. [Known pitfalls](#11-known-pitfalls)

---

## 1. What the engine does

The engine answers "give me these parameters for these stations between these times" for
station observations, lightning, mobile and external observations (NetAtmo, RoadCloud,
FMI IoT, TAPSI), magnetometer data, radiosoundings and more. It:

* resolves places, coordinates, bounding boxes and station identifiers into stations;
* maps parameter names to database measurands per station type;
* routes each query to a **database driver** (PostgreSQL, SpatiaLite, Oracle through
  delfoi, or a dummy);
* serves recent data from **caches** (SpatiaLite or PostgreSQL on disk, plus in-memory
  caches), which background threads keep filled from the databases;
* returns `TS::TimeSeriesVectorPtr` for the timeseries-style plugins, and implements
  `makeQuery()` for the WFS stored queries.

Its main users are the timeseries, wfs, edr and wms plugins.

## 2. Building and testing

```bash
make                 # observation.so
make test            # Catch2 tests in test/
make test-headers    # every header compiles on its own
cd test && make ObservationMemoryCacheTest && ./ObservationMemoryCacheTest
```

The unit tests need `../observation.so` and an installed geonames engine
(`/usr/share/smartmet/engines/geonames.so`). The engine's behaviour against real data is
tested by the timeseries, wfs and edr plugin suites, which run against SQLite, a local
PostgreSQL test database, or (at FMI) Oracle. `cnf/db_registry/` and `cnf/stations.*` are
the test registry and station files.

The station file used by the tests comes from the `smartmet-test-data` package. If
observation tests fail oddly, first check that the packaged `stations.txt` is intact
(`rpm -V smartmet-test-data`).

## 3. Layers

```
Engine (pure virtual API)              DisabledEngine (empty answers when not configured)
  └─ EngineImpl
       ├─ EngineParameters             configuration, station types, parameter map,
       │                               StationInfo (shared, AtomicSharedPtr), shared caches
       └─ DatabaseDriverProxy          routes each call to a driver by table and time
            └─ DatabaseDriverContainer
                 ├─ PostgreSQLDatabaseDriverForFmiData / ForMobileData
                 ├─ SpatiaLiteDatabaseDriver
                 ├─ Oracle drivers (loaded from delfoi via DatabaseDriverFactory)
                 └─ DummyDatabaseDriver
                 each with ObservationCacheAdmin* → caches:
                      SpatiaLiteCache / PostgreSQLCache (disk)
                      ObservationMemoryCache, FlashMemoryCache (memory)
                      DummyCache, ObservationCacheProxy
```

## 4. A request

`EngineImpl::values(settings)`:

1. **Adjust the settings** (`beforeQuery()`): drop unknown parameters (remembering their
   positions, so the result keeps the requested column order with missing columns), and
   limit `stationgroups` to the groups the station type allows (a caller cannot add
   groups).
2. **Choose the driver** (§5) by the station type's table and the requested period.
3. **The driver**:
   * checks that every parameter is known for the station type (`parameterSanityCheck`);
   * applies the station type's default `data_quality` filter unless the request has
     one;
   * **cache or database**: if the station type is cached and the cache covers the
     **whole** requested interval (`dataAvailableInCache()`), the answer comes from the
     cache (`valuesFromCache()`); otherwise from the database;
   * resolves the stations (§7), runs the query, and builds the time series in the
     requested time zone with the requested time step.
4. The result is a `TS::TimeSeriesVectorPtr`, one series per parameter.

`Settings` (see `Settings.h`) carries the locations (`taggedLocations`, `taggedFMISIDs`,
WKT area, bounding box), `parameters`, `stationtype` and `stationgroups`, `starttime` /
`endtime` (default: the last 24 hours), `timestep`, `wantedtime`, `hours` / `weekdays`,
`numberofstations` (1) and `maxdistance` (50 km), `dataFilter`, `timezone`,
`useDataCache` (true), `preventDatabaseQuery`, and the request limits.

## 5. Database drivers and table routing

Drivers are declared in `database_driver_info`:

```
database_driver_info:
(
  { name = "postgresql_replica_fmi_observations";
    tables = ["observation_data:28", "weather_data_qc:28", "flash_data:28"];
    caches = ["spatialite_common_cache:observation_data,weather_data_qc,flash_data"];
    active = true; },
  { name = "postgresql_master_fmi_observations";
    tables = ["weather_data_qc:800", "observation_data", "flash_data", "measurand"];
    caches = [];
    active = true; },
  { name = "oracle_fmi_observations";
    tables = ["weather_data_qc", "radiosounding", "stuk_radionuclide", "aviobs"];
    caches = [];
    active = true; }
);
```

* The **name** selects the implementation (`postgresql_…`, `spatialite_…`, `oracle_…`,
  and the FMI-data, mobile and magnetometer variants); `active = false` skips it.
* **`tables`** lists what the driver serves. `table:N` limits it to data from the last `N`
  days.
* **`caches`** lists the caches this driver owns and fills, as
  `cache-name:table,table,…`; empty means no cache.
* Per-driver parameters (update intervals, cache durations, pool sizes) may be added to
  each group and override the engine-wide values (`DatabaseDriverBase` reads them).

**Routing** (`DatabaseDriverContainer::resolveDriver(table, start, end)`): the drivers for
the table are sorted by their day limit, and the first one whose window
`[now − N days, now]` contains the request's **start time** takes the **whole** request. A
driver without a limit is the fallback. Without a start or end time, the first driver is
used. In the example, a query for the last week of `observation_data` goes to the replica
(and its cache), and a query that starts two months ago goes to the master database, even
for the part that is recent. If no driver is active at all, a dummy driver serves every
table.

## 6. Caches

Each cache-owning driver runs **update threads** (`ObservationCacheAdminBase`), one per
data kind, every `finUpdateInterval` (FMI observations), `extUpdateInterval` (mobile and
external), `flashUpdateInterval` (lightning), and the corresponding station and
magnetometer intervals. An update:

1. asks the cache for its latest data time and modification time, and subtracts
   `updateExtraInterval` to catch rows that arrived late;
2. reads the newer rows from the database, in chunks of `finCacheUpdateSize` hours when the
   gap is large (after a restart);
3. inserts them (`fillDataCache()`, `fillMovingLocationsCache()`);
4. trims the disk cache to `finCacheDuration` hours and the memory cache to
   `finMemoryCacheDuration` hours (`cleanDataCache()`), and likewise for the other kinds.

At start, the driver's `init()` runs one update of every cache in parallel
(`startInitialCacheUpdates()`), loads the stations from the database if the stations file
was missing, **waits for all of it**, and only then starts the periodic update threads. The
engine's `init()`, and so every plugin waiting in `getEngine()`, waits with it.
`cache.disableUpdates` (or `disableAllCacheUpdates`) stops the updates, which is useful
with a prepared test cache. A cache file that is read-only while updates are enabled is
refused at start. "Fake cache" settings let a cache emulate data periods for
tests.

The **memory caches** (`ObservationMemoryCache`, `FlashMemoryCache`) sit in front of the
disk cache for the most recent hours. The **station resolution caches** (nearest-station
candidates, sized by `nearestStationsCacheSize`) are shared by all drivers through
`EngineParameters`; `cache.locationCacheSize` sizes the caches' own location lookups.

## 7. Stations

`StationInfo` is the station registry: loaded from `serializedStationsFile` (if the file
is missing or empty, the registry starts empty and a cache-owning driver loads the
stations from the database during its start-up), with a NearTree for nearest-station searches and
indexes by fmisid, WMO, LPNN, RWSID and WSI. It is replaced atomically on
`reloadStations()` (`Fmi::AtomicSharedPtr`), so queries never see a half-built registry.

`translateToFMISID()` turns the request's locations into a list of tagged fmisids:

1. **Explicit identifiers** (`fmisid=`, `wmo=`, `lpnn=`, `wsi=`, geoids) are taken as they
   are.
2. **A bounding box** gives every station inside it.
3. **Places and coordinates** become nearest-station searches: the stations within
   `maxdistance` of the point, sorted by distance (ties by station id, so results are
   deterministic), filtered to the requested station groups and to stations in use during
   the requested period, up to `numberofstations`. When the data comes from a cache,
   `nearestStationExtraCandidates` (3) extra candidates are fetched, so that stations
   lacking the requested parameters can be skipped without reducing the count. If the place itself is a
   station (geonames stores fmisids as alternative names) and that station is usable, it is
   used directly.
4. **A station named explicitly is never substituted.** If the caller asked for
   `fmisid=100963` and that station has no data for the station type or period, the result
   for it is empty. It is not replaced by the nearest other station, even though the same
   id also arrives as a nearest-station search.

The candidate search depends only on the geometry, not on the time or the groups, which is
why its results can be cached.

## 8. Station types, parameters and the DB registry

* **`stationtypes`** lists the valid station types; **`oracle_stationtypelist`** (used by
  all drivers despite its name) configures each: `stationGroups`, `producerIds`,
  `databaseTableName`, `cached`, and `useCommonQueryMethod` (query the generic
  `observation_data`-style table instead of a type-specific one).
* **`parameters`** maps each parameter name to a measurand per station type (see
  Configuring-Observation-Parameters.md). `isParameter()`, `getParameterId()` and
  `isParameterVariant()` answer from this map.
* **The DB registry** (`dbRegistryFolderPath`, `cnf/db_registry/*.conf`) describes the
  database tables for `makeQuery()` (the WFS stored queries): column names, types and
  methods.
* Data quality filters default per station type and can be overridden with
  `data_quality` in the request's data filter.

## 9. The plugin API

Plugins get the engine with `reactor->getEngine<Engine::Observation::Engine>("observation")`.
If the engine is disabled in the server configuration, they get a `DisabledEngine` that
returns empty results.

| Call | Purpose |
|------|---------|
| `values(settings)`, `values(settings, timeSeriesOptions)` | Time series for the requested stations and parameters. |
| `makeQuery(queryBase)` | DB-registry-driven queries for WFS. |
| `getFlashCount()`, `observablePropertyQuery()` | Lightning counts; observable properties for WFS metadata. |
| `getStations()`, `getStationsByArea()`, `getStationsByBoundingBox()`, `translateToFMISID()` | Station lookups. |
| `isParameter()`, `isParameterVariant()`, `getParameterId()`, `getValidStationTypes()`, `isValidStationType()` | Parameter and station type checks. |
| `getProducerInfo()`, `getParameterInfo()`, `getStationInfo()`, `metaData()`, `getMeasurandInfo()`, `getLatestDataUpdateTime()` | Metadata and admin tables. |
| `reloadStations()`, `ready()`, `getGeonames()`, `dbRegistry()` | Maintenance and accessors. |

## 10. Binary compatibility

`Engine` is an abstract class of **pure virtual** methods, and plugins call through its
vtable. Add new virtual methods **at the end** of `Engine`; never insert, reorder or
remove them. Changing the layout of `Settings`, `QueryBase` or the other types plugins
construct is also an ABI change. Release the engine and the plugins that use it together,
and bump their `Requires:`. An old plugin calling a shifted vtable slot does not fail to
load; it calls the wrong method.

## 11. Known pitfalls

* **Routing is by start time.** A request whose start lies outside a fast driver's window
  goes entirely to the slower one (and bypasses that driver's cache), even for the recent
  part.
* **The cache must cover the whole interval.** A request that starts one hour before the
  cache begins goes to the database for all of it.
* **Explicit stations are never replaced** (§7). An explicit `fmisid` with no data gives an
  empty result, by design.
* **Unknown parameters are dropped silently** in `beforeQuery()` and come back as missing
  columns, not as an error.
* **Start-up waits for the caches.** The engine's initialisation includes one full cache
  update (§6); after a long downtime, with long cache durations or a slow database, that
  is the whole server's start-up time.
* **The Oracle drivers come from delfoi at run time.** A server without
  `smartmet-library-delfoi` cannot use `oracle_…` drivers, and the failure appears only
  when the driver is created.
* **Pure virtual API** (§10).
