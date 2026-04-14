# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

The observation engine (`smartmet-engine-observation`) provides weather station observation data to the SmartMet Server. Plugins like `timeseries` and `wfs` use it to query station observations. It supports multiple database backends (PostgreSQL, Oracle via delfoi, SpatiaLite) with a multi-level caching architecture (disk cache + in-memory cache).

## Build commands

```bash
make                  # Build observation.so
make test             # Build and run all tests
make format           # Run clang-format on all source files
make clean            # Remove build artifacts
make rpm              # Build RPM package
make test-headers     # Verify each header compiles independently
```

Run a single test:
```bash
cd test && make ObservationMemoryCacheTest && ./ObservationMemoryCacheTest
```

Tests require `../observation.so` to be built first and `/usr/share/smartmet/engines/geonames.so` to be installed. Test framework is Catch2.

## Key dependencies

pkg-config: `gdal`, `configpp`, `sqlite3`, `spatialite`. Links against: `smartmet-timeseries`, `smartmet-spine`, `smartmet-macgyver`, `smartmet-locus`, `smartmet-gis`, Boost (thread, iostreams, locale, serialization).

## Architecture

### Engine abstraction

`Engine` (abstract) has two implementations:
- **`EngineImpl`** — the real engine: loads config, initializes caches and database drivers, processes queries
- **`DisabledEngine`** — null object that returns empty results when the engine is disabled in server config

The factory `Engine::create()` decides which to instantiate. The engine is loaded dynamically by SmartMet Server via the `engine_class_creator()` C entry point in `Engine.cpp`.

### Database driver layer

All database access goes through `DatabaseDriverInterface`. Concrete drivers:
- **`PostgreSQLDatabaseDriver`** (with FmiData and MobileData specializations)
- **`SpatiaLiteDatabaseDriver`** — lightweight SQLite-based alternative
- **`DummyDatabaseDriver`** — for testing/disabled mode

Oracle drivers are loaded dynamically at runtime via `DatabaseDriverProxy` / `DatabaseDriverFactory` from `smartmet-library-delfoi`.

### Caching architecture

Two-level cache: disk-backed cache (`PostgreSQLCache` or `SpatiaLiteCache`) fronted by in-memory caches (`ObservationMemoryCache` for observations, `FlashMemoryCache` for lightning data). All implement `ObservationCache`. There's also `DummyCache` for no-op mode and `ObservationCacheProxy` for delegation.

Thread safety relies on `Fmi::AtomicSharedPtr` for lock-free station info updates and `Fmi::Pool` for database connection pooling.

### Station management

`StationInfo` is the central station registry — loaded from a serialized file (`stations.txt`), with spatial indexing via NearTree for geographic nearest-station queries. Lookup methods exist for FMISID, WMO, LPNN, RWSID, and WSI identifiers.

### Query flow

1. Plugin calls `Engine::values(Settings)` with query parameters (locations, time range, parameters, station type)
2. `EngineImpl::beforeQuery()` validates and adjusts settings
3. Request is delegated to the database driver
4. Driver checks cache first (`ObservationMemoryCache` → disk cache → database)
5. Results returned as `TS::TimeSeriesVectorPtr`

### Configuration

libconfig format. Main config defines: database connection, station types, parameter aliases (mapping human names to DB fields), cache settings, and DB registry path. The `cnf/db_registry/` directory contains 20 `.conf` files that define schemas for database tables (observations, radiosoundings, stations, measurands, etc.).

## Source layout

All source (155 files) lives in `observation/`. Key groupings:
- **Engine core**: `Engine.h/cpp`, `EngineImpl.h/cpp`, `DisabledEngine.h/cpp`
- **DB drivers**: `DatabaseDriverInterface.h`, `DatabaseDriverBase.*`, `PostgreSQLDatabaseDriver.*`, `SpatiaLiteDatabaseDriver.*`, `DummyDatabaseDriver.*`, `DatabaseDriverProxy.*`, `DatabaseDriverFactory.*`
- **Cache**: `ObservationCache.h`, `PostgreSQLCache.*`, `SpatiaLiteCache.*`, `ObservationMemoryCache.*`, `FlashMemoryCache.*`, `DummyCache.*`
- **Stations**: `StationInfo.*`, `DatabaseStations.*`, `StationGroups.*`
- **Config**: `EngineParameters.*`, `Settings.*`, `DBRegistry.*`, `DBRegistryConfig.*`, `StationtypeConfig.*`, `ParameterMap.*`
- **Query/data types**: `QueryBase.*`, `QueryResult.*`, `DataItem.*`, `FlashDataItem.*`, `MobileExternalDataItem.*`, `MagnetometerDataItem.*`

## Unresolved symbol checking

The Makefile's link step explicitly checks for unresolved symbols with `ldd -r`, but intentionally exempts `SmartMet::Engine::Geonames::*` references — these are resolved at runtime when SmartMet Server loads both engines.
