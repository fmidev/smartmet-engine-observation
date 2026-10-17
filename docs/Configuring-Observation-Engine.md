This page should explain how to setup the configuration file for Observation Engine.

## Enable Engine Configuration

Add the `observation` definition to `engines` structure in your main configuration file.

```text
engines:
{
  observation: { 
    configfile = "engines/observation.conf";
  }; 
};
```

The location for engines configuration can be a relative or an absolute path. In the example the configuration is relative to the main configuration in a directory named `engines` in a file named `observation.conf`.

## Disabling the engine

The engine can be loaded in a dummy mode that needs no database, for plugins that look it
up at startup but are not configured to use observations. The dummy (`DisabledEngine`)
returns empty results for time series, station and flash queries, and throws for WFS
registry queries (`makeQuery`). Dummy mode is enabled in either of two ways:

1. Leave `configfile` out of the `observation` section, or set it to an empty string:

   ```text
   engines:
   {
     observation:
     {
       configfile = "";
     };
   };
   ```

2. Set the engine's own disable flag in its configuration file. No other settings are
   read, so this line alone is enough:

   ```text
   disabled = true;
   ```

Setting `disabled = true` in the server's `engines.observation` section is different: the
server then does not load `observation.so` at all, and plugins that require the engine fail
at startup.

## Available Settings

The settings are read with the usual SmartMet extensions (`@include`, `$(VAR)`, ...).
Most of the settings of the database drivers and caches are not at the top level but in
the `database_info` group, under the name of the driver or cache.

### `quiet`

Optional boolean, default `true`. Suppress informational messages. The value is also the
default of the `quiet` setting of each driver and cache, where the default is `false` if
the top-level setting is missing.

### `serializedStationsFile`

Required path. The file the station registry is loaded from at start up, and where a
driver which loads the stations from the database saves them. If the file is missing or
empty, the registry starts empty and the stations are loaded from the database.

### `dbRegistryFolderPath`

Required path. The directory of the DB registry files (see `cnf/db_registry/`), which
describe the database tables for the WFS stored queries.

### `dbDriverFile`

Optional path, needed only by the Oracle drivers: the shared library which implements them
(`libsmartmet-delfoi.so`).

### `disable_nonfree_stations`

Optional boolean, default `false`. When `true`, stations loaded from PostgreSQL must have
`open_data = 'Y'` in `station_metadata_access_policy`. Stations whose data may be used
freely only inside FMI are then discarded. Enable this on open data servers only.

### `nearestStationExtraCandidates`

Optional integer, default 3. When the data comes from a cache, this many extra stations
are fetched in nearest station searches, so that stations lacking the requested
parameters can be skipped without reducing the number of stations.

### `cache`

Optional sizes (number of entries) of the caches shared by all database drivers:

```text
cache:
{
	nearestStationsCacheSize = 100000;	// nearest station candidates of coordinates
	geoIdCacheSize           = 10000;	// places resolved from geoids
	queryResultBaseCacheSize = 1000;	// query results of the PostgreSQL drivers
};
```

The values shown are the defaults.

### `stationtypes`

Required list of the station types, that is the values of the `producer` option of the
plugins:

```text
stationtypes:
[
	"fmi",
	"road"
];
```

### `stationtypelist`

Required list of the settings of each station type:

```text
stationtypelist:
(
	{
		stationtype          = "fmi";
		useCommonQueryMethod = true;
		stationGroups        = ["AWS", "SYNOP", "CLIM", "AVI"];
		producerIds          = [13,17,27,2,3,14,26,28];
		databaseTableName    = "observation_data_r1";
	},
	{
		stationtype   = "road";
		stationGroups = ["EXTRWS"];
		producerIds   = [1];
		databaseTableName = "weather_data_qc";
	}
);
```

* `stationtype` is required.
* `stationGroups` are the station groups of the station type in the database.
* `producerIds` are the producer ids of the data in the database.
* `databaseTableName` is the table the data is read from. It is required when
  `useCommonQueryMethod` is true.
* `useCommonQueryMethod` (default `false`) queries the generic `observation_data` style
  table instead of a station type specific one.

The mobile and external station types (`roadcloud`, `teconer`, `netatmo`, `fmi_iot` and
`tapsi_qc`) take only `producerIds`, which must hold the producer id, and an optional
`databaseTableName` (default `ext_obsdata`).

### `data_quality_filter`

Optional. The data quality filter used when the request has none, by default `le 5`, and
overrides per station type:

```text
data_quality_filter:
{
	default_filter = "le 5";
	override:
	{
		road = "le 9";
	};
};
```

### `meta_data`

Optional metadata of the station types, see
[How-To-Add-Weather-Stations.md](How-To-Add-Weather-Stations.md#meta-data-for-station-types).

### `parameters`

Required. The parameter names and their mapping to the database, see
[Configuring-Observation-Parameters.md](Configuring-Observation-Parameters.md).

### `database_driver_info`

Required list of the database drivers:

```text
database_driver_info:
(
	{
		name   = "postgresql_master_fmi_observations";
		tables = ["observation_data", "weather_data_qc", "flash_data", "measurand"];
		caches = ["spatialite_common_cache:observation_data,weather_data_qc,flash_data"];
		active = true;
	}
);
```

* `name` selects the implementation by its prefix (`postgresql_`, `spatialite_` or
  `oracle_`). The names of the drivers serving data end with `_observations`, and a
  PostgreSQL driver whose name ends with `mobile_observations` serves the mobile and
  external observations.
* `tables` lists the tables the driver serves. `table:N` limits it to the data of the
  last `N` days.
* `caches` lists the caches the driver owns and fills, as `cache-name:table,table,...`.
  The names of the caches end with `_cache`, and their prefix (`spatialite_` or
  `postgresql_`) selects the implementation.
* `active = false` skips the driver.

See the [developer guide](developer-guide.md#5-database-drivers-and-table-routing) for how
a request is routed to a driver.

### `database_info`

The connections and settings of the drivers and caches, in two groups keyed by the name of
the driver or cache:

```text
database_info:
{
	connect_info:
	{
		postgresql_master_fmi_observations:
		{
			host            = "localhost";
			port            = 5432;
			database        = "obsdb";
			username        = "...";
			password        = "...";
			encoding        = "UTF8";
			connect_timeout = 30;
			// slow_query_limit = 0;
		};
		spatialite_common_cache:
		{
			spatialiteFile = "/var/smartmet/observation/cache.sqlite";
		};
	};
	common_info:
	{
		postgresql_master_fmi_observations:
		{
			loadStations           = true;
			finCacheUpdateInterval = 60;
			finCacheDuration       = 36;
		};
		spatialite_common_cache:
		{
			...
		};
	};
};
```

**`connect_info`** of a PostgreSQL driver or cache requires `host`, `port`, `database`,
`username`, `password`, `encoding` and `connect_timeout`; `slow_query_limit` is optional.
A SpatiaLite cache requires `spatialiteFile`. An Oracle driver requires `service`,
`username` and `password`, and takes optional `nls_lang` (default `NLS_LANG=.UTF8`) and
`poolSize` (default: the top-level `database_driver.poolSize`). An `override` list of
groups with a `name` list of host name prefixes overrides these settings on matching
hosts, and an `extra` list of the same form adds a second connection on matching hosts.
The Oracle settings may also be in a separate file whose path is given instead of the
group.

**`common_info`** of a PostgreSQL or Oracle driver, all optional:

| Setting | Default | Meaning |
|---------|---------|---------|
| `quiet` | top-level `quiet` | Suppress informational messages. |
| `poolSize` | 10 | Connection pool size. |
| `loadStations` | false | Load the stations from the database into `serializedStationsFile`. Only one driver may do it. |
| `connectionTimeout` | 30 | Seconds. |
| `timer` | false | Print query timings. |
| `disableAllCacheUpdates` | false | Do not update the caches of the driver. |
| `finCacheUpdateInterval`, `extCacheUpdateInterval`, `flashCacheUpdateInterval`, `magnetometerCacheUpdateInterval`, `stationsCacheUpdateInterval` | 0 | Seconds between the cache updates of FMI observations, road and foreign observations, lightning, magnetometer data and stations. 0 disables the updates. |
| `updateExtraInterval` | 10 | Seconds subtracted from the latest modification time when updating, to catch late rows. |
| `finCacheDuration`, `extCacheDuration`, `flashCacheDuration`, `magnetometerCacheDuration` | 0 | Hours of data kept in the disk caches. |
| `finMemoryCacheDuration`, `extMemoryCacheDuration`, `flashMemoryCacheDuration` | 0 | Hours of data kept in the memory caches, 0 disables them. |
| `finCacheUpdateSize`, `extCacheUpdateSize` | 0 | Hours read from the database at a time when the cache is far behind, 0 for no limit. |
| `flash_emulator` | | `active`, `bbox` and `strokes_per_minute` of generated test lightning. |

The mobile driver (`..._mobile_observations`) takes `quiet`, `poolSize`,
`connectionTimeout`, `disableAllCacheUpdates`, `loadFmiIoTStations` (default `true`), and
the update intervals and cache durations `roadCloudCacheUpdateInterval`,
`netAtmoCacheUpdateInterval`, `fmiIoTCacheUpdateInterval`, `tapsiQcCacheUpdateInterval`,
`roadCloudCacheDuration`, `netAtmoCacheDuration`, `fmiIoTCacheDuration` and
`tapsiQcCacheDuration`.

A PostgreSQL cache takes `quiet`, `poolSize` (10), `maxInsertSize`, and the insert cache
sizes `dataInsertCacheSize`, `movingLocationsInsertCacheSize`,
`weatherDataQCInsertCacheSize`, `flashInsertCacheSize`, `roadCloudInsertCacheSize`,
`netAtmoInsertCacheSize`, `fmiIoTInsertCacheSize`, `tapsiQcInsertCacheSize` and
`magnetometerInsertCacheSize`, all optional.

A SpatiaLite cache requires these settings:

```text
spatialite_common_cache:
{
	poolSize		= 50;		// connections
	maxInsertSize		= 5000;		// rows per insert
	locationCacheSize	= 10000;		// required but currently not used
	dataInsertCacheSize	= 100000;	// insert caches avoid inserting the same rows again
	weatherDataQCInsertCacheSize = 100000;
	flashInsertCacheSize	= 100000;
	threads			= 0;		// SQLite helper threads, 0 = none
	timeout			= 30000;	// milliseconds
	cache_size		= 0;		// SQLite cache_size: 0 = default, positive = pages, negative = KiB
	shared_cache		= false;
	read_uncommitted	= false;
	synchronous		= "NORMAL";	// OFF | NORMAL | FULL | EXTRA
	journal_mode		= "WAL";	// DELETE | TRUNCATE | PERSIST | MEMORY | WAL | OFF
	auto_vacuum		= "NONE";	// NONE | FULL | INCREMENTAL
	temp_store		= "DEFAULT";	// DEFAULT | FILE | MEMORY
	wal_autocheckpoint	= 1000;		// pages
	mmap_size		= 0;		// bytes
};
```

The other insert cache sizes (`movingLocationsInsertCacheSize`,
`roadCloudInsertCacheSize`, `netAtmoInsertCacheSize`, `fmiIoTInsertCacheSize`,
`tapsiQcInsertCacheSize`, `magnetometerInsertCacheSize`) are optional, as is `quiet`. A
`fake_cache` group may define fixed data periods per table (`observation_data`,
`weather_data_qc`, `flash_data`) for tests.

### `database_driver.poolSize`

The default connection pool size of the Oracle drivers, required when an Oracle driver
does not set `poolSize`.

### `disabled`

See [Disabling the engine](#disabling-the-engine).
