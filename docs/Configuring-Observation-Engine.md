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

## Available Settings

TODO: We need to explain the content of the configuration file

* List all availble settings that can be included in the file
* Tell if the setting is mandatory or not
* What possible values can the setting have
* What value is a lot, what value is too little 
* What are the default values, if any
* How each value affects the SmartMet Server
* Are there any requirements for a setting (I'm looking at you SQLite)

### `quiet`

TODO

### `timer`

TODO

### `poolsize`

TODO

### `spatialitePoolSize`

TODO

### `maxInsertSize`

TODO

### `dbRegistryFolderPath`

TODO

### `serializedStationsFile`

TODO

### `spatialiteFile`

TODO


### `finUpdateInterval`

TODO


### `extUpdateInterval`

TODO

### `flashUpdateInterval`

TODO

### `cache`

TODO

```text
{
	poolSize = 10;
	disableUpdates = true;
	boundingBoxCacheSize = 10000;
	stationCacheSize = 10000;
	resultCacheSize = 10000;
	locationCacheSize = 10000;
	spatialiteCacheDuration = 36;
	spatialiteFlashCacheDuration = 17600;
};
```

### `database`

TODO

```text
{
	service = "...";
	username = "...";
	password = "...";
	nls_lang = "NLS_LANG=.UTF8";
};
```

### `sqlite`

TODO

```text
{
	threading_mode	= "MULTITHREAD";	// MULTITHREAD | SERIALIZED
	timeout		= 10000;		// milliseconds
	shared_cache	= false;
	memstatus	= false;		// disable statistics
	synchronous	= "NORMAL";		// OFF=0, NORMAL=1, 2=FULL, 3=EXTRA
	journal_mode	= "WAL";		// DELETE | TRUNCATE | PERSIST | MEMORY | WAL | OFF
	mmap_size       = 0L;
};
```


### `stationtypes`

TODO

```text
[
	"fmi"
];
```

### `oracle_stationtypelist`

TODO

```text
{
	fmi:
	{
		cached = true;
		stationGroups = ["ASDF", "FDSA"];
		producerIds = [1,2,3];
	};
};
```

### `oracle_stationtype`

TODO

```text
{
	fmi = "1";
};
```

### `parameters`

TODO

```text
[
	"Temperature"
];
```

## Map Parameters To Database

TODO: Explain why is this necessary, and how it should be done.

```text
Temperature:
{
	fmi = "t";
};
```
