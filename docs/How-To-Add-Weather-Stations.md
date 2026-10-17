This guide assumes a working SmartMet Server with observations already in use, and is for adding new stations only. This is not a guide for the initial configuration of observation engine.

## Configuration File

Locate the settings file for observation engine. You can see it's location from SmartMet Server's main configuration file in the block `engines`

```
engines:
{
	observation:
	{
		disabled        = false;
		configfile      = "engines/observation.conf";
	};
```

In the example the observation.conf file is in `engines`-directory realative to the main configuration file.


## Station types

Weather stations can be grouped by their qualities or by whatever is necessary.

Directive `stationtypes` is an array that contains the names of station groups.

These names work as values for the `producer` field in query string when using a timeseries request. [See: smartmet-plugin-timeseries](https://github.com/fmidev/smartmet-plugin-timeseries/)

```
stationtypes:
[
        "local",
        "foreign"
];
```

This example defines two station types. These names alone are not enough for the configuration to work.

## Meta Data for Station Types

The `meta_data` directive can contain additional information and/or limits to describe station types.

```
meta_data:
{
    bbox:
    {
        foreign = "-180.0,-90.0,180.0,90.0,EPSG:4326";
        default = "18.0,59.0,33.0,70.0,EPSG:4326";
    };
    timestep:
    {
        foreign = 1;
        default = 10;
    };
    first_observation:
    {
        foreign = "20190325090000";
        default = "190001010000";
    };
};
```

Example has defined the area where the stations are located with bounding box coordinates with `bbox` for station type `foreign`, and a default value for others.

Defined `timestep` (minutes) for station type named `foreign`, and a default value.

The first available point in time for station types observations with `first_observation`.
A `last_observation` time may be given the same way; the default is the current time.
The defaults are the whole world, 1 minute and 1900-01-01.

## Station Type Definitions

Each station type is defined once more in the `stationtypelist` list, now as a group with
the settings which tell what is searched from the database.

The `stationGroups` is an array of the station group codes in the database. The stations
of the station type are the members of these groups.
The `producerIds` is an array of the producer ids of the data in the database. The same
station group may hold data of several producers.

In order to _add new weather stations available in smartmet-server_, add the stations to
one of the station groups in the database, or add their group to the `stationGroups`
array of the station type. If their data is stored under a new producer, also add the
producer id to the `producerIds` array.

```
stationtypelist:
(
        {
                stationtype = "local";
                useCommonQueryMethod = true;
                stationGroups = ["ASDF", "EXTASDF"];
                producerIds = [1,13,17];
                databaseTableName = "my_database_table";
        },
        {
                stationtype = "foreign";
                stationGroups = ["EXTSYNOP"];
                producerIds = [1];
                databaseTableName = "weather_data_qc";
        }
);
```

`useCommonQueryMethod = true` reads the data from a generic observation table such as
`observation_data_r1`, and then `databaseTableName` is required. See
[Configuring-Observation-Engine.md](Configuring-Observation-Engine.md#stationtypelist) for
the details.

The station registry is loaded from `serializedStationsFile`, and refreshed from the
database by the driver which has `loadStations = true`. New stations become available
once the registry has been refreshed (`stationsCacheUpdateInterval`), or immediately with
the `reloadstations` admin request.

Now you may still need to map the available observation phenomena in the database to the
station types, see
[Configuring-Observation-Parameters.md](Configuring-Observation-Parameters.md).
