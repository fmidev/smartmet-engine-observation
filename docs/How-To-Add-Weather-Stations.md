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

The `metadata` directive can contain additional information and/or limits to describe station types.

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

Defined `timestep` for station type named `foreign`, and a default value.

The first available point in time for station types observations with `first_observation`.

## Oracle Table Definitions

For some reason, the station types are divided in two setting blocks, `oracle_stationtypelist` and `postgresql_stationtypelist`. The station types are listed here a second time, and are now defined as objects with special attributes.

The `stationGroups` is an array containing names matching things in the database.
The `producerIds` is another array containing numbers matching things in the database.
Somehow these two define what is being searched from the database. It seems that same `stationGroup` can point to multiple `producerIds` in the database, but this is definitely not a requirement.

In order to _add new weather stations available in smartmet-server_, add the stations' group name in the database to `stationGroups` array in the configuration file. But also add the identity of the weather station to `producresIds` array. 

```
oracle_stationtypelist:
{
        local:
        {
                useCommonQueryMethod = true;
                cached = true;
                stationGroups = ["ASDF", "EXTASDF"];
                producerIds = [1,13,17];
                databaseTableName = "my_database_table";
        };
};
```

## PostgreSQL Table Definitions

This may or may not contain `stationGroups` attribute `¯\_(ツ)_/¯`.

```
postgresql_stationtypelist:
{
        foreign:
        {
                cached = true;
                useCommonQueryMethod = true;
                producerId = 1;
        };
};
```

## More Oracle Things 

The station types need to be defined _a third time_ (Say what? -ed.)

Even more settings are needed for each station type. This `oracle_stationtype` is a thing.


```
oracle_stationtype:
{
        local = "-5";
};
```

## More PostgreSQL Things

Like the `oracle_stationtype`, the directive `postgresql_stationtype` is also a thing for defining station type settings a third time.

```
postgresql_stationtype:
{
        foreign = "1";
};
```

Now you should have something somewhere. Perhaps you still need to map the available observation phenomena in the database to station types.

