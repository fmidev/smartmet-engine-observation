Observation parameters define what can be read from the database.

## List All Parameters

In `parameters` directive a list of ALL possible observation parameter names that can be requested from smartmet-server's interface. The names are case insensitive, must be unique, and must not begin with `qc_`, which is reserved for requesting the quality codes of the parameters.

```
parameters:
[
        "Temperature",
        "Pressure",
        "some_silly_thing"
];
```

## Define All Parameters (Again But Better)

All of the names previously defined in `parameters` array have to be defined _a second time_. This time each of them is mapped to a database. Each definition is it's own object and the name must match the item in the array.

The attributes are the same as the names in `stationtypes` array, and their values tell how the parameter is found in the database for that station type: a measurand id (a number) for the station types reading the generic observation tables and for the mobile and external station types, otherwise the name of the parameter in the table of the station type.

In the example a parameter named `Temperature` can be used in a timeseries request. When the `producer` has the value `local` it will search a parameter named `123` from the database. If the `producer` is named `foreign` it will search a parameter names `temperatureparam` in the database.

```
Temperature:
{
        local = "123";
        foreign = "temperatureparam";
};

Pressure:
{
        local = "456";
        foreign = "super_pressure";
};

some_silly_thing:
{
        local = "789";
        foreign = "RadiationXYZ";
};
```
