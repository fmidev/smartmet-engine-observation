#include "SpecialParameters.h"
#include <engines/geonames/Engine.h>
#include <locus/Query.h>
#include <macgyver/Astronomy.h>
#include <macgyver/CharsetTools.h>
#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>
#include <macgyver/ValueFormatter.h>
#include <timeseries/ParameterKeywords.h>
#include <timeseries/ParameterTools.h>

using namespace SmartMet;
using Engine::Observation::SpecialParameters;

SpecialParameters& SpecialParameters::mutable_instance()
{
  static SpecialParameters special_parameters;
  return special_parameters;
}

void SpecialParameters::setGeonames(Engine::Geonames::Engine* itsGeonames)
{
  mutable_instance().itsGeonames = itsGeonames;
}

TS::Value SpecialParameters::getValue(const std::string& param_name,
                                      const SpecialParameters::Args& args) const
{
  assert(itsGeonames);
  try
  {
    auto it = handler_map.find(param_name);
    if (it == handler_map.end() || !it->second)
    {
      std::string msg = "Unsupported special parameter '" + param_name + "'";
      Fmi::Exception exception(BCP, "Operation processing failed!");
      exception.addDetail(msg);
      throw exception;
    }

    return it->second(args);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

bool SpecialParameters::is_supported(const std::string& param_name) const
{
  return handler_map.count(param_name) > 0;
}

TS::TimedValue SpecialParameters::getTimedValue(const std::string& param_name,
                                                const Args& args) const
{
  try
  {
    TS::Value value = getValue(param_name, args);
    return {args.obstime, value};
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

TS::TimedValue SpecialParameters::getTimedValue(
    const Spine::Station& station,
    const std::string& stationType,
    const std::string& parameter,
    const Fmi::LocalDateTime& obstime,
    const Fmi::LocalDateTime& origintime,
    const std::string& timeZone,
    const Settings* settings) const
{
  try
  {
    const SpecialParameters::Args args(
        station, stationType, obstime, origintime, timeZone, settings);
    TS::Value value = getValue(parameter, args);
    return {obstime, value};
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

SpecialParameters::SpecialParameters()
    : tf(Fmi::TimeFormatter::create("iso")),
      utc_tz(Fmi::TimeZoneFactory::instance().time_zone_from_string("UTC"))
{
  handler_map[DIRECTION_PARAM] = [](const SpecialParameters::Args& d) -> TS::Value
  {
    TS::Value value = TS::None();
    if (d.station.stationDirection >= 0)
    {
      Fmi::ValueFormatterParam vfp;
      Fmi::ValueFormatter valueFormatter(vfp);
      value = valueFormatter.format(d.station.stationDirection, 1);
    }
    return value;
  };

  // FIXME: miksi Station::distance on std::string?
  handler_map[DISTANCE_PARAM] = [](const SpecialParameters::Args& d) -> TS::Value
  {
    TS::Value value = TS::None();
    if (!d.station.distance.empty())
    {
      Fmi::ValueFormatterParam vfp;
      Fmi::ValueFormatter valueFormatter(vfp);
      value = valueFormatter.format(Fmi::stod(d.station.distance), 1);
    }
    return d.station.distance;
  };

  handler_map[STATION_ELEVATION_PARAM] = [](const SpecialParameters::Args& d) -> TS::Value
  { return d.station.station_elevation; };

  handler_map[STATIONTYPE_PARAM] = [](const SpecialParameters::Args& d) -> TS::Value
  { return d.station.station_type; };


  handler_map[FMISID_PARAM] = [](const SpecialParameters::Args& d) -> TS::Value
  {
    TS::Value value = TS::None();
    if (d.station.fmisid > 0)
    {
      value = d.station.fmisid;
    }
    return value;
  };

  handler_map[LPNN_PARAM] = [](const SpecialParameters::Args& d) -> TS::Value
  {
    TS::Value value = TS::None();
    if (d.station.lpnn > 0)
    {
      value = d.station.lpnn;
    }
    return value;
  };

  handler_map[NAME_PARAM] = [this](const SpecialParameters::Args& d) -> TS::Value
  {
    auto loc = d.get_location(itsGeonames);
    if (loc)
      return loc->name;

    if (d.station.requestedName.length() > 0)
      return d.station.requestedName;

    return d.station.station_formal_name(d.settings->language);
  };

  // BEGIN: Things that should perhaps not be here

  handler_map[ISOTIME_PARAM] = [](const SpecialParameters::Args& d) -> TS::Value
  { return Fmi::to_iso_string(d.obstime.utc_time()); };

  handler_map[LOCALTIME_PARAM] = [](const SpecialParameters::Args& d) -> TS::Value
  {
    const Fmi::DateTime utc = d.obstime.utc_time();
    auto& tzf = Fmi::TimeZoneFactory::instance();
    Fmi::TimeZonePtr tz = tzf.time_zone_from_string(d.station.timezone);
    return Fmi::LocalDateTime(utc, tz);
  };

  handler_map[UTCTIME_PARAM] =
      handler_map[UTC_PARAM] = [this](const SpecialParameters::Args& d) -> TS::Value
  {
    Fmi::LocalDateTime utc(d.obstime.utc_time(), utc_tz);
    return utc;
  };

  handler_map[MODEL_PARAM] = [](const SpecialParameters::Args& d) -> TS::Value
  { return d.stationType; };

  // modtime is only for timeseries compatibility
  handler_map["modtime"] = [](const SpecialParameters::Args& /* d */) -> TS::Value { return ""; };

  handler_map[GEOID_PARAM] = [](const SpecialParameters::Args& d) -> TS::Value
  { return d.station.geoid; };

  handler_map[ELEVATION_PARAM] = handler_map[STATION_ELEVATION_PARAM] =
      [](const SpecialParameters::Args& d) -> TS::Value { return d.station.station_elevation; };

  // END: Things that should perhaps not be here.

  handler_map[ORIGINTIME_PARAM] = [](const SpecialParameters::Args& d) -> TS::Value
  { return d.origintime; };

  handler_map[PLACE_PARAM] = [](const SpecialParameters::Args& d) -> TS::Value
  { return d.station.tag; };

  handler_map[RWSID_PARAM] = [](const SpecialParameters::Args& d) -> TS::Value
  {
    TS::Value value = TS::None();
    if (d.station.rwsid > 0)
    {
      value = d.station.rwsid;
    }
    return value;
  };

  // FIXME: is this correct?
  handler_map[SENSOR_NO_PARAM] = [](const SpecialParameters::Args& /* d */) -> TS::Value
  { return 1; };

  // FIXME: Station::stationary on std::string. Pitäisikö olla bool vai int?
  handler_map[STATIONARY_PARAM] = [](const SpecialParameters::Args& d) -> TS::Value
  { return d.station.stationary; };

  handler_map[LATITUDE_PARAM] = handler_map[LAT_PARAM] =
      [](const SpecialParameters::Args& d) -> TS::Value { return d.station.latitude_out; };

  handler_map[LONGITUDE_PARAM] = handler_map[LON_PARAM] =
      [](const SpecialParameters::Args& d) -> TS::Value { return d.station.longitude_out; };

  handler_map[STATIONLATITUDE_PARAM] = handler_map[STATIONLAT_PARAM] =
      [](const SpecialParameters::Args& d) -> TS::Value { return d.station.latitude_out; };

  handler_map[STATIONLONGITUDE_PARAM] = handler_map[STATIONLON_PARAM] =
      [](const SpecialParameters::Args& d) -> TS::Value { return d.station.longitude_out; };

  handler_map[STATIONNAME_PARAM] =
      handler_map[STATION_NAME_PARAM] = [](const SpecialParameters::Args& d) -> TS::Value
  { return d.station.station_formal_name(d.settings->language); };

  handler_map[TZ_PARAM] = [](const SpecialParameters::Args& d) -> TS::Value
  {
    if (d.timeZone == "localtime")
      return d.station.timezone;

    return d.timeZone;
  };

  handler_map[WMO_PARAM] = [](const SpecialParameters::Args& d) -> TS::Value
  {
    TS::Value value = TS::None();
    if (d.station.wmo > 0)
      value = d.station.wmo;
    return value;
  };

  // FIXME: implement: requires initial coordinate system
  handler_map[X_PARAM] = parameter_handler_t();

  // FIXME: implement: requires initial coordinate system
  handler_map[Y_PARAM] = parameter_handler_t();
}

Spine::LocationPtr SpecialParameters::Args::get_location(Geonames::Engine* itsGeonames) const
{
  Spine::LocationPtr ptr;
  if (!location_ptr && settings && itsGeonames)
  {
    Locus::QueryOptions opts;
    opts.SetLanguage(settings->language);
    opts.SetResultLimit(1);
    opts.SetCountries("");
    opts.SetSearchVariants(true);
    auto places = itsGeonames->idSearch(opts, station.geoid);
    if (!places.empty())
    {
      ptr = *places.begin();
    }
    location_ptr = ptr;
  }
  else if (location_ptr)
  {
    ptr = *location_ptr;
  }
  return ptr;
}
