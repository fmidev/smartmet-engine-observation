#include "SpecialParameters.h"
#include <macgyver/StringConversion.h>
#include <macgyver/Exception.h>
#include <spine/ValueFormatter.h>
#include <spine/ParameterTools.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{

Spine::TimeSeries::TimedValue getSpecialParameterValue(
    const Spine::Station &station,
    const std::string &stationType,
    const std::string &parameter,
    const boost::local_time::local_date_time &obstime,
    const boost::local_time::local_date_time &origintime,
	const std::string& timeZone)
{
  try
  {
	Spine::TimeSeries::Value value = Spine::TimeSeries::None();

    if (parameter == "name")
    {
      if (station.requestedName.length() > 0)
        value = station.requestedName;
      else
        value = station.station_formal_name;
    }
    else if (parameter == "geoid")
      value = station.geoid;

    else if (parameter == "stationname" || parameter == "station_name")
      value = station.station_formal_name;

    else if (parameter == "distance")
    {
      if (!station.distance.empty())
		{
		  Spine::ValueFormatterParam vfp;
		  Spine::ValueFormatter valueFormatter(vfp);
		  value = valueFormatter.format(Fmi::stod(station.distance), 1);
		}
    }
    else if (parameter == "stationary")
      value = station.stationary;

    else if (parameter == "stationlongitude" || parameter == "stationlon")
      value = station.longitude_out;

    else if (parameter == "stationlatitude" || parameter == "stationlat")
      value = station.latitude_out;

    else if (parameter == "longitude" || parameter == "lon")
      value = station.requestedLon;

    else if (parameter == "latitude" || parameter == "lat")
      value = station.requestedLat;

    else if (parameter == "elevation" || parameter == "station_elevation")
      value = station.station_elevation;

    else if (parameter == "wmo")
    {
      if (station.wmo > 0)
        value = station.wmo;
    }
    else if (parameter == "lpnn")
	  {
      if (station.lpnn > 0)
		value = station.lpnn;
	  }
    else if (parameter == "fmisid")
	  {
		if(station.fmisid > 0)
		  value = station.fmisid;
	  }

    else if (parameter == "rwsid")
	  {
		if(station.rwsid > 0)
		  value = station.rwsid;
	  }

    // modtime is only for timeseries compatibility
    else if (parameter == "modtime")
      value = "";

    else if (parameter == "model")
      value = stationType;

    else if (parameter == "localtime")
      value = obstime;

    // origintime is always the current time
    else if (parameter == "origintime")
      value = origintime;

    else if (parameter == "timestring")
      value = "";

    else if (parameter == "tz")
    {
      if (timeZone == "localtime")
        value = station.timezone;
      else
        value = timeZone;
    }
    else if (parameter == "region")
      value = station.region;

    else if (parameter == "iso2")
      value = station.iso2;

    else if (parameter == "direction")
    {
      if (station.stationDirection >= 0)
		{
		  Spine::ValueFormatterParam vfp;
		  Spine::ValueFormatter valueFormatter(vfp);
		  value = valueFormatter.format(station.stationDirection, 1);
		}
    }
    else if (parameter == "country")
      value = station.country;

    else if (parameter == "place")
    {
      value = station.tag;
    }
    else if (parameter == "sensor_no")
      value = 1;
    else if (Spine::is_time_parameter(parameter))
      value = obstime;
    else
    {
      std::string msg = "Unsupported special parameter '" + parameter + "'";
      Fmi::Exception exception(BCP, "Operation processing failed!");
      exception.addDetail(msg);
      throw exception;
    }

    return Spine::TimeSeries::TimedValue(obstime, value);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
