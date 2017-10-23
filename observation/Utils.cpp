#include "Utils.h"
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/xml_iarchive.hpp>
#include <boost/date_time/posix_time/time_serialize.hpp>
#include <boost/filesystem.hpp>
#include <boost/serialization/vector.hpp>
#include <macgyver/TypeName.h>
#include <spine/Convenience.h>
#include <spine/Exception.h>
#include <fstream>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
bool removePrefix(std::string& parameter, const std::string& prefix)
{
  try
  {
    std::size_t prefixLen = prefix.length();
    if ((parameter.length() > prefixLen) && parameter.compare(0, prefixLen, prefix) == 0)
    {
      parameter.erase(0, prefixLen);
      return true;
    }

    return false;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

/** *\brief Return true of a parameter looks to be normal enough to be an observation
 */

bool not_special(const SmartMet::Spine::Parameter& theParam)
{
  try
  {
    switch (theParam.type())
    {
      case SmartMet::Spine::Parameter::Type::Data:
      case SmartMet::Spine::Parameter::Type::Landscaped:
        return true;
      case SmartMet::Spine::Parameter::Type::DataDerived:
      case SmartMet::Spine::Parameter::Type::DataIndependent:
        return false;
    }
    // NOT REACHED
    return false;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

std::string trimCommasFromEnd(const std::string& what)
{
  try
  {
    size_t end = what.find_last_not_of(",");
    return what.substr(0, end + 1);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

/* Translates parameter names to match the parameter name in the database.
 * If the name is not found in parameter map, return the given name.
 */

std::string translateParameter(const std::string& paramname,
                               const std::string& stationType,
                               ParameterMap& parameterMap)
{
  try
  {
    // All parameters are in lower case in parametermap
    std::string p = Fmi::ascii_tolower_copy(paramname);
    if (!parameterMap[p][stationType].empty())
      return parameterMap[p][stationType];
    else
      return p;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

// Calculates station direction in degrees from given coordinates
void calculateStationDirection(SmartMet::Spine::Station& station)
{
  try
  {
    double direction;
    double lon1 = deg2rad(station.requestedLon);
    double lat1 = deg2rad(station.requestedLat);
    double lon2 = deg2rad(station.longitude_out);
    double lat2 = deg2rad(station.latitude_out);

    double dlon = lon2 - lon1;

    direction = rad2deg(
        atan2(sin(dlon) * cos(lat2), cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(dlon)));

    if (direction < 0)
    {
      direction += 360.0;
    }

    station.stationDirection = std::round(10.0 * direction) / 10.0;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

// Utility method to convert degrees to radians
double deg2rad(double deg)
{
  return (deg * PI / 180);
}

// Utility method to convert radians to degrees
double rad2deg(double rad)
{
  return (rad * 180 / PI);
}

std::string windCompass8(double direction)
{
  static const std::string names[] = {"N", "NE", "E", "SE", "S", "SW", "W", "NW"};

  int i = static_cast<int>((direction + 22.5) / 45) % 8;
  return names[i];
}

std::string windCompass16(double direction)
{
  static const std::string names[] = {"N",
                                      "NNE",
                                      "NE",
                                      "ENE",
                                      "E",
                                      "ESE",
                                      "SE",
                                      "SSE",
                                      "S",
                                      "SSW",
                                      "SW",
                                      "WSW",
                                      "W",
                                      "WNW",
                                      "NW",
                                      "NNW"};

  int i = static_cast<int>((direction + 11.25) / 22.5) % 16;
  return names[i];
}

std::string windCompass32(double direction)
{
  static const std::string names[] = {"N", "NbE", "NNE", "NEbN", "NE", "NEbE", "ENE", "EbN",
                                      "E", "EbS", "ESE", "SEbE", "SE", "SEbS", "SSE", "SbE",
                                      "S", "SbW", "SSW", "SWbS", "SW", "SWbW", "WSW", "WbS",
                                      "W", "WbN", "WNW", "NWbW", "NW", "NWbN", "NNW", "NbW"};

  int i = static_cast<int>((direction + 5.625) / 11.25) % 32;
  return names[i];
}

std::string parseParameterName(const std::string& parameter)
{
  try
  {
    std::string name = Fmi::ascii_tolower_copy(parameter);

    removePrefix(name, "qc_");

    // No understrike
    if (name.find_last_of("_") == std::string::npos)
      return name;

    size_t startpos = name.find_last_of("_");
    size_t endpos = name.length();

    int length = boost::numeric_cast<int>(endpos - startpos - 1);

    // Test appearance of the parameter between TRS_10MIN_DIF and TRS_10MIN_DIF_1
    std::string sensorNumber = name.substr(startpos + 1, length);
    try
    {
      Fmi::stoi(sensorNumber);
    }
    catch (...)
    {
      return name;
    }

    return name.substr(0, startpos);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

/*
 * The sensor number is given after an underscore, for example KELI_1
 */

int parseSensorNumber(const std::string& parameter)
{
  try
  {
    size_t startpos, endpos;
    int defaultSensorNumber = 1;
    std::string sensorNumber;
    startpos = parameter.find_last_of("_");
    endpos = parameter.length();
    int length = boost::numeric_cast<int>(endpos - startpos - 1);

    // If sensor number is given, for example KELI_1, return requested number
    if (startpos != std::string::npos && endpos != std::string::npos)
    {
      sensorNumber = parameter.substr(startpos + 1, length);
      try
      {
        return Fmi::stoi(sensorNumber);
      }
      catch (...)
      {
        return defaultSensorNumber;
      }
    }

    return defaultSensorNumber;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

SmartMet::Spine::Stations removeDuplicateStations(SmartMet::Spine::Stations& stations)
{
  try
  {
    std::vector<int> ids;
    SmartMet::Spine::Stations noDuplicates;
    for (const SmartMet::Spine::Station& s : stations)
    {
      if (std::find(ids.begin(), ids.end(), s.station_id) == ids.end())
      {
        noDuplicates.push_back(s);
        // BUG? Why is station_id double?
        ids.push_back(boost::numeric_cast<int>(s.station_id));
      }
    }
    return noDuplicates;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Round down the given time to start of day
 */
// ----------------------------------------------------------------------

boost::posix_time::ptime day_start(const boost::posix_time::ptime& t)
{
  if (t.is_not_a_date_time() || t.is_special())
    return t;
  return boost::posix_time::ptime(t.date(), boost::posix_time::hours(0));
}

// ----------------------------------------------------------------------
/*!
 * \brief Round up the given time to end of day
 */
// ----------------------------------------------------------------------

boost::posix_time::ptime day_end(const boost::posix_time::ptime& t)
{
  if (t.is_not_a_date_time() || t.is_special())
    return t;
  auto tmp = boost::posix_time::ptime(t.date(), boost::posix_time::hours(0));
  tmp += boost::gregorian::days(1);
  return tmp;
}

std::string timeToString(const boost::posix_time::ptime& time)
{
  try
  {
    char timestamp[100];
    sprintf(timestamp,
            "%d%02d%02d%02d%02d",
            static_cast<unsigned int>(time.date().year()),
            static_cast<unsigned int>(time.date().month()),
            static_cast<unsigned int>(time.date().day()),
            static_cast<unsigned int>(time.time_of_day().hours()),
            static_cast<unsigned int>(time.time_of_day().minutes()));

    return timestamp;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void logMessage(const std::string& message, bool quiet)
{
  try
  {
    if (!quiet)
      std::cout << SmartMet::Spine::log_time_str() << ' ' << message << std::endl;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

std::string getLocationCacheKey(int geoID,
                                int numberOfStations,
                                std::string stationType,
                                int maxDistance,
                                const boost::posix_time::ptime& starttime,
                                const boost::posix_time::ptime& endtime)
{
  try
  {
    std::string locationCacheKey = "";

    locationCacheKey += Fmi::to_string(geoID);
    locationCacheKey += "-";
    locationCacheKey += Fmi::to_string(numberOfStations);
    locationCacheKey += "-";
    locationCacheKey += stationType;
    locationCacheKey += "-";
    locationCacheKey += Fmi::to_string(maxDistance);
    locationCacheKey += "-";
    locationCacheKey += Fmi::to_iso_string(starttime);
    locationCacheKey += "-";
    locationCacheKey += Fmi::to_iso_string(endtime);
    return locationCacheKey;
  }
  catch (...)
  {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
