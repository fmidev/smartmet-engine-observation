#include "Keywords.h"
#include "Utils.h"
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/xml_iarchive.hpp>
#include <boost/date_time/posix_time/time_serialize.hpp>
#include <boost/filesystem.hpp>
#include <boost/regex.hpp>
#include <boost/serialization/vector.hpp>
#include <macgyver/Astronomy.h>
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
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

/** *\brief Return true of a parameter looks to be normal enough to be an observation
 */

bool not_special(const Spine::Parameter& theParam)
{
  try
  {
    switch (theParam.type())
    {
      case Spine::Parameter::Type::Data:
      case Spine::Parameter::Type::Landscaped:
        return true;
      case Spine::Parameter::Type::DataDerived:
      case Spine::Parameter::Type::DataIndependent:
        return false;
    }
    // NOT REACHED
    return false;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
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
    throw Spine::Exception::Trace(BCP, "Operation failed!");
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
    std::string ret = parameterMap.getParameter(p, stationType);
    if (!ret.empty())
      return ret;
    else
      return p;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// Calculates station direction in degrees from given coordinates
void calculateStationDirection(Spine::Station& station)
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
    throw Spine::Exception::Trace(BCP, "Operation failed!");
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

    if (boost::regex_match(sensorNumber, boost::regex("^[0-9]+$")))
      return name.substr(0, startpos);

    return name;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
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
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

Spine::Stations removeDuplicateStations(Spine::Stations& stations)
{
  try
  {
    std::vector<int> ids;
    Spine::Stations noDuplicates;
    for (const Spine::Station& s : stations)
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
    throw Spine::Exception::Trace(BCP, "Operation failed!");
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
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

void logMessage(const std::string& message, bool quiet)
{
  try
  {
    if (!quiet)
      std::cout << Spine::log_time_str() << ' ' << message << std::endl;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
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
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

boost::optional<int> calcSmartsymbolNumber(int wawa,
                                           int cloudiness,
                                           double temperature,
                                           const boost::local_time::local_date_time& ldt,
                                           double lat,
                                           double lon)
{
  boost::optional<int> smartsymbol = {};

  const int wawa_group1[] = {0, 4, 5, 10, 20, 21, 22, 23, 24, 25};
  const int wawa_group2[] = {30, 31, 32, 33, 34};

  const int cloudiness_limit1 = 0;
  const int cloudiness_limit2 = 1;
  const int cloudiness_limit3 = 5;
  const int cloudiness_limit4 = 7;
  const int cloudiness_limit5 = 9;

  if (std::find(std::begin(wawa_group1), std::end(wawa_group1), wawa) != std::end(wawa_group1))
  {
    if (cloudiness <= cloudiness_limit1)
      smartsymbol = 1;
    else if (cloudiness <= cloudiness_limit2)
      smartsymbol = 2;
    else if (cloudiness <= cloudiness_limit3)
      smartsymbol = 4;
    else if (cloudiness <= cloudiness_limit4)
      smartsymbol = 6;
    else if (cloudiness <= cloudiness_limit5)
      smartsymbol = 7;
  }
  else if (std::find(std::begin(wawa_group2), std::end(wawa_group2), wawa) != std::end(wawa_group2))
  {
    if (cloudiness <= cloudiness_limit1)
      smartsymbol = 1;
    else if (cloudiness <= cloudiness_limit2)
      smartsymbol = 2;
    else if (cloudiness <= cloudiness_limit3)
      smartsymbol = 4;
    else if (cloudiness <= cloudiness_limit4)
      smartsymbol = 6;
    else if (cloudiness <= cloudiness_limit5)
      smartsymbol = 9;
  }

  else if (wawa == 40 || wawa == 41)
  {
    if (temperature <= 0)
    {
      if (cloudiness <= 5)
        smartsymbol = 51;
      else if (cloudiness <= 7)
        smartsymbol = 54;
      else if (cloudiness <= 9)
        smartsymbol = 57;
    }
    else
    {
      if (cloudiness <= 5)
        smartsymbol = 31;
      else if (cloudiness <= 7)
        smartsymbol = 34;
      else if (cloudiness <= 9)
        smartsymbol = 37;
    }
  }
  else if (wawa == 42)
  {
    if (temperature <= 0)
    {
      if (cloudiness <= 5)
        smartsymbol = 53;
      else if (cloudiness <= 7)
        smartsymbol = 56;
      else if (cloudiness <= 9)
        smartsymbol = 59;
    }
    else
    {
      if (cloudiness <= 5)
        smartsymbol = 33;
      else if (cloudiness <= 7)
        smartsymbol = 36;
      else if (cloudiness <= 9)
        smartsymbol = 39;
    }
  }
  else if (wawa >= 50 && wawa <= 53)
  {
    if (cloudiness <= 9)
      smartsymbol = 11;
  }
  else if (wawa >= 54 && wawa <= 56)
  {
    if (cloudiness <= 9)
      smartsymbol = 14;
  }
  else if (wawa == 60)
  {
    if (cloudiness <= 5)
      smartsymbol = 31;
    else if (cloudiness <= 7)
      smartsymbol = 34;
    else if (cloudiness <= 9)
      smartsymbol = 37;
  }
  else if (wawa == 61)
  {
    if (cloudiness <= 5)
      smartsymbol = 31;
    else if (cloudiness <= 7)
      smartsymbol = 34;
    else if (cloudiness <= 9)
      smartsymbol = 37;
  }
  else if (wawa == 62)
  {
    if (cloudiness <= 5)
      smartsymbol = 32;
    else if (cloudiness <= 7)
      smartsymbol = 35;
    else if (cloudiness <= 9)
      smartsymbol = 38;
  }
  else if (wawa == 63)
  {
    if (cloudiness <= 5)
      smartsymbol = 33;
    else if (cloudiness <= 7)
      smartsymbol = 36;
    else if (cloudiness <= 9)
      smartsymbol = 39;
  }
  else if (wawa >= 64 && wawa <= 66)
  {
    if (cloudiness <= 9)
      smartsymbol = 17;
  }
  else if (wawa == 67)
  {
    if (cloudiness <= 5)
      smartsymbol = 41;
    else if (cloudiness <= 7)
      smartsymbol = 44;
    else if (cloudiness <= 9)
      smartsymbol = 47;
  }
  else if (wawa == 68)
  {
    if (cloudiness <= 5)
      smartsymbol = 42;
    else if (cloudiness <= 7)
      smartsymbol = 45;
    else if (cloudiness <= 9)
      smartsymbol = 48;
  }
  else if (wawa == 70)
  {
    if (cloudiness <= 5)
      smartsymbol = 51;
    else if (cloudiness <= 7)
      smartsymbol = 54;
    else if (cloudiness <= 9)
      smartsymbol = 57;
  }
  else if (wawa == 71)
  {
    if (cloudiness <= 5)
      smartsymbol = 51;
    else if (cloudiness <= 7)
      smartsymbol = 54;
    else if (cloudiness <= 9)
      smartsymbol = 57;
  }
  else if (wawa == 72)
  {
    if (cloudiness <= 5)
      smartsymbol = 52;
    else if (cloudiness <= 7)
      smartsymbol = 55;
    else if (cloudiness <= 9)
      smartsymbol = 58;
  }
  else if (wawa == 73)
  {
    if (cloudiness <= 5)
      smartsymbol = 53;
    else if (cloudiness <= 7)
      smartsymbol = 56;
    else if (cloudiness <= 9)
      smartsymbol = 59;
  }
  else if (wawa == 74)
  {
    if (cloudiness <= 5)
      smartsymbol = 51;
    else if (cloudiness <= 7)
      smartsymbol = 54;
    else if (cloudiness <= 9)
      smartsymbol = 57;
  }
  else if (wawa == 75)
  {
    if (cloudiness <= 5)
      smartsymbol = 52;
    else if (cloudiness <= 7)
      smartsymbol = 55;
    else if (cloudiness <= 9)
      smartsymbol = 58;
  }
  else if (wawa == 76)
  {
    if (cloudiness <= 5)
      smartsymbol = 53;
    else if (cloudiness <= 7)
      smartsymbol = 56;
    else if (cloudiness <= 9)
      smartsymbol = 59;
  }
  else if (wawa == 77)
  {
    if (cloudiness <= 9)
      smartsymbol = 57;
  }
  else if (wawa == 78)
  {
    if (cloudiness <= 9)
      smartsymbol = 57;
  }
  else if (wawa == 80)
  {
    if (temperature <= 0)
    {
      if (cloudiness <= 5)
        smartsymbol = 51;
      else if (cloudiness <= 7)
        smartsymbol = 54;
      else if (cloudiness <= 9)
        smartsymbol = 57;
    }
    else
    {
      if (cloudiness <= 5)
        smartsymbol = 21;
      else if (cloudiness <= 7)
        smartsymbol = 24;
      else if (cloudiness <= 9)
        smartsymbol = 27;
    }
  }
  else if (wawa >= 81 && wawa <= 84)
  {
    if (cloudiness <= 5)
      smartsymbol = 21;
    else if (cloudiness <= 7)
      smartsymbol = 24;
    else if (cloudiness <= 9)
      smartsymbol = 27;
  }
  else if (wawa == 85)
  {
    if (cloudiness <= 5)
      smartsymbol = 51;
    else if (cloudiness <= 7)
      smartsymbol = 54;
    else if (cloudiness <= 9)
      smartsymbol = 57;
  }
  else if (wawa == 86)
  {
    if (cloudiness <= 5)
      smartsymbol = 52;
    else if (cloudiness <= 7)
      smartsymbol = 55;
    else if (cloudiness <= 9)
      smartsymbol = 58;
  }
  else if (wawa == 87)
  {
    if (cloudiness <= 5)
      smartsymbol = 53;
    else if (cloudiness <= 7)
      smartsymbol = 56;
    else if (cloudiness <= 9)
      smartsymbol = 59;
  }
  else if (wawa == 89)
  {
    if (cloudiness <= 5)
      smartsymbol = 61;
    else if (cloudiness <= 7)
      smartsymbol = 64;
    else if (cloudiness <= 9)
      smartsymbol = 67;
  }

  // Add day/night information
  Fmi::Astronomy::solar_position_t sp = Fmi::Astronomy::solar_position(ldt, lon, lat);
  if (smartsymbol)
  {
    if (sp.dark())
      return 100 + *smartsymbol;
    return *smartsymbol;
  }

  // No valid combination found, return empty value
  return {};
}

bool is_time_parameter(std::string paramname)
{
  Fmi::ascii_tolower(paramname);
  try
  {
    return (
        paramname == TIME_PARAM || paramname == ISOTIME_PARAM || paramname == XMLTIME_PARAM ||
        paramname == ORIGINTIME_PARAM || paramname == LOCALTIME_PARAM ||
        paramname == UTCTIME_PARAM || paramname == EPOCHTIME_PARAM ||
        paramname == SUNELEVATION_PARAM || paramname == SUNDECLINATION_PARAM ||
        paramname == SUNAZIMUTH_PARAM || paramname == DARK_PARAM || paramname == MOONPHASE_PARAM ||
        paramname == MOONRISE_PARAM || paramname == MOONRISE2_PARAM || paramname == MOONSET_PARAM ||
        paramname == MOONSET2_PARAM || paramname == MOONRISETODAY_PARAM ||
        paramname == MOONRISE2TODAY_PARAM || paramname == MOONSETTODAY_PARAM ||
        paramname == MOONSET2TODAY_PARAM || paramname == MOONUP24H_PARAM ||
        paramname == MOONDOWN24H_PARAM || paramname == SUNRISE_PARAM || paramname == SUNSET_PARAM ||
        paramname == NOON_PARAM || paramname == SUNRISETODAY_PARAM ||
        paramname == SUNSETTODAY_PARAM || paramname == DAYLENGTH_PARAM ||
        paramname == TIMESTRING_PARAM || paramname == WDAY_PARAM || paramname == WEEKDAY_PARAM ||
        paramname == MON_PARAM || paramname == MONTH_PARAM || paramname == HOUR_PARAM ||
        paramname == TZ_PARAM || paramname == ORIGINTIME_PARAM ||
        (paramname.substr(0, 5) == "date(" && paramname[paramname.size() - 1] == ')'));
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}


}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
