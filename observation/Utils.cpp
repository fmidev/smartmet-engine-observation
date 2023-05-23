#include "Utils.h"
#include "Keywords.h"
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/xml_iarchive.hpp>
#include <boost/date_time/posix_time/time_serialize.hpp>
#include <boost/filesystem.hpp>
#include <boost/serialization/vector.hpp>
#include <macgyver/Astronomy.h>
#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>
#include <macgyver/TypeName.h>
#include <spine/Convenience.h>
#include <fstream>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
namespace Utils
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
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
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
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

std::string trimCommasFromEnd(const std::string& what)
{
  try
  {
    size_t end = what.find_last_not_of(',');
    return what.substr(0, end + 1);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
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
    return p;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// Calculates station direction in degrees from given coordinates
void calculateStationDirection(Spine::Station& station)
{
  try
  {
    double lon1 = deg2rad(station.requestedLon);
    double lat1 = deg2rad(station.requestedLat);
    double lon2 = deg2rad(station.longitude_out);
    double lat2 = deg2rad(station.latitude_out);

    double dlon = lon2 - lon1;

    double direction = rad2deg(
        atan2(sin(dlon) * cos(lat2), cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(dlon)));

    if (direction < 0)
      direction += 360.0;

    station.stationDirection = std::round(10.0 * direction) / 10.0;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
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

std::string windCompass8(double direction, const std::string& missingValue)
{
  if (direction < 0)
    return missingValue;
  std::array<std::string, 8> names{"N", "NE", "E", "SE", "S", "SW", "W", "NW"};

  int i = static_cast<int>((direction + 22.5) / 45) % 8;
  return names[i];
}

std::string windCompass16(double direction, const std::string& missingValue)
{
  if (direction < 0)
    return missingValue;
  std::array<std::string, 16> names{"N",
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

std::string windCompass32(double direction, const std::string& missingValue)
{
  if (direction < 0)
    return missingValue;
  std::array<std::string, 32> names{"N", "NbE", "NNE", "NEbN", "NE", "NEbE", "ENE", "EbN",
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
    if (name.find_last_of('_') == std::string::npos)
      return name;

    size_t startpos = name.find_last_of('_');
    size_t endpos = name.length();

    int length = boost::numeric_cast<int>(endpos - startpos - 1);

    // Test appearance of the parameter between TRS_10MIN_DIF and TRS_10MIN_DIF_1
    std::string sensorNumber = name.substr(startpos + 1, length);

    if (Fmi::looks_unsigned_int(sensorNumber))
      return name.substr(0, startpos);

    return name;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

/*
 * The sensor number is given after an underscore, for example KELI_1
 */

int parseSensorNumber(const std::string& parameter)
{
  try
  {
    int defaultSensorNumber = 1;
    std::string sensorNumber;
    std::size_t startpos = parameter.find_last_of('_');
    std::size_t endpos = parameter.length();
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
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
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
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

boost::posix_time::ptime utc_second_clock()
{
  auto now = boost::posix_time::second_clock::universal_time();
  return {now.date(), boost::posix_time::seconds(now.time_of_day().total_seconds())};
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
  return {t.date(), boost::posix_time::hours(0)};
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

void logMessage(const std::string& message, bool quiet)
{
  try
  {
    if (!quiet)
      std::cout << Spine::log_time_str() << ' ' << message << std::endl;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
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

  const std::array<int, 10> wawa_group1{0, 4, 5, 10, 20, 21, 22, 23, 24, 25};
  const std::array<int, 5> wawa_group2{30, 31, 32, 33, 34};

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
  else if (wawa == 60 || wawa == 61)
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
  else if (wawa == 70 || wawa == 71 || wawa == 74)
  {
    if (cloudiness <= 5)
      smartsymbol = 51;
    else if (cloudiness <= 7)
      smartsymbol = 54;
    else if (cloudiness <= 9)
      smartsymbol = 57;
  }
  else if (wawa == 72 || wawa == 75 || wawa == 86)
  {
    if (cloudiness <= 5)
      smartsymbol = 52;
    else if (cloudiness <= 7)
      smartsymbol = 55;
    else if (cloudiness <= 9)
      smartsymbol = 58;
  }
  else if (wawa == 73 || wawa == 76 || wawa == 87)
  {
    if (cloudiness <= 5)
      smartsymbol = 53;
    else if (cloudiness <= 7)
      smartsymbol = 56;
    else if (cloudiness <= 9)
      smartsymbol = 59;
  }
  else if (wawa == 77 || wawa == 78)
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

TS::TimeSeriesVectorPtr initializeResultVector(const Settings& settings)
{
  TS::TimeSeriesVectorPtr ret = boost::make_shared<TS::TimeSeriesVector>();

  // Set timeseries objects for each requested parameter
  for (unsigned int i = 0; i < settings.parameters.size(); i++)
    ret->emplace_back(TS::TimeSeries(settings.localTimePool));

  return ret;
}

boost::posix_time::ptime epoch2ptime(double epoch)
{
  boost::posix_time::ptime ret =
      boost::posix_time::from_time_t(static_cast<std::time_t>(floor(epoch)));
  ret += boost::posix_time::microseconds(static_cast<long>((epoch - floor(epoch)) * 1000000));

  return ret;
}

std::string getStringValue(const TS::Value& tv)
{
  // For some reason different databases/drivers don't simply use int for FMISID.
  // This is workaround code, FMISID should always be int.

  if (const double* dvalue = boost::get<double>(&tv))
    return Fmi::to_string(*dvalue);

  if (const int* ivalue = boost::get<int>(&tv))
    return Fmi::to_string(*ivalue);

  if (const std::string* svalue = boost::get<std::string>(&tv))
    return *svalue;

  // These are just for getting more informative error messages:

  if (boost::get<TS::None>(&tv))
  {
    throw Fmi::Exception(BCP, "Encountered NULL FMISID");
  }

  if (boost::get<TS::LonLat>(&tv))
  {
    throw Fmi::Exception(BCP, "Encountered LonLat FMISID");
  }

  if (boost::get<boost::local_time::local_date_time>(&tv))
  {
    throw Fmi::Exception(BCP, "Encountered date FMISID");
  }

  // All should be handled above, but if someone extends the variant, this handles it too:

  throw Fmi::Exception(BCP, "Failed to extract FMISID (double/int/string) from variant");
}

bool isParameter(const std::string& name,
                 const std::string& stationType,
                 const ParameterMap& parameterMap)
{
  try
  {
    std::string parameterName = Fmi::ascii_tolower_copy(name);
    removePrefix(parameterName, "qc_");

    if (boost::algorithm::ends_with(parameterName, DATA_SOURCE))
      return true;

    // Is the alias configured.
    const auto namePtr = parameterMap.find(parameterName);

    if (namePtr == parameterMap.end())
      return false;

    // Is the stationType configured inside configuration block of the alias.
    std::string stationTypeLowerCase = Fmi::ascii_tolower_copy(stationType);
    auto stationTypeMapPtr = namePtr->second.find(stationTypeLowerCase);

    if (stationTypeMapPtr == namePtr->second.end())
      stationTypeMapPtr = namePtr->second.find(DEFAULT_STATIONTYPE);
    if (stationTypeMapPtr == namePtr->second.end())
      return false;

    return true;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

bool isParameterVariant(const std::string& name, const ParameterMap& parameterMap)
{
  try
  {
    auto parameterLowerCase = Fmi::ascii_tolower_copy(name);
    removePrefix(parameterLowerCase, "qc_");
    // Is the alias configured.
    const auto namePtr = parameterMap.find(parameterLowerCase);

    if (namePtr == parameterMap.end())
      return false;

    return true;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace Utils
}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
