#include "Utils.h"
#include "Keywords.h"
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/xml_iarchive.hpp>
#include <boost/serialization/vector.hpp>
#include <macgyver/Astronomy.h>
#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>
#include <macgyver/TypeName.h>
#include <spine/Convenience.h>
#include <filesystem>
#include <fstream>
#include <unordered_set>

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
                               const ParameterMap& parameterMap)
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
    double lon2 = deg2rad(station.longitude);
    double lat2 = deg2rad(station.latitude);

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
  const std::array<std::string, 8> names{"N", "NE", "E", "SE", "S", "SW", "W", "NW"};

  int i = static_cast<int>((direction + 22.5) / 45) % 8;
  return names[i];
}

std::string windCompass16(double direction, const std::string& missingValue)
{
  if (direction < 0)
    return missingValue;
  const std::array<std::string, 16> names{"N",
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
  const std::array<std::string, 32> names{"N", "NbE", "NNE", "NEbN", "NE", "NEbE", "ENE", "EbN",
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
    std::size_t startpos = parameter.find_last_of('_');
    std::size_t endpos = parameter.length();
    int length = boost::numeric_cast<int>(endpos - startpos - 1);

    // If sensor number is given, for example KELI_1, return requested number
    if (startpos != std::string::npos && endpos != std::string::npos)
    {
      auto sensorNumber = parameter.substr(startpos + 1, length);
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

Spine::Stations removeDuplicateStations(const Spine::Stations& stations)
{
  try
  {
    std::unordered_set<int> used_ids;

    Spine::Stations noDuplicates;
    for (const Spine::Station& s : stations)
    {
      if (used_ids.find(s.fmisid) == used_ids.end())
      {
        noDuplicates.push_back(s);
        used_ids.insert(s.fmisid);
      }
    }
    return noDuplicates;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

Fmi::DateTime utc_second_clock()
{
  auto now = Fmi::SecondClock::universal_time();
  return {now.date(), Fmi::Seconds(now.time_of_day().total_seconds())};
}

// ----------------------------------------------------------------------
/*!
 * \brief Round down the given time to start of day
 */
// ----------------------------------------------------------------------

Fmi::DateTime day_start(const Fmi::DateTime& t)
{
  if (t.is_not_a_date_time() || t.is_special())
    return t;
  return {t.date(), Fmi::Hours(0)};
}

// ----------------------------------------------------------------------
/*!
 * \brief Round up the given time to end of day
 */
// ----------------------------------------------------------------------

Fmi::DateTime day_end(const Fmi::DateTime& t)
{
  if (t.is_not_a_date_time() || t.is_special())
    return t;
  auto tmp = Fmi::DateTime(t.date(), Fmi::Hours(0));
  tmp += Fmi::date_time::Days(1);
  return tmp;
}

void logMessage(const std::string& message, bool quiet)
{
  try
  {
    if (!quiet)
      std::cout << Spine::log_time_str() << ' ' << message << '\n';
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

namespace
{

// ── calcSmartsymbolNumber helpers ─────────────────────────────────────────────

// Cloudiness → symbol on 3-level scale {base, base+3, base+6}, or nullopt when > 9.
std::optional<int> cloudSymbol3(int base, int cloudiness)
{
  if (cloudiness <= 5) return base;
  if (cloudiness <= 7) return base + 3;
  if (cloudiness <= 9) return base + 6;
  return {};
}

// Cloudiness → symbol on the 5-level clear/overcast scale.
// top_symbol is 7 (wawa_group1) or 9 (wawa_group2).
std::optional<int> cloudSymbol5(int cloudiness, int top_symbol)
{
  if (cloudiness <= 0) return 1;
  if (cloudiness <= 1) return 2;
  if (cloudiness <= 5) return 4;
  if (cloudiness <= 7) return 6;
  if (cloudiness <= 9) return top_symbol;
  return {};
}

enum class WawaPatternType
{
  None,
  Fixed,       // single symbol when cloudiness <= 9
  ThreeLevel,  // {base, base+3, base+6} for cloudiness <= 5/7/9
  FiveLevel    // full clear-overcast scale; base field = top symbol (7 or 9)
};

struct WawaPattern
{
  WawaPatternType type;
  int base;  // Fixed: symbol; ThreeLevel: first symbol; FiveLevel: top symbol
};

// Map wawa code + temperature to a symbol pattern.
WawaPattern wawaToSymbolPattern(int wawa, double temperature)
{
  const std::array<int, 10> group1{0, 4, 5, 10, 20, 21, 22, 23, 24, 25};
  const std::array<int, 5> group2{30, 31, 32, 33, 34};

  if (std::find(std::begin(group1), std::end(group1), wawa) != std::end(group1))
    return {WawaPatternType::FiveLevel, 7};
  if (std::find(std::begin(group2), std::end(group2), wawa) != std::end(group2))
    return {WawaPatternType::FiveLevel, 9};

  switch (wawa)
  {
    case 40: case 41: return {WawaPatternType::ThreeLevel, temperature <= 0 ? 51 : 31};
    case 42:          return {WawaPatternType::ThreeLevel, temperature <= 0 ? 53 : 33};
    case 60: case 61: return {WawaPatternType::ThreeLevel, 31};
    case 62:          return {WawaPatternType::ThreeLevel, 32};
    case 63:          return {WawaPatternType::ThreeLevel, 33};
    case 67:          return {WawaPatternType::ThreeLevel, 41};
    case 68:          return {WawaPatternType::ThreeLevel, 42};
    case 70: case 71: case 74: case 85: return {WawaPatternType::ThreeLevel, 51};
    case 72: case 75: case 86: return {WawaPatternType::ThreeLevel, 52};
    case 73: case 76: case 87: return {WawaPatternType::ThreeLevel, 53};
    case 77: case 78: return {WawaPatternType::Fixed, 57};
    case 80:          return {WawaPatternType::ThreeLevel, temperature <= 0 ? 51 : 21};
    case 89:          return {WawaPatternType::ThreeLevel, 61};
    default: break;
  }
  if (wawa >= 50 && wawa <= 53) return {WawaPatternType::Fixed, 11};
  if (wawa >= 54 && wawa <= 56) return {WawaPatternType::Fixed, 14};
  if (wawa >= 64 && wawa <= 66) return {WawaPatternType::Fixed, 17};
  if (wawa >= 81 && wawa <= 84) return {WawaPatternType::ThreeLevel, 21};
  return {WawaPatternType::None, 0};
}

}  // namespace

std::optional<int> calcSmartsymbolNumber(int wawa,
                                         int cloudiness,
                                         double temperature,
                                         const Fmi::LocalDateTime& ldt,
                                         double lat,
                                         double lon)
{
  const auto pattern = wawaToSymbolPattern(wawa, temperature);

  std::optional<int> smartsymbol;
  switch (pattern.type)
  {
    case WawaPatternType::ThreeLevel: smartsymbol = cloudSymbol3(pattern.base, cloudiness); break;
    case WawaPatternType::FiveLevel:  smartsymbol = cloudSymbol5(cloudiness, pattern.base); break;
    case WawaPatternType::Fixed:      if (cloudiness <= 9) smartsymbol = pattern.base; break;
    default: break;
  }

  if (!smartsymbol)
    return {};

  // Add day/night information
  Fmi::Astronomy::solar_position_t sp = Fmi::Astronomy::solar_position(ldt, lon, lat);
  return sp.dark() ? 100 + *smartsymbol : *smartsymbol;
}

TS::TimeSeriesVectorPtr initializeResultVector(const Settings& settings)
{
  TS::TimeSeriesVectorPtr ret = std::make_shared<TS::TimeSeriesVector>();

  // Set timeseries objects for each requested parameter
  for (unsigned int i = 0; i < settings.parameters.size(); i++)
    ret->emplace_back();

  return ret;
}

Fmi::DateTime epoch2ptime(double epoch)
{
  Fmi::DateTime ret = Fmi::date_time::from_time_t(static_cast<std::time_t>(floor(epoch)));
  ret += Fmi::date_time::Microseconds(static_cast<long>((epoch - floor(epoch)) * 1000000));

  return ret;
}

std::string getStringValue(const TS::Value& tv)
{
  // For some reason different databases/drivers don't simply use int for FMISID.
  // This is workaround code, FMISID should always be int.

  if (const double* dvalue = std::get_if<double>(&tv))
    return Fmi::to_string(*dvalue);

  if (const int* ivalue = std::get_if<int>(&tv))
    return Fmi::to_string(*ivalue);

  if (const std::string* svalue = std::get_if<std::string>(&tv))
    return *svalue;

  // These are just for getting more informative error messages:

  if (std::get_if<TS::None>(&tv))
  {
    throw Fmi::Exception(BCP, "Encountered NULL FMISID");
  }

  if (std::get_if<TS::LonLat>(&tv))
  {
    throw Fmi::Exception(BCP, "Encountered LonLat FMISID");
  }

  if (std::get_if<Fmi::LocalDateTime>(&tv))
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

    return (stationTypeMapPtr != namePtr->second.end());
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

    return (namePtr != parameterMap.end());
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
