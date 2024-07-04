#pragma once

#define PI 3.14159265358979323846

#include "ParameterMap.h"
#include "Settings.h"
#include <boost/algorithm/string.hpp>
#include <macgyver/LocalDateTime.h>
#include <optional>
#include <boost/utility.hpp>
#include <spine/ConfigBase.h>
#include <spine/Parameter.h>
#include <spine/Station.h>
#include <timeseries/TimeSeriesInclude.h>
#include <map>
#include <string>
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
struct FlashCounts
{
  int flashcount{0};
  int strokecount{0};
  int iccount{0};
};

namespace Utils
{
/** \brief Remove given prefix from an input string.
 * @param[in,out] parameter The string from which the prefix is wanted to remove.
 * @param[in] prefix The prefix string we are looking for.
 * @return true if the given prefix is found and removed otherwise false.
 */
bool removePrefix(std::string& parameter, const std::string& prefix);

/** *\brief Return true of a parameter looks to be normal enough to be an observation
 */

bool not_special(const Spine::Parameter& theParam);

std::string trimCommasFromEnd(const std::string& what);

std::string translateParameter(const std::string& paramname,
                               const std::string& stationType,
                               ParameterMap& parameterMap);

void calculateStationDirection(Spine::Station& station);
double deg2rad(double deg);
double rad2deg(double rad);

std::string windCompass8(double direction, const std::string& missingValue);
std::string windCompass16(double direction, const std::string& missingValue);
std::string windCompass32(double direction, const std::string& missingValue);

std::string parseParameterName(const std::string& parameter);
int parseSensorNumber(const std::string& parameter);

Spine::Stations removeDuplicateStations(const Spine::Stations& stations);

// Rounded to seconds
Fmi::DateTime utc_second_clock();

// ----------------------------------------------------------------------
/*!
 * \brief Round down the given time to start of day
 */
// ----------------------------------------------------------------------
Fmi::DateTime day_start(const Fmi::DateTime& t);

// ----------------------------------------------------------------------
/*!
 * \brief Round up the given time to end of day
 */
// ----------------------------------------------------------------------
Fmi::DateTime day_end(const Fmi::DateTime& t);

// ----------------------------------------------------------------------
/*!
 * \brief Write message to std::cout if quiet flag is false
 */
// ----------------------------------------------------------------------
void logMessage(const std::string& message, bool quiet);

// ----------------------------------------------------------------------
/*!
 * \brief Calculate the weather symbol using wawa code, temperature and cloudiness
 *
 * The logic is described in:
 * https://wiki.fmi.fi/display/PROJEKTIT/Havaintojen+muuntaminen+SmartSymboliksi
 */
// ----------------------------------------------------------------------

std::optional<int> calcSmartsymbolNumber(int wawa,
                                           int cloudiness,
                                           double temperature,
                                           const Fmi::LocalDateTime& ldt,
                                           double lat,
                                           double lon);

// ----------------------------------------------------------------------
/*!
 * \brief Allocates and initializes a result vector for a query
 *
 */
// ----------------------------------------------------------------------

TS::TimeSeriesVectorPtr initializeResultVector(const Settings& settings);

// ----------------------------------------------------------------------
/*!
 * \brief converts epoch toptime
 *
 */
// ----------------------------------------------------------------------

Fmi::DateTime epoch2ptime(double epoch);

// ----------------------------------------------------------------------
/*!
 * \brief converts TS::Value to string
 *
 */
// ----------------------------------------------------------------------

std::string getStringValue(const TS::Value& tv);

// ----------------------------------------------------------------------
/*!
 * \brief Checks weather specified parameter name with specified stationtype can be found in
 * parameterMap
 *
 */
// ----------------------------------------------------------------------

bool isParameter(const std::string& name,
                 const std::string& stationType,
                 const ParameterMap& parameterMap);

// ----------------------------------------------------------------------
/*!
 * \brief Checks weather specified parameter name can be found in parameterMap
 *
 */
// ----------------------------------------------------------------------

bool isParameterVariant(const std::string& name, const ParameterMap& parameterMap);

}  // namespace Utils
}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
