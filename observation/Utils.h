#pragma once

#define PI 3.14159265358979323846

#include <spine/ConfigBase.h>
#include <spine/Parameter.h>
#include <spine/Station.h>

#include <boost/algorithm/string.hpp>
#include <boost/date_time/local_time/local_time.hpp>
#include <boost/optional.hpp>
#include <boost/utility.hpp>
#include <map>
#include <string>
#include <vector>

#include "ParameterMap.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
typedef struct FlashCounts
{
  int flashcount;
  int strokecount;
  int iccount;
} FlashCounts;

/** \brief Remove given prefix from an input string.
 * @param[in,out] parameter The string from which the prefix is wanted to remove.
 * @param[in] prefix The prefix string we are looking for.
 * @return true if the given prefix is found and removed otherwise false.
 */
bool removePrefix(std::string& parameter, const std::string& prefix);

/** *\brief Return true of a parameter looks to be normal enough to be an observation
 */

bool not_special(const SmartMet::Spine::Parameter& theParam);

std::string trimCommasFromEnd(const std::string& what);

std::string translateParameter(const std::string& paramname,
                               const std::string& stationType,
                               ParameterMap& parameterMap);

void calculateStationDirection(SmartMet::Spine::Station& station);
double deg2rad(double deg);
double rad2deg(double rad);

std::string windCompass8(double direction);
std::string windCompass16(double direction);
std::string windCompass32(double direction);

std::string parseParameterName(const std::string& parameter);
int parseSensorNumber(const std::string& parameter);

SmartMet::Spine::Stations removeDuplicateStations(SmartMet::Spine::Stations& stations);

// ----------------------------------------------------------------------
/*!
 * \brief Round down the given time to start of day
 */
// ----------------------------------------------------------------------
boost::posix_time::ptime day_start(const boost::posix_time::ptime& t);

// ----------------------------------------------------------------------
/*!
 * \brief Round up the given time to end of day
 */
// ----------------------------------------------------------------------
boost::posix_time::ptime day_end(const boost::posix_time::ptime& t);

// ----------------------------------------------------------------------
/*!
 * \brief Convert ptime to string
 */
// ----------------------------------------------------------------------
std::string timeToString(const boost::posix_time::ptime& time);

// ----------------------------------------------------------------------
/*!
 * \brief Write message to std::cout if quiet flag is false
 */
// ----------------------------------------------------------------------
void logMessage(const std::string& message, bool quiet);

// ----------------------------------------------------------------------
/*!
 * \brief Returns location cacahe key
 */
// ----------------------------------------------------------------------

std::string getLocationCacheKey(int geoID,
                                int numberOfStations,
                                std::string stationType,
                                int maxDistance,
                                const boost::posix_time::ptime& starttime,
                                const boost::posix_time::ptime& endtime);

// ----------------------------------------------------------------------
/*!
 * \brief Calculate the weather symbol using wawa code, temperature and cloudiness
 *
 * The logic is described in:
 * https://wiki.fmi.fi/display/PROJEKTIT/Havaintojen+muuntaminen+SmartSymboliksi
 */
// ----------------------------------------------------------------------

boost::optional<int> calcSmartsymbolNumber(int wawa,
                                           int cloudiness,
                                           double temperature,
                                           const boost::local_time::local_date_time& ldt,
                                           double lat,
                                           double lon);

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
