#pragma once

#include "FlashDataItem.h"
#include "Settings.h"
#include "SpatiaLite.h"
#include <boost/shared_ptr.hpp>
#include <macgyver/TimeZones.h>
#include <spine/TimeSeries.h>
#include <unordered_set>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class SpatiaLite;

// RAM cache for lightning data. Intended for speeding up the
// retrieval of the most recent observations.

class FlashMemoryCache
{
 public:
  /**
   * @brief Get the starting time of the cache
   * @retval boost::posix_time::ptime The starting time of the data, or is_not_a_date_time if not
   * initialized yet
   */
  boost::posix_time::ptime getStartTime() const;

  /**
   * @brief Insert cached observations into observation_data table. Never called simultaneously with
   * clean.
   * @param cacheData Observation data to be inserted into the table, sorted by time and flash_id
   */

  std::size_t fill(const std::vector<FlashDataItem>& flashCacheData) const;

  /**
   * @brief Delete old flash observations. Never called simultaneously with fill.
   * @param newstarttime Delete everything older than given time
   */
  void clean(const boost::posix_time::ptime& newstarttime) const;

  /**
   * @brief Retrieve flash data
   * @param settings Time interval, bbox etc
   * @parameterMap Parameters to retrieve
   * @timezones Global timezone information
   */

  Spine::TimeSeries::TimeSeriesVectorPtr getData(const Settings& settings,
                                                 const ParameterMapPtr& parameterMap,
                                                 const Fmi::TimeZones& timezones) const;

 private:
  // The actual flash data in the cache
  using FlashDataVector = std::vector<FlashDataItem>;
  mutable boost::shared_ptr<FlashDataVector> itsFlashData;

  // Last value passed to clean()
  mutable boost::shared_ptr<boost::posix_time::ptime> itsStartTime;

  // All the hash values for the flashes in the cache
  mutable std::unordered_set<std::size_t> itsHashValues;

};  // class FlashMemoryCache

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
