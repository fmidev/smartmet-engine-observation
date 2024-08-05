#pragma once

#include "FlashDataItem.h"
#include "Settings.h"
#include "SpatiaLite.h"
#include <memory>
#include <macgyver/AtomicSharedPtr.h>
#include <macgyver/TimeZones.h>
#include <timeseries/TimeSeriesInclude.h>
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
   * @retval Fmi::DateTime The starting time of the data, or is_not_a_date_time if not
   * initialized yet
   */
  Fmi::DateTime getStartTime() const;

  /**
   * @brief Insert cached observations into observation_data table. Never called simultaneously with
   * clean.
   * @param cacheData Observation data to be inserted into the table, sorted by time and flash_id
   */

  std::size_t fill(const FlashDataItems& flashCacheData) const;

  /**
   * @brief Delete old flash observations. Never called simultaneously with fill.
   * @param newstarttime Delete everything older than given time
   */
  void clean(const Fmi::DateTime& newstarttime) const;

  /**
   * @brief Retrieve flash data
   * @param settings Time interval, bbox etc
   * @parameterMap Parameters to retrieve
   * @timezones Global timezone information
   */

  TS::TimeSeriesVectorPtr getData(const Settings& settings,
                                  const ParameterMapPtr& parameterMap,
                                  const Fmi::TimeZones& timezones) const;

  FlashCounts getFlashCount(const Fmi::DateTime& starttime,
                            const Fmi::DateTime& endtime,
                            const Spine::TaggedLocationList& locations) const;

 private:
  // The actual flash data in the cache
  using FlashDataVector = FlashDataItems;
  mutable Fmi::AtomicSharedPtr<FlashDataVector> itsFlashData;

  // Last value passed to clean()
  mutable Fmi::AtomicSharedPtr<Fmi::DateTime> itsStartTime;

  // All the hash values for the flashes in the cache
  mutable std::unordered_set<std::size_t> itsHashValues;

};  // class FlashMemoryCache

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
