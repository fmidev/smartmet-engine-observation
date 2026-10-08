#pragma once

#include "FlashDataItem.h"
#include "Settings.h"
#include "SpatiaLite.h"
#include <macgyver/AtomicSharedPtr.h>
#include <macgyver/TimeZones.h>
#include <timeseries/TimeSeriesInclude.h>
#include <cstdint>
#include <memory>
#include <optional>
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

  /**
   * @brief Fingerprint of the strokes in a time window and bounding box
   * @retval The id of the newest fill which added strokes overlapping the closed time
   *         interval and the lon/lat box (edges inclusive), zero if no fill has, and
   *         nullopt if the window starts before the cache start time
   */
  std::optional<std::uint64_t> latestGeneration(const Fmi::DateTime& starttime,
                                                const Fmi::DateTime& endtime,
                                                double minlon,
                                                double minlat,
                                                double maxlon,
                                                double maxlat) const;

  // One record per fill() which added strokes
  struct Generation
  {
    std::uint64_t id = 0;   // monotonically increasing, starts at 1
    Fmi::DateTime mintime;  // stroke time range of the strokes added
    Fmi::DateTime maxtime;
    double minlon = 0;  // bounding box of the strokes added
    double minlat = 0;
    double maxlon = 0;
    double maxlat = 0;
  };

 private:
  // The actual flash data in the cache
  using FlashDataVector = FlashDataItems;
  mutable Fmi::AtomicSharedPtr<FlashDataVector> itsFlashData;

  // Last value passed to clean()
  mutable Fmi::AtomicSharedPtr<Fmi::DateTime> itsStartTime;

  // All the hash values for the flashes in the cache
  mutable std::unordered_set<std::size_t> itsHashValues;

  // Generation log, oldest first. Like the data, replaced atomically so readers never lock.
  using Generations = std::vector<Generation>;
  mutable Fmi::AtomicSharedPtr<Generations> itsGenerations;

  // Id of the latest generation, only fill() updates it
  mutable std::uint64_t itsLatestGeneration = 0;

};  // class FlashMemoryCache

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
