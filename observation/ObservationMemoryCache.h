#pragma once

#include "DataItem.h"
#include "ParameterMap.h"
#include "Settings.h"
#include <macgyver/TimeZones.h>
#include <spine/Station.h>
#include <spine/TimeSeries.h>
#include <spine/TimeSeriesGeneratorOptions.h>
#include <map>
#include <unordered_set>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
struct ObservableProperty;

class ObservationMemoryCache
{
 public:
  /**
   * @brief Get the starting time of the cache
   * @retval The starting time of the data, or is_not_a_date_time if not initialized yet
   */

  boost::posix_time::ptime getStartTime() const;

  /**
   * @brief Add new observations to the cache. Never called simultaneously with clean.
   * @param cacheData The data to be added, sorted by time and station number
   * @retval Number of new observations inserted
   */

  std::size_t fill(const std::vector<DataItem> &cacheData) const;

  /**
   * @brief Delete old observations. Never called simultaneously with fill.
   * @param newstarttime Delete everything older than given time
   */

  void clean(const boost::posix_time::ptime &newstarttime) const;

  /**
   * @brief Retrieve observations from stations
   * @param stations The stations to retrieve
   * @param settings Time interval etc
   * @param parameteMap Desired parameters
   * @param timezones Global timezone information
   * @retval Time series vectors
   */

  Spine::TimeSeries::TimeSeriesVectorPtr getData(const Spine::Stations &stations,
                                                 const Settings &settings,
                                                 const ParameterMapPtr &parameterMap,
                                                 const Fmi::TimeZones &timezones) const;

  /**
   * @brief Retrieve observations from stations
   * @param stations The stations to retrieve
   * @param settings Time interval etc
   * @param parameteMap Desired parameters
   * @param timeSeriesOptions The time points to be picked
   * @param timezones Global timezone information
   * @retval Time series vectors
   */

  Spine::TimeSeries::TimeSeriesVectorPtr getData(
      const Spine::Stations &stations,
      const Settings &settings,
      const ParameterMapPtr &parameterMap,
      const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions,
      const Fmi::TimeZones &timezones) const;

 private:
  // The actual observations are divided by fmisid into vectors which are sorted by time
  // The observations for a single station are stored behind a share pointer which
  // is cheap to copy, since we usually do not update a lot of stations at a time.

  using StationObservations = std::vector<DataItem>;
  using Observations = std::map<int, boost::shared_ptr<StationObservations>>;
  mutable boost::shared_ptr<Observations> itsObservations;

  // Last value passed to clean()
  mutable boost::shared_ptr<boost::posix_time::ptime> itsStartTime;

  // All the hash values for the observations in the cache
  mutable std::unordered_set<std::size_t> itsHashValues;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
