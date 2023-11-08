#pragma once

#include "DataItem.h"
#include "LocationDataItem.h"
#include "ParameterMap.h"
#include "Settings.h"
#include <boost/smart_ptr/atomic_shared_ptr.hpp>
#include <macgyver/TimeZones.h>
#include <spine/Station.h>
#include <timeseries/TimeSeriesInclude.h>
#include <map>
#include <unordered_set>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
struct QueryMapping;
class StationInfo;

class ObservationMemoryCache
{
 public:
  virtual ~ObservationMemoryCache();

  ObservationMemoryCache() = default;
  ObservationMemoryCache(const ObservationMemoryCache &other) = delete;
  ObservationMemoryCache(ObservationMemoryCache &&other) = delete;
  ObservationMemoryCache &operator=(const ObservationMemoryCache &other) = delete;
  ObservationMemoryCache &operator=(ObservationMemoryCache &&other) = delete;

  /**
   * @brief Get the starting time of the cache
   * @retval The starting time of the data, or is_not_a_date_time if not initialized yet
   */

  Fmi::DateTime getStartTime() const;

  /**
   * @brief Add new observations to the cache. Never called simultaneously with clean.
   * @param cacheData The data to be added, sorted by time and station number
   * @retval Number of new observations inserted
   */

  std::size_t fill(const DataItems &cacheData) const;

  /**
   * @brief Delete old observations. Never called simultaneously with fill.
   * @param newstarttime Delete everything older than given time
   */

  void clean(const Fmi::DateTime &newstarttime) const;

  /**
   * @brief Retrieve observations from stations
   * @param stations The stations to retrieve
   * @param settings Time interval etc
   * @param qmap Mapping from query options to output timeseries positions
   * @retval Vector of narrow table observations
   */

  LocationDataItems read_observations(const Spine::Stations &stations,
                                      const Settings &settings,
                                      const StationInfo &stationInfo,
                                      const std::set<std::string> &stationgroup_codes,
                                      const QueryMapping &qmap) const;

 private:
  // The actual observations are divided by fmisid into vectors which are sorted by time

  using Observations = std::map<int, boost::atomic_shared_ptr<DataItems> *>;
  mutable boost::atomic_shared_ptr<Observations> itsObservations;

  // Last value passed to clean()
  mutable boost::atomic_shared_ptr<Fmi::DateTime> itsStartTime;

  // All the hash values for the observations in the cache
  mutable std::unordered_set<std::size_t> itsHashValues;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
