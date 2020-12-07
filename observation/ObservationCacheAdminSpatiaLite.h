#pragma once

#include "ObservationCacheAdminBase.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class ObservationCacheAdminSpatiaLite : public ObservationCacheAdminBase
{
 public:
  ObservationCacheAdminSpatiaLite(const DatabaseDriverParameters& p,
                                  SmartMet::Engine::Geonames::Engine* geonames,
                                  std::atomic<bool>& conn_ok,
                                  bool timer);
  void readObservationCacheData(std::vector<DataItem>& cacheData,
                                const boost::posix_time::time_period& dataPeriod,
                                const std::string& fmisid,
                                const std::string& measurandId,
                                const Fmi::TimeZones& timezones) const
  {
  }
  void readWeatherDataQCCacheData(std::vector<WeatherDataQCItem>& cacheData,
                                  const boost::posix_time::time_period& dataPeriod,
                                  const std::string& fmisid,
                                  const std::string& measurandId,
                                  const Fmi::TimeZones& timezones) const
  {
  }
  void readFlashCacheData(std::vector<FlashDataItem>& cacheData,
                          const boost::posix_time::time_period& dataPeriod,
                          const Fmi::TimeZones& timezones) const
  {
  }

  void readObservationCacheData(std::vector<DataItem>& cacheData,
                                const boost::posix_time::ptime& startTime,
                                const boost::posix_time::ptime& lastModifiedTime,
                                const Fmi::TimeZones& timezones) const
  {
  }
  void readWeatherDataQCCacheData(std::vector<WeatherDataQCItem>& cacheData,
                                  const boost::posix_time::ptime& startTime,
                                  const boost::posix_time::ptime& lastModifiedTime,
                                  const Fmi::TimeZones& timezones) const
  {
  }
  void readFlashCacheData(std::vector<FlashDataItem>& cacheData,
                          const boost::posix_time::ptime& startTime,
                          const boost::posix_time::ptime& lastStrokeTime,
                          const boost::posix_time::ptime& lastModifiedTime,
                          const Fmi::TimeZones& timezones) const
  {
  }
  std::pair<boost::posix_time::ptime, boost::posix_time::ptime> getLatestWeatherDataQCTime(
      const boost::shared_ptr<ObservationCache>& cache) const;
  std::pair<boost::posix_time::ptime, boost::posix_time::ptime> getLatestObservationTime(
      const boost::shared_ptr<ObservationCache>& cache) const;
  std::map<std::string, boost::posix_time::ptime> getLatestFlashTime(
      const boost::shared_ptr<ObservationCache>&) const;

  void loadStations(const std::string& serializedStationsFile) {}
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
