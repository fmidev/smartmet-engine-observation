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
  void readMovingStationsCacheData(std::vector<MovingLocationItem>& cacheData,
								  const boost::posix_time::ptime& startTime,
								   const boost::posix_time::ptime& lastModifiedTime,
								   const Fmi::TimeZones& timezones) const override
  {
  }
  void readObservationCacheData(std::vector<DataItem>& cacheData,
                                const boost::posix_time::time_period& dataPeriod,
                                const std::string& fmisid,
                                const std::string& measurandId,
                                const Fmi::TimeZones& timezones) const override
  {
  }
  void readWeatherDataQCCacheData(std::vector<WeatherDataQCItem>& cacheData,
                                  const boost::posix_time::time_period& dataPeriod,
                                  const std::string& fmisid,
                                  const std::string& measurandId,
                                  const Fmi::TimeZones& timezones) const override
  {
  }
  void readFlashCacheData(std::vector<FlashDataItem>& cacheData,
                          const boost::posix_time::time_period& dataPeriod,
                          const Fmi::TimeZones& timezones) const override
  {
  }

  void readObservationCacheData(std::vector<DataItem>& cacheData,
                                const boost::posix_time::ptime& startTime,
                                const boost::posix_time::ptime& lastModifiedTime,
                                const Fmi::TimeZones& timezones) const override
  {
  }
  void readWeatherDataQCCacheData(std::vector<WeatherDataQCItem>& cacheData,
                                  const boost::posix_time::ptime& startTime,
                                  const boost::posix_time::ptime& lastModifiedTime,
                                  const Fmi::TimeZones& timezones) const override
  {
  }
  void readFlashCacheData(std::vector<FlashDataItem>& cacheData,
                          const boost::posix_time::ptime& startTime,
                          const boost::posix_time::ptime& lastStrokeTime,
                          const boost::posix_time::ptime& lastModifiedTime,
                          const Fmi::TimeZones& timezones) const override
  {
  }
  std::pair<boost::posix_time::ptime, boost::posix_time::ptime> getLatestWeatherDataQCTime(
      const std::shared_ptr<ObservationCache>& cache) const override;
  std::pair<boost::posix_time::ptime, boost::posix_time::ptime> getLatestObservationTime(
      const std::shared_ptr<ObservationCache>& cache) const override;
  std::map<std::string, boost::posix_time::ptime> getLatestFlashTime(
      const std::shared_ptr<ObservationCache>&) const override;

  void loadStations(const std::string& serializedStationsFile) override {}
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
