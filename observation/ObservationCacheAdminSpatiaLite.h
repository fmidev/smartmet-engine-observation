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
                                   const Fmi::DateTime& startTime,
                                   const Fmi::DateTime& lastModifiedTime,
                                   const Fmi::TimeZones& timezones) const override
  {
  }
  void readObservationCacheData(DataItems& cacheData,
                                const Fmi::TimePeriod& dataPeriod,
                                const std::string& fmisid,
                                const std::string& measurandId,
                                const Fmi::TimeZones& timezones) const override
  {
  }
  void readWeatherDataQCCacheData(std::vector<WeatherDataQCItem>& cacheData,
                                  const Fmi::TimePeriod& dataPeriod,
                                  const std::string& fmisid,
                                  const std::string& measurandId,
                                  const Fmi::TimeZones& timezones) const override
  {
  }
  void readFlashCacheData(std::vector<FlashDataItem>& cacheData,
                          const Fmi::TimePeriod& dataPeriod,
                          const Fmi::TimeZones& timezones) const override
  {
  }

  void readObservationCacheData(DataItems& cacheData,
                                const Fmi::DateTime& startTime,
                                const Fmi::DateTime& lastModifiedTime,
                                const Fmi::TimeZones& timezones) const override
  {
  }
  void readWeatherDataQCCacheData(std::vector<WeatherDataQCItem>& cacheData,
                                  const Fmi::DateTime& startTime,
                                  const Fmi::DateTime& lastModifiedTime,
                                  const Fmi::TimeZones& timezones) const override
  {
  }
  void readFlashCacheData(std::vector<FlashDataItem>& cacheData,
                          const Fmi::DateTime& startTime,
                          const Fmi::DateTime& lastStrokeTime,
                          const Fmi::DateTime& lastModifiedTime,
                          const Fmi::TimeZones& timezones) const override
  {
  }
  std::pair<Fmi::DateTime, Fmi::DateTime> getLatestWeatherDataQCTime(
      const std::shared_ptr<ObservationCache>& cache) const override;
  std::pair<Fmi::DateTime, Fmi::DateTime> getLatestObservationTime(
      const std::shared_ptr<ObservationCache>& cache) const override;
  std::map<std::string, Fmi::DateTime> getLatestFlashTime(
      const std::shared_ptr<ObservationCache>&) const override;

  void loadStations(const std::string& serializedStationsFile) override {}
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
