
#pragma once

#include "ObservationCacheAdminBase.h"
#include "PostgreSQLDriverParameters.h"
#include "PostgreSQLObsDBConnectionPool.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class ObservationCacheAdminPostgreSQL : public ObservationCacheAdminBase
{
 public:
  ObservationCacheAdminPostgreSQL(const PostgreSQLDriverParameters& p,
                                  const std::unique_ptr<PostgreSQLObsDBConnectionPool>& pcp,
                                  Geonames::Engine* geonames,
                                  std::atomic<bool>& conn_ok,
                                  bool timer);
  void readObservationCacheData(DataItems& cacheData,
                                const Fmi::TimePeriod& dataPeriod,
                                const std::string& fmisid,
                                const std::string& measuradId,
                                const Fmi::TimeZones& timezones) const override;
  void readFlashCacheData(std::vector<FlashDataItem>& cacheData,
                          const Fmi::TimePeriod& dataPeriod,
                          const Fmi::TimeZones& timezones) const override;
  void readWeatherDataQCCacheData(DataItems& cacheData,
                                  const Fmi::TimePeriod& dataPeriod,
                                  const std::string& fmisid,
                                  const std::string& measuradId,
                                  const Fmi::TimeZones& timezones) const override;
  void readMovingStationsCacheData(std::vector<MovingLocationItem>& cacheData,
                                   const Fmi::DateTime& startTime,
                                   const Fmi::DateTime& lastModifiedTime,
                                   const Fmi::TimeZones& timezones) const override;
  void readObservationCacheData(DataItems& cacheData,
                                const Fmi::DateTime& startTime,
                                const Fmi::DateTime& lastModifiedTime,
                                const Fmi::TimeZones& timezones) const override;

  void readWeatherDataQCCacheData(DataItems& cacheData,
                                  const Fmi::DateTime& startTime,
                                  const Fmi::DateTime& lastModifiedTime,
                                  const Fmi::TimeZones& timezones) const override;
  void readFlashCacheData(std::vector<FlashDataItem>& cacheData,
                          const Fmi::DateTime& startTime,
                          const Fmi::DateTime& lastStrokeTime,
                          const Fmi::DateTime& lastModifiedTime,
                          const Fmi::TimeZones& timezones) const override;
  std::pair<Fmi::DateTime, Fmi::DateTime> getLatestWeatherDataQCTime(
      const std::shared_ptr<ObservationCache>& cache) const override;
  std::pair<Fmi::DateTime, Fmi::DateTime> getLatestObservationTime(
      const std::shared_ptr<ObservationCache>& cache) const override;
  std::map<std::string, Fmi::DateTime> getLatestFlashTime(
      const std::shared_ptr<ObservationCache>&) const override;
  void readMobileCacheData(const std::string& producer,
                           std::vector<MobileExternalDataItem>& cacheData,
                           Fmi::DateTime lastTime,
                           Fmi::DateTime lastCreatedTime,
                           const Fmi::TimeZones& timezones) const override;
  void readMagnetometerCacheData(std::vector<MagnetometerDataItem>& cacheData,
                                 const Fmi::DateTime& startTime,
                                 const Fmi::DateTime& lastModifiedTime,
                                 const Fmi::TimeZones& timezones) const override;

  void loadStations(const std::string& serializedStationsFile) override;

 private:
  const std::unique_ptr<PostgreSQLObsDBConnectionPool>& itsPostgreSQLConnectionPool;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
