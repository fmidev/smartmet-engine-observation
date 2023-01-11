
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
  void readObservationCacheData(std::vector<DataItem>& cacheData,
                                const boost::posix_time::time_period& dataPeriod,
                                const std::string& fmisid,
                                const std::string& measuradId,
                                const Fmi::TimeZones& timezones) const override;
  void readFlashCacheData(std::vector<FlashDataItem>& cacheData,
                          const boost::posix_time::time_period& dataPeriod,
                          const Fmi::TimeZones& timezones) const override;
  void readWeatherDataQCCacheData(std::vector<WeatherDataQCItem>& cacheData,
                                  const boost::posix_time::time_period& dataPeriod,
                                  const std::string& fmisid,
                                  const std::string& measuradId,
                                  const Fmi::TimeZones& timezones) const override;
  void readMovingStationsCacheData(std::vector<MovingLocationItem>& cacheData,
								  const boost::posix_time::ptime& startTime,
								   const boost::posix_time::ptime& lastModifiedTime,
								   const Fmi::TimeZones& timezones) const override;
  void readObservationCacheData(std::vector<DataItem>& cacheData,
                                const boost::posix_time::ptime& startTime,
                                const boost::posix_time::ptime& lastModifiedTime,
                                const Fmi::TimeZones& timezones) const override;

  void readWeatherDataQCCacheData(std::vector<WeatherDataQCItem>& cacheData,
                                  const boost::posix_time::ptime& startTime,
                                  const boost::posix_time::ptime& lastModifiedTime,
                                  const Fmi::TimeZones& timezones) const override;
  void readFlashCacheData(std::vector<FlashDataItem>& cacheData,
                          const boost::posix_time::ptime& startTime,
                          const boost::posix_time::ptime& lastStrokeTime,
                          const boost::posix_time::ptime& lastModifiedTime,
                          const Fmi::TimeZones& timezones) const override;
  std::pair<boost::posix_time::ptime, boost::posix_time::ptime> getLatestWeatherDataQCTime(
      const std::shared_ptr<ObservationCache>& cache) const override;
  std::pair<boost::posix_time::ptime, boost::posix_time::ptime> getLatestObservationTime(
      const std::shared_ptr<ObservationCache>& cache) const override;
  std::map<std::string, boost::posix_time::ptime> getLatestFlashTime(
      const std::shared_ptr<ObservationCache>&) const override;
  void readMobileCacheData(const std::string& producer,
                           std::vector<MobileExternalDataItem>& cacheData,
                           boost::posix_time::ptime lastTime,
                           boost::posix_time::ptime lastCreatedTime,
                           const Fmi::TimeZones& timezones) const override;
  void readMagnetometerCacheData(std::vector<MagnetometerDataItem>& cacheData,
                                 const boost::posix_time::ptime& startTime,
                                 const boost::posix_time::ptime& lastModifiedTime,
                                 const Fmi::TimeZones& timezones) const override;

  void loadStations(const std::string& serializedStationsFile) override;

 private:
  const std::unique_ptr<PostgreSQLObsDBConnectionPool>& itsPostgreSQLConnectionPool;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
