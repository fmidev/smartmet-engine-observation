#pragma once

#include "DatabaseDriverParameters.h"
#include "ObservationCacheProxy.h"
#include <engines/geonames/Engine.h>
#include <macgyver/AsyncTaskGroup.h>
#include <macgyver/TimeZones.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class ObservationCacheAdminBase
{
 public:
  virtual void readObservationCacheData(std::vector<DataItem>& cacheData,
                                        const boost::posix_time::time_period& dataPeriod,
                                        const std::string& fmisid,
                                        const std::string& measuradId,
                                        const Fmi::TimeZones& timezones) const = 0;
  virtual void readFlashCacheData(std::vector<FlashDataItem>& cacheData,
                                  const boost::posix_time::time_period& dataPeriod,
                                  const Fmi::TimeZones& timezones) const = 0;
  virtual void readWeatherDataQCCacheData(std::vector<WeatherDataQCItem>& cacheData,
                                          const boost::posix_time::time_period& dataPeriod,
                                          const std::string& fmisid,
                                          const std::string& measuradId,
                                          const Fmi::TimeZones& timezones) const = 0;
  virtual void readObservationCacheData(std::vector<DataItem>& cacheData,
                                        const boost::posix_time::ptime& startTime,
                                        const boost::posix_time::ptime& lastModifiedTime,
                                        const Fmi::TimeZones& timezones) const = 0;
  virtual void readFlashCacheData(std::vector<FlashDataItem>& cacheData,
                                  const boost::posix_time::ptime& startTime,
                                  const boost::posix_time::ptime& lastStrokeTime,
                                  const boost::posix_time::ptime& lastModifiedTime,
                                  const Fmi::TimeZones& timezones) const = 0;
  virtual void readWeatherDataQCCacheData(std::vector<WeatherDataQCItem>& cacheData,
                                          const boost::posix_time::ptime& startTime,
                                          const boost::posix_time::ptime& lastModifiedTime,
                                          const Fmi::TimeZones& timezones) const = 0;
  // Get start time from obstime and last_modified
  virtual std::pair<boost::posix_time::ptime, boost::posix_time::ptime> getLatestWeatherDataQCTime(
      const boost::shared_ptr<ObservationCache>&) const = 0;
  virtual std::pair<boost::posix_time::ptime, boost::posix_time::ptime> getLatestObservationTime(
      const boost::shared_ptr<ObservationCache>&) const = 0;
  virtual std::map<std::string, boost::posix_time::ptime> getLatestFlashTime(
      const boost::shared_ptr<ObservationCache>&) const = 0;
  virtual void readMobileCacheData(const std::string& producer,
                                   std::vector<MobileExternalDataItem>& cacheData,
                                   boost::posix_time::ptime lastTime,
                                   boost::posix_time::ptime lastCreatedTime,
                                   const Fmi::TimeZones& timezones) const
  {
  }

  virtual void loadStations(const std::string& serializedStationsFile) = 0;
  void reloadStations();

  void init();
  void shutdown();

 protected:
  ObservationCacheAdminBase(const DatabaseDriverParameters& parameters,
                            SmartMet::Engine::Geonames::Engine* geonames,
                            boost::atomic<bool>& conn_ok,
                            bool timer);

  virtual ~ObservationCacheAdminBase();

  void addInfoToStations(SmartMet::Spine::Stations& stations, const std::string& language) const;

  const DatabaseDriverParameters& itsParameters;
  const boost::shared_ptr<ObservationCacheProxy>& itsCacheProxy;
  SmartMet::Engine::Geonames::Engine* itsGeonames;
  boost::atomic<bool> itsShutdownRequested;
  boost::atomic<bool>& itsConnectionsOK;
  bool itsTimer{false};
  Fmi::TimeZones itsTimeZones;
  bool itsStationsCurrentlyLoading{false};

 private:
  void updateObservationFakeCache(boost::shared_ptr<ObservationCache>& cache) const;
  void updateWeatherDataQCFakeCache(boost::shared_ptr<ObservationCache>& cache) const;
  void updateFlashFakeCache(boost::shared_ptr<ObservationCache>& cache) const;

  void startCacheUpdateThreads(const std::set<std::string>& tables);

  void loadStations();
  void updateFlashCache() const;
  void updateObservationCache() const;
  void updateWeatherDataQCCache() const;
  void updateNetAtmoCache() const;
  void updateRoadCloudCache() const;
  void updateFmiIoTCache() const;

  void updateFlashCacheLoop();
  void updateObservationCacheLoop();
  void updateWeatherDataQCCacheLoop();
  void updateNetAtmoCacheLoop();
  void updateRoadCloudCacheLoop();
  void updateFmiIoTCacheLoop();
  void updateStationsCacheLoop();

  void calculateStationDirection(SmartMet::Spine::Station& station) const;
  void addInfoToStation(SmartMet::Spine::Station& station, const std::string& language) const;
  boost::shared_ptr<ObservationCache> getCache(const std::string& tablename) const;
  std::string driverName() const;

  std::unique_ptr<Fmi::AsyncTaskGroup> background_tasks;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
