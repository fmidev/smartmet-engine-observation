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
  virtual ~ObservationCacheAdminBase();
  ObservationCacheAdminBase() = delete;
  ObservationCacheAdminBase(const ObservationCacheAdminBase& other) = delete;
  ObservationCacheAdminBase(ObservationCacheAdminBase&& other) = delete;
  ObservationCacheAdminBase& operator=(const ObservationCacheAdminBase& other) = delete;
  ObservationCacheAdminBase& operator=(ObservationCacheAdminBase&& other) = delete;

  virtual void readObservationCacheData(DataItems& cacheData,
                                        const Fmi::TimePeriod& dataPeriod,
                                        const std::string& fmisid,
                                        const std::string& measuradId,
                                        const Fmi::TimeZones& timezones) const = 0;
  virtual void readFlashCacheData(std::vector<FlashDataItem>& cacheData,
                                  const Fmi::TimePeriod& dataPeriod,
                                  const Fmi::TimeZones& timezones) const = 0;
  virtual void readWeatherDataQCCacheData(std::vector<DataItem>& cacheData,
                                          const Fmi::TimePeriod& dataPeriod,
                                          const std::string& fmisid,
                                          const std::string& measuradId,
                                          const Fmi::TimeZones& timezones) const = 0;
  virtual void readMovingStationsCacheData(std::vector<MovingLocationItem>& cacheData,
                                           const Fmi::DateTime& startTime,
                                           const Fmi::DateTime& lastModifiedTime,
                                           const Fmi::TimeZones& timezones) const = 0;
  virtual void readObservationCacheData(DataItems& cacheData,
                                        const Fmi::DateTime& startTime,
                                        const Fmi::DateTime& lastModifiedTime,
                                        const Fmi::TimeZones& timezones) const = 0;
  virtual void readFlashCacheData(std::vector<FlashDataItem>& cacheData,
                                  const Fmi::DateTime& startTime,
                                  const Fmi::DateTime& lastStrokeTime,
                                  const Fmi::DateTime& lastModifiedTime,
                                  const Fmi::TimeZones& timezones) const = 0;
  virtual void readWeatherDataQCCacheData(std::vector<DataItem>& cacheData,
                                          const Fmi::DateTime& startTime,
                                          const Fmi::DateTime& lastModifiedTime,
                                          const Fmi::TimeZones& timezones) const = 0;
  // Get start time from obstime and last_modified
  virtual std::pair<Fmi::DateTime, Fmi::DateTime> getLatestWeatherDataQCTime(
      const std::shared_ptr<ObservationCache>&) const = 0;
  virtual std::pair<Fmi::DateTime, Fmi::DateTime> getLatestObservationTime(
      const std::shared_ptr<ObservationCache>&) const = 0;
  virtual std::map<std::string, Fmi::DateTime> getLatestFlashTime(
      const std::shared_ptr<ObservationCache>&) const = 0;
  virtual void readMobileCacheData(const std::string& producer,
                                   std::vector<MobileExternalDataItem>& cacheData,
                                   Fmi::DateTime lastTime,
                                   Fmi::DateTime lastCreatedTime,
                                   const Fmi::TimeZones& timezones) const
  {
  }
  virtual void readMagnetometerCacheData(std::vector<MagnetometerDataItem>& cacheData,
                                         const Fmi::DateTime& startTime,
                                         const Fmi::DateTime& lastModifiedTime,
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
                            std::atomic<bool>& conn_ok,
                            bool timer);

  void addInfoToStations(Spine::Stations& stations, const std::string& language) const;

  const DatabaseDriverParameters& itsParameters;
  const std::shared_ptr<ObservationCacheProxy>& itsCacheProxy;
  SmartMet::Engine::Geonames::Engine* itsGeonames;
  std::atomic<bool>& itsConnectionsOK;
  bool itsTimer{false};
  Fmi::TimeZones itsTimeZones;
  bool itsStationsCurrentlyLoading{false};

 private:
  void updateObservationFakeCache(std::shared_ptr<ObservationCache>& cache) const;
  void updateWeatherDataQCFakeCache(std::shared_ptr<ObservationCache>& cache) const;
  void updateFlashFakeCache(std::shared_ptr<ObservationCache>& cache) const;
  void emulateFlashCacheUpdate(std::shared_ptr<ObservationCache>& cache) const;

  void startCacheUpdateThreads(const std::set<std::string>& tables);

  void loadStations();
  void updateFlashCache() const;
  void updateObservationCache() const;
  void updateWeatherDataQCCache() const;
  void updateNetAtmoCache() const;
  void updateRoadCloudCache() const;
  void updateFmiIoTCache() const;
  void updateTapsiQcCache() const;
  void updateMagnetometerCache() const;

  void updateFlashCacheLoop();
  void updateObservationCacheLoop();
  void updateWeatherDataQCCacheLoop();
  void updateNetAtmoCacheLoop();
  void updateRoadCloudCacheLoop();
  void updateFmiIoTCacheLoop();
  void updateTapsiQcCacheLoop();
  void updateMagnetometerCacheLoop();
  void updateStationsCacheLoop();

  void calculateStationDirection(Spine::Station& station) const;
  void addInfoToStation(Spine::Station& station, const std::string& language) const;
  std::shared_ptr<ObservationCache> getCache(const std::string& tablename) const;
  std::string driverName() const;

  std::shared_ptr<Fmi::AsyncTaskGroup> itsBackgroundTasks;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
