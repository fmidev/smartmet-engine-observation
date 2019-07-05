#pragma once

#include "EngineParameters.h"
#include "ObservationCache.h"
#include "Settings.h"

#include <macgyver/Cache.h>

#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class ObservableProperty;

class DummyCache : public ObservationCache
{
 public:
  DummyCache(const EngineParametersPtr &p);

  void initializeConnectionPool();
  void initializeCaches(int finCacheDuration,
                        int extCacheDuration,
                        int flashCacheDuration,
                        int flashMemoryCacheDuration);

  Spine::TimeSeries::TimeSeriesVectorPtr valuesFromCache(Settings &settings);
  Spine::TimeSeries::TimeSeriesVectorPtr valuesFromCache(
      Settings &settings, const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions);
  Spine::Stations getStationsByTaggedLocations(const Spine::TaggedLocationList &taggedLocations,
                                               const int numberofstations,
                                               const std::string &stationtype,
                                               const int maxdistance,
                                               const std::set<std::string> &stationgroup_codes,
                                               const boost::posix_time::ptime &starttime,
                                               const boost::posix_time::ptime &endtime);

  bool dataAvailableInCache(const Settings &settings) const;
  bool flashIntervalIsCached(const boost::posix_time::ptime &starttime,
                             const boost::posix_time::ptime &endtime) const;
  void getStationsByBoundingBox(Spine::Stations &stations, const Settings &settings) const;
  void updateStationsAndGroups(const StationInfo &info) const;

  Spine::Stations findAllStationsFromGroups(const std::set<std::string> stationgroup_codes,
                                            const StationInfo &info,
                                            const boost::posix_time::ptime &starttime,
                                            const boost::posix_time::ptime &endtime) const;
  bool getStationById(Spine::Station &station,
                      int station_id,
                      const std::set<std::string> &stationgroup_codes,
                      const boost::posix_time::ptime &starttime,
                      const boost::posix_time::ptime &endtime) const;
  Spine::Stations findStationsInsideArea(const Settings &settings,
                                         const std::string &areaWkt,
                                         const StationInfo &info) const;
  FlashCounts getFlashCount(const boost::posix_time::ptime &starttime,
                            const boost::posix_time::ptime &endtime,
                            const Spine::TaggedLocationList &locations) const;

  boost::posix_time::ptime getLatestFlashTime() const;
  std::size_t fillFlashDataCache(const FlashDataItems &flashCacheData) const;
  void cleanFlashDataCache(const boost::posix_time::time_duration &timetokeep,
                           const boost::posix_time::time_duration &timetokeep_memory) const;

  boost::posix_time::ptime getLatestObservationModifiedTime() const;
  boost::posix_time::ptime getLatestObservationTime() const;
  std::size_t fillDataCache(const DataItems &cacheData) const;
  void cleanDataCache(const boost::posix_time::time_duration &timetokeep,
                      const boost::posix_time::time_duration &timetokeep_memory) const;

  boost::posix_time::ptime getLatestWeatherDataQCTime() const;
  std::size_t fillWeatherDataQCCache(const WeatherDataQCItems &cacheData) const;
  void cleanWeatherDataQCCache(const boost::posix_time::time_duration &timetokeep) const;

  bool roadCloudIntervalIsCached(const boost::posix_time::ptime &starttime,
                                 const boost::posix_time::ptime &endtime) const;
  boost::posix_time::ptime getLatestRoadCloudDataTime() const;
  std::size_t fillRoadCloudCache(const MobileExternalDataItems &mobileExternalCacheData) const;
  void cleanRoadCloudCache(const boost::posix_time::time_duration &timetokeep) const;

  bool netAtmoIntervalIsCached(const boost::posix_time::ptime &starttime,
                               const boost::posix_time::ptime &endtime) const;
  boost::posix_time::ptime getLatestNetAtmoDataTime() const;
  std::size_t fillNetAtmoCache(const MobileExternalDataItems &mobileExternalCacheData) const;
  void cleanNetAtmoCache(const boost::posix_time::time_duration &timetokeep) const;

  void fillLocationCache(const LocationItems &locations) const;

  boost::shared_ptr<std::vector<ObservableProperty> > observablePropertyQuery(
      std::vector<std::string> &parameters, const std::string language) const;
  bool cacheHasStations() const;
  void shutdown();

 private:
  EngineParametersPtr itsParameters;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
