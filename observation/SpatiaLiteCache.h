#pragma once

#include "EngineParameters.h"
#include "ObservationCache.h"
#include "Settings.h"
#include "SpatiaLiteCacheParameters.h"
#include "SpatiaLiteConnectionPool.h"
#include "StationtypeConfig.h"

#include <macgyver/Cache.h>

#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class ObservableProperty;

class SpatiaLiteCache : public ObservationCache
{
 public:
  SpatiaLiteCache(boost::shared_ptr<EngineParameters> p, Spine::ConfigBase &cfg);
  ~SpatiaLiteCache();

  void initializeConnectionPool(int finCacheDuration);

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
                      const std::set<std::string> &stationgroup_codes) const;
  Spine::Stations findStationsInsideArea(const Settings &settings,
                                         const std::string &areaWkt,
                                         const StationInfo &info) const;
  FlashCounts getFlashCount(const boost::posix_time::ptime &starttime,
                            const boost::posix_time::ptime &endtime,
                            const Spine::TaggedLocationList &locations) const;
  boost::posix_time::ptime getLatestFlashTime() const;
  std::size_t fillFlashDataCache(const std::vector<FlashDataItem> &flashCacheData) const;
  void cleanFlashDataCache(const boost::posix_time::time_duration &timetokeep) const;
  boost::posix_time::ptime getLatestObservationTime() const;
  std::size_t fillDataCache(const std::vector<DataItem> &cacheData) const;
  void cleanDataCache(const boost::posix_time::time_duration &timetokeep) const;
  boost::posix_time::ptime getLatestWeatherDataQCTime() const;
  std::size_t fillWeatherDataQCCache(const std::vector<WeatherDataQCItem> &cacheData) const;
  void cleanWeatherDataQCCache(const boost::posix_time::time_duration &timetokeep) const;
  void fillLocationCache(const std::vector<LocationItem> &locations) const;

  boost::shared_ptr<std::vector<ObservableProperty> > observablePropertyQuery(
      std::vector<std::string> &parameters, const std::string language) const;
  bool cacheHasStations() const;

  void shutdown();

 private:
  Spine::Stations getStationsFromSpatiaLite(Settings &settings,
                                            boost::shared_ptr<SpatiaLite> spatialitedb);
  bool timeIntervalIsCached(const boost::posix_time::ptime &starttime,
                            const boost::posix_time::ptime &endtime) const;
  bool timeIntervalWeatherDataQCIsCached(const boost::posix_time::ptime &starttime,
                                         const boost::posix_time::ptime &endtime) const;
  Spine::TimeSeries::TimeSeriesVectorPtr flashValuesFromSpatiaLite(Settings &settings) const;
  void readConfig(Spine::ConfigBase &cfg);

  SpatiaLiteConnectionPool *itsConnectionPool = nullptr;
  Fmi::Cache::Cache<std::string, std::vector<Spine::Station> > itsLocationCache;
  Fmi::TimeZones itsTimeZones;

  SpatiaLiteCacheParameters itsParameters;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
