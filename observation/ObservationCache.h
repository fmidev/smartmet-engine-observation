#pragma once

#include "DataItem.h"
#include "FlashDataItem.h"
#include "LocationItem.h"
#include "MobileExternalDataItem.h"
#include "Settings.h"
#include "StationInfo.h"
#include "Utils.h"
#include "WeatherDataQCItem.h"
#include <spine/Station.h>
#include <spine/TimeSeries.h>
#include <spine/TimeSeriesGeneratorOptions.h>

#include <macgyver/Cache.h>

#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
struct ObservableProperty;

class ObservationCache
{
 public:
  virtual ~ObservationCache();

  virtual void initializeConnectionPool() = 0;
  virtual void initializeCaches(int finCacheDuration,
                                int extCacheDuration,
                                int flashCacheDuration,
                                int flashMemoryCacheDuration) = 0;

  virtual Spine::TimeSeries::TimeSeriesVectorPtr valuesFromCache(Settings &settings) = 0;

  virtual Spine::TimeSeries::TimeSeriesVectorPtr valuesFromCache(
      Settings &settings, const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions) = 0;

  virtual Spine::Stations getStationsByTaggedLocations(
      const Spine::TaggedLocationList &taggedLocations,
      const int numberofstations,
      const std::string &stationtype,
      const int maxdistance,
      const std::set<std::string> &stationgroup_codes,
      const boost::posix_time::ptime &starttime,
      const boost::posix_time::ptime &endtime) = 0;

  virtual bool dataAvailableInCache(const Settings &settings) const = 0;

  virtual bool flashIntervalIsCached(const boost::posix_time::ptime &starttime,
                                     const boost::posix_time::ptime &endtime) const = 0;

  virtual void getStationsByBoundingBox(Spine::Stations &stations,
                                        const Settings &settings) const = 0;

  virtual void updateStationsAndGroups(const StationInfo &info) const = 0;

  virtual Spine::Stations findAllStationsFromGroups(
      const std::set<std::string> stationgroup_codes,
      const StationInfo &info,
      const boost::posix_time::ptime &starttime,
      const boost::posix_time::ptime &endtime) const = 0;

  virtual bool getStationById(Spine::Station &station,
                              int station_id,
                              const std::set<std::string> &stationgroup_codes,
                              const boost::posix_time::ptime &starttime,
                              const boost::posix_time::ptime &endtime) const = 0;

  virtual Spine::Stations findStationsInsideArea(const Settings &settings,
                                                 const std::string &areaWkt,
                                                 const StationInfo &info) const = 0;

  virtual FlashCounts getFlashCount(const boost::posix_time::ptime &starttime,
                                    const boost::posix_time::ptime &endtime,
                                    const Spine::TaggedLocationList &locations) const = 0;

  virtual boost::posix_time::ptime getLatestFlashTime() const = 0;
  virtual std::size_t fillFlashDataCache(const FlashDataItems &flashCacheData) const = 0;
  virtual void cleanFlashDataCache(
      const boost::posix_time::time_duration &timetokeep,
      const boost::posix_time::time_duration &timetokeep_memory) const = 0;

  virtual boost::posix_time::ptime getLatestObservationModifiedTime() const = 0;
  virtual boost::posix_time::ptime getLatestObservationTime() const = 0;
  virtual std::size_t fillDataCache(const DataItems &cacheData) const = 0;
  virtual void cleanDataCache(const boost::posix_time::time_duration &timetokeep,
                              const boost::posix_time::time_duration &timetokeep_memory) const = 0;

  virtual boost::posix_time::ptime getLatestWeatherDataQCTime() const = 0;
  virtual std::size_t fillWeatherDataQCCache(const WeatherDataQCItems &cacheData) const = 0;
  virtual void cleanWeatherDataQCCache(
      const boost::posix_time::time_duration &timetokeep) const = 0;

  virtual bool roadCloudIntervalIsCached(const boost::posix_time::ptime &starttime,
                                         const boost::posix_time::ptime &endtime) const = 0;
  virtual boost::posix_time::ptime getLatestRoadCloudDataTime() const = 0;
  virtual std::size_t fillRoadCloudCache(
      const MobileExternalDataItems &mobileExternalCacheData) const = 0;
  virtual void cleanRoadCloudCache(const boost::posix_time::time_duration &timetokeep) const = 0;

  virtual bool netAtmoIntervalIsCached(const boost::posix_time::ptime &starttime,
                                       const boost::posix_time::ptime &endtime) const = 0;
  virtual boost::posix_time::ptime getLatestNetAtmoDataTime() const = 0;
  virtual std::size_t fillNetAtmoCache(
      const MobileExternalDataItems &mobileExternalCacheData) const = 0;
  virtual void cleanNetAtmoCache(const boost::posix_time::time_duration &timetokeep) const = 0;

  virtual void fillLocationCache(const LocationItems &locations) const = 0;

  virtual boost::shared_ptr<std::vector<ObservableProperty> > observablePropertyQuery(
      std::vector<std::string> &parameters, const std::string language) const = 0;
  virtual bool cacheHasStations() const = 0;
  virtual void shutdown() = 0;

 protected:
  ObservationCache();
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
