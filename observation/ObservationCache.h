#pragma once

#include "Settings.h"
#include "Utils.h"
#include "DataItem.h"
#include "FlashDataItem.h"
#include "WeatherDataQCItem.h"
#include "LocationItem.h"
#include <spine/TimeSeries.h>
#include <spine/Station.h>
#include <spine/TimeSeriesGeneratorOptions.h>

#include <jssatomic/atomic_shared_ptr.hpp>

#include <macgyver/Cache.h>

#include <string>

namespace SmartMet {
namespace Engine {
namespace Observation {

class StationInfo;
class StationtypeConfig;
class ObservableProperty;

struct ObservationCacheParameters {
  std::string cacheId;
  int connectionPoolSize;
  std::string cacheFile;
  std::size_t maxInsertSize;
  std::string synchronous;
  std::string journalMode;
  std::size_t mmapSize;
  std::string threadingMode;
  bool memstatus;
  bool sharedCache;
  int cacheTimeout;
  int cacheDuration;
  int flashCacheDuration;
  bool quiet;
  bool cacheHasStations;
  jss::atomic_shared_ptr<boost::posix_time::time_period> *cachePeriod;
  jss::atomic_shared_ptr<boost::posix_time::time_period> *qcDataPeriod;
  jss::atomic_shared_ptr<boost::posix_time::time_period> *flashPeriod;
  jss::atomic_shared_ptr<StationInfo> *stationInfo;
  std::map<std::string, std::map<std::string, std::string> > *parameterMap;
  StationtypeConfig *stationtypeConfig;
};

class ObservationCache {
public:
  virtual ~ObservationCache();

  virtual void initializeConnectionPool() = 0;

  virtual Spine::TimeSeries::TimeSeriesVectorPtr
  valuesFromCache(Settings &settings) = 0;
  virtual Spine::TimeSeries::TimeSeriesVectorPtr valuesFromCache(
      Settings &settings,
      const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions) = 0;
  virtual Spine::Stations getStationsByTaggedLocations(
      const Spine::TaggedLocationList &taggedLocations,
      const int numberofstations, const std::string &stationtype,
      const int maxdistance, const std::set<std::string> &stationgroup_codes,
      const boost::posix_time::ptime &starttime,
      const boost::posix_time::ptime &endtime) = 0;
  virtual void updateCachePeriod(const boost::posix_time::ptime &timetokeep,
                                 boost::posix_time::ptime last_time) = 0;
  virtual void updateQCDataPeriod(const boost::posix_time::ptime &timetokeep,
                                  boost::posix_time::ptime last_time) = 0;
  virtual void updateFlashPeriod(const boost::posix_time::ptime &timetokeep,
                                 boost::posix_time::ptime last_time) = 0;

  virtual bool dataAvailableInCache(const Settings &settings) const = 0;
  virtual bool
  flashIntervalIsCached(const boost::posix_time::ptime &starttime,
                        const boost::posix_time::ptime &endtime) const = 0;
  virtual void getStationsByBoundingBox(Spine::Stations &stations,
                                        const Settings &settings) const = 0;
  virtual void updateStationsAndGroups(const StationInfo &info) const = 0;

  virtual int getCacheDuration() const = 0;
  virtual int getFlashCacheDuration() const = 0;

  virtual Spine::Stations
  findAllStationsFromGroups(const std::set<std::string> stationgroup_codes,
                            const StationInfo &info,
                            const boost::posix_time::ptime &starttime,
                            const boost::posix_time::ptime &endtime) const = 0;
  virtual bool
  getStationById(Spine::Station &station, int station_id,
                 const std::set<std::string> &stationgroup_codes) const = 0;
  virtual Spine::Stations
  findStationsInsideArea(const Settings &settings, const std::string &areaWkt,
                         const StationInfo &info) const = 0;
  virtual FlashCounts
  getFlashCount(const boost::posix_time::ptime &starttime,
                const boost::posix_time::ptime &endtime,
                const Spine::TaggedLocationList &locations) const = 0;
  virtual boost::posix_time::ptime getLatestFlashTime() const = 0;
  virtual void fillFlashDataCache(
      const std::vector<FlashDataItem> &flashCacheData) const = 0;
  virtual void
  cleanFlashDataCache(const boost::posix_time::ptime &timetokeep) const = 0;
  virtual boost::posix_time::ptime getLatestObservationTime() const = 0;
  virtual void fillDataCache(const std::vector<DataItem> &cacheData) const = 0;
  virtual void
  cleanDataCache(const boost::posix_time::ptime &last_time) const = 0;
  virtual boost::posix_time::ptime getLatestWeatherDataQCTime() const = 0;
  virtual void fillWeatherDataQCCache(
      const std::vector<WeatherDataQCItem> &cacheData) const = 0;
  virtual void
  cleanWeatherDataQCCache(const boost::posix_time::ptime &last_time) const = 0;
  virtual void
  fillLocationCache(const std::vector<LocationItem> &locations) const = 0;
  virtual boost::shared_ptr<std::vector<ObservableProperty> >
  observablePropertyQuery(std::vector<std::string> &parameters,
                          const std::string language) const = 0;

  virtual void shutdown() = 0;

  bool cacheHasStations() const;

protected:
  ObservationCache(boost::shared_ptr<ObservationCacheParameters> p);

  boost::shared_ptr<ObservationCacheParameters> itsParameters;
};

} // namespace Observation
} // namespace Engine
} // namespace SmartMet
