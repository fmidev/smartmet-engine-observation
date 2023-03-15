#pragma once

#include "DatabaseDriverParameters.h"
#include "DatabaseStations.h"
#include "Engine.h"
#include "FmiIoTStation.h"
#include "Keywords.h"
#include "MetaData.h"
#include "ObservableProperty.h"
#include "ObservationCache.h"
#include "ProducerGroups.h"
#include "QueryBase.h"
#include "QueryResultBase.h"
#include "Settings.h"
#include "StationGroups.h"
#include "StationSettings.h"
#include "Utils.h"
#include <boost/atomic.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/thread/condition.hpp>
#include <spine/Station.h>
#include <timeseries/TimeSeriesInclude.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class DatabaseDriverBase
{
 public:
  DatabaseDriverBase(std::string name) : itsDriverName(std::move(name)) {}

  DatabaseDriverBase() = delete;
  DatabaseDriverBase(const DatabaseDriverBase &other) = delete;
  DatabaseDriverBase(DatabaseDriverBase &&other) = delete;
  DatabaseDriverBase &operator=(const DatabaseDriverBase &other) = delete;
  DatabaseDriverBase &operator=(DatabaseDriverBase &&other) = delete;

  virtual ~DatabaseDriverBase();
  virtual void init(SmartMet::Engine::Observation::Engine *obsengine) = 0;
  virtual std::string id() const = 0;

  virtual void makeQuery(SmartMet::Engine::Observation::QueryBase *qb) = 0;
  virtual TS::TimeSeriesVectorPtr values(Settings &settings) = 0;
  virtual TS::TimeSeriesVectorPtr values(
      Settings &settings, const TS::TimeSeriesGeneratorOptions &timeSeriesOptions) = 0;
  virtual void getMovingStationsByArea(Spine::Stations &stations,
                                       const std::string &stationtype,
                                       const boost::posix_time::ptime &starttime,
                                       const boost::posix_time::ptime &endtime,
                                       const std::string &wkt) const = 0;
  virtual Spine::TaggedFMISIDList translateToFMISID(const boost::posix_time::ptime &starttime,
                                                    const boost::posix_time::ptime &endtime,
                                                    const std::string &stationtype,
                                                    const StationSettings &stationSettings) const;
  virtual void getStationsByArea(Spine::Stations &stations,
                                 const std::string &stationtype,
                                 const boost::posix_time::ptime &starttime,
                                 const boost::posix_time::ptime &endtime,
                                 const std::string &wkt) const;
  virtual void getStationsByBoundingBox(Spine::Stations &stations, const Settings &settings) const;

  virtual void getStations(Spine::Stations &stations, const Settings &settings) const;

  virtual FlashCounts getFlashCount(const boost::posix_time::ptime & /* starttime */,
                                    const boost::posix_time::ptime & /* endtime */,
                                    const Spine::TaggedLocationList & /* locations */) const
  {
    return {};
  }

  virtual std::shared_ptr<std::vector<ObservableProperty>> observablePropertyQuery(
      std::vector<std::string> &parameters, const std::string language) = 0;

  virtual void getStationGroups(StationGroups &sg) const {}
  virtual void getProducerGroups(ProducerGroups &pg) const {}

  virtual void shutdown() = 0;

  const std::set<std::string> &supportedProducers() const { return itsSupportedProducers; }
  virtual void getFMIIoTStations(std::shared_ptr<FmiIoTStations> &stations) const {}
  MetaData metaData(const std::string &producer) const;
  const std::string &name() const { return itsDriverName; }

  bool responsibleForLoadingStations() const { return itsLoadStations; }
  virtual void reloadStations() {}
  static std::string resolveDatabaseTableName(const std::string &producer,
                                              const StationtypeConfig &stationtypeConfig);
  TS::TimeSeriesVectorPtr checkForEmptyQuery(Settings &settings) const;
  TS::TimeSeriesVectorPtr checkForEmptyQuery(
      Settings &settings, const TS::TimeSeriesGeneratorOptions &timeSeriesOptions) const;

  virtual Fmi::Cache::CacheStatistics getCacheStats() const
  {
    return Fmi::Cache::CacheStatistics();
  }

 protected:
  void parameterSanityCheck(const std::string &stationtype,
                            const std::vector<Spine::Parameter> &parameters,
                            const ParameterMap &parameterMap) const;
  void updateProducers(const EngineParametersPtr &p, Settings &settings) const;
  void readConfig(Spine::ConfigBase &cfg, DatabaseDriverParameters &parameters);
  void readMetaData(Spine::ConfigBase &cfg);
  std::string resolveCacheTableName(const std::string &producer,
                                    const StationtypeConfig &stationtypeConfig) const;
  std::shared_ptr<ObservationCache> resolveCache(const std::string &producer,
                                                 const EngineParametersPtr &parameters) const;

  std::set<std::string> itsSupportedProducers;
  std::string itsDriverName;
  std::map<std::string, MetaData> itsMetaData;
  bool itsTimer{false};
  Fmi::TimeZones itsTimeZones;
  std::atomic<bool> itsConnectionsOK{false};
  bool itsQuiet{true};
  std::unique_ptr<DatabaseStations> itsDatabaseStations;
  bool itsLoadStations{false};
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
