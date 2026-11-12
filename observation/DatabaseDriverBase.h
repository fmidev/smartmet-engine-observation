#pragma once

#include "DatabaseStations.h"  // IWYU pragma: keep (unique_ptr member, implicit destructor)
#include "EngineParameters.h"
#include "MeasurandInfo.h"
#include "MetaData.h"
#include "ObservableProperty.h"
#include "ObservationCache.h"
#include "Utils.h"
#include <macgyver/CacheStats.h>
#include <macgyver/DateTime.h>
#include <macgyver/TimeZones.h>
#include <spine/Location.h>
#include <spine/Station.h>
#include <timeseries/TimeSeries.h>
#include <timeseries/TimeSeriesGeneratorOptions.h>
#include <timeseries/TimeSeriesInclude.h>
#include <atomic>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace SmartMet::Engine::Observation
{
class Engine;
class FmiIoTStations;
class ParameterMap;
class ProducerGroups;
class QueryBase;
class Settings;
class StationGroups;
class StationtypeConfig;
struct DatabaseDriverParameters;
struct StationSettings;
}  // namespace SmartMet::Engine::Observation
namespace SmartMet::Spine
{
class ConfigBase;
class Parameter;
}  // namespace SmartMet::Spine

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
  DatabaseDriverBase(const DatabaseDriverBase& other) = delete;
  DatabaseDriverBase(DatabaseDriverBase&& other) = delete;
  DatabaseDriverBase& operator=(const DatabaseDriverBase& other) = delete;
  DatabaseDriverBase& operator=(DatabaseDriverBase&& other) = delete;

  virtual ~DatabaseDriverBase();
  virtual void init(SmartMet::Engine::Observation::Engine* obsengine) = 0;
  virtual std::string id() const = 0;

  virtual void makeQuery(SmartMet::Engine::Observation::QueryBase* qb) = 0;
  virtual TS::TimeSeriesVectorPtr values(Settings& settings) = 0;
  virtual TS::TimeSeriesVectorPtr values(
      Settings& settings, const TS::TimeSeriesGeneratorOptions& timeSeriesOptions) = 0;
  virtual void getMovingStationsByArea(Spine::Stations& stations,
                                       const Settings& settings,
                                       const std::string& wkt) const = 0;
  virtual Spine::TaggedFMISIDList translateToFMISID(const Settings& settings,
                                                    const StationSettings& stationSettings) const;
  virtual void getStationsByArea(Spine::Stations& stations,
                                 const Settings& settings,
                                 const std::string& wkt) const;
  virtual void getStationsByBoundingBox(Spine::Stations& stations, const Settings& settings) const;

  virtual void getStations(Spine::Stations& stations, const Settings& settings) const;

  virtual FlashCounts getFlashCount(const Fmi::DateTime& /* starttime */,
                                    const Fmi::DateTime& /* endtime */,
                                    const Spine::TaggedLocationList& /* locations */) const
  {
    return {};
  }

  virtual std::shared_ptr<std::vector<ObservableProperty>> observablePropertyQuery(
      std::vector<std::string>& parameters, const std::string& language) = 0;

  virtual void getStationGroups(StationGroups& sg) const {}
  virtual void getProducerGroups(ProducerGroups& pg) const {}

  virtual void shutdown() = 0;

  virtual Fmi::DateTime getLatestDataUpdateTime(const std::string& producer,
                                                const Fmi::DateTime& from,
                                                const MeasurandInfo& measurand_info) const;

  const std::set<std::string>& supportedProducers() const { return itsSupportedProducers; }
  virtual void getFMIIoTStations(std::shared_ptr<FmiIoTStations>& stations) const {}
  virtual MetaData metaData(const std::string& producer, const Settings& settings);
  virtual MetaData metaData(const std::string& /* producer */,
                            const Settings& /*settings */,
                            MetaData& md)
  {
    return md;
  }
  const std::string& name() const { return itsDriverName; }

  bool responsibleForLoadingStations() const { return itsLoadStations; }
  virtual void reloadStations() {}
  static std::string resolveDatabaseTableName(const std::string& producer,
                                              const StationtypeConfig& stationtypeConfig);
  static std::string resolveCacheTableName(const std::string& producer,
                                           const StationtypeConfig& stationtypeConfig);
  TS::TimeSeriesVectorPtr checkForEmptyQuery(const Settings& settings) const;
  TS::TimeSeriesVectorPtr checkForEmptyQuery(
      const Settings& settings, const TS::TimeSeriesGeneratorOptions& timeSeriesOptions) const;

  virtual Fmi::Cache::CacheStatistics getCacheStats() const;

  virtual MeasurandInfo getMeasurandInfo() const { return {}; }

 protected:
  void readConfig(Spine::ConfigBase& cfg, DatabaseDriverParameters& parameters);
  void readMetaData(Spine::ConfigBase& cfg);
  std::shared_ptr<ObservationCache> resolveCache(const std::string& producer,
                                                 const EngineParametersPtr& parameters) const;
  void getMeasurandAndProducerIds(const std::string& producer,
                                  const MeasurandInfo& minfo,
                                  const EngineParametersPtr& ep,
                                  std::string& producerIds,
                                  std::string& measurandIds) const;

  static void parameterSanityCheck(const std::string& stationtype,
                                   const std::vector<Spine::Parameter>& parameters,
                                   const ParameterMap& parameterMap);
  static void updateProducers(const EngineParametersPtr& p, Settings& settings);

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
