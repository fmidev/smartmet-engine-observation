#pragma once

#include "DatabaseDriverParameters.h"
#include "DatabaseStations.h"
#include "FmiIoTStation.h"
#include <boost/atomic.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/thread/condition.hpp>
#include "Engine.h"
#include "MetaData.h"
#include "ObservableProperty.h"
#include "ObservationCache.h"
#include "QueryBase.h"
#include "QueryResultBase.h"
#include "Settings.h"
#include "StationSettings.h"
#include "Utils.h"
#include <spine/Station.h>
#include <spine/TimeSeries.h>
#include <spine/TimeSeriesGeneratorOptions.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{

class DatabaseDriverBase
{
 public:
  DatabaseDriverBase(const std::string &name) : itsDriverName(name) {}

  virtual ~DatabaseDriverBase();
  virtual void init(SmartMet::Engine::Observation::Engine *obsengine) = 0;
  virtual std::string id() const = 0;

  virtual void makeQuery(SmartMet::Engine::Observation::QueryBase *qb) = 0;
  virtual Spine::TimeSeries::TimeSeriesVectorPtr values(Settings &settings) = 0;
  virtual Spine::TimeSeries::TimeSeriesVectorPtr values(Settings &settings, const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions) = 0;
  Spine::TaggedFMISIDList translateToFMISID(
      const boost::posix_time::ptime &starttime,
      const boost::posix_time::ptime &endtime,
      const std::string &stationtype,
      const StationSettings &stationSettings) const;
  void getStationsByArea(Spine::Stations &stations,
                         const std::string &stationtype,
                         const boost::posix_time::ptime &starttime,
                         const boost::posix_time::ptime &endtime,
                         const std::string &wkt) const;
  void getStationsByBoundingBox(Spine::Stations &stations,
                                const Settings &settings) const;

  void getStations(Spine::Stations &stations, const Settings &settings) const;

  virtual FlashCounts getFlashCount(
      const boost::posix_time::ptime &starttime,
      const boost::posix_time::ptime &endtime,
      const Spine::TaggedLocationList &locations) const
  {
    return FlashCounts();
  }

  virtual boost::shared_ptr<std::vector<ObservableProperty>>
  observablePropertyQuery(std::vector<std::string> &parameters, const std::string language) = 0;

  virtual void shutdown() = 0;

  const std::set<std::string> &supportedProducers() const { return itsSupportedProducers; }
  virtual void getFMIIoTStations(boost::shared_ptr<FmiIoTStations> &stations) const {}
  MetaData metaData(const std::string &producer) const;
  const std::string &name() const { return itsDriverName; }

  bool responsibleForLoadingStations() const { return itsLoadStations; }
  virtual void reloadStations() {}
  static std::string resolveDatabaseTableName(
      const std::string &producer, const StationtypeConfig &stationtypeConfig);

 protected:
  bool isParameter(const std::string &alias,
                   const std::string &stationType,
                   const ParameterMap &parameterMap) const;
  bool isParameterVariant(const std::string &name,
                          const ParameterMap &parameterMap) const;

  void parameterSanityCheck(const std::string &stationtype,
                            const std::vector<Spine::Parameter> &parameters,
                            const ParameterMap &parameterMap) const;
  void updateProducers(const EngineParametersPtr &p,
                       Settings &settings) const;
  void readConfig(Spine::ConfigBase &cfg, DatabaseDriverParameters &parameters);
  void readMetaData(Spine::ConfigBase &cfg);
  std::string resolveCacheTableName(
      const std::string &producer,
      const StationtypeConfig &stationtypeConfig) const;
  boost::shared_ptr<ObservationCache> resolveCache(
      const std::string &producer, const EngineParametersPtr &parameters) const;

  boost::atomic<bool> itsShutdownRequested{false};
  std::set<std::string> itsSupportedProducers;
  std::string itsDriverName;
  std::map<std::string, MetaData> itsMetaData;
  bool itsTimer{false};
  Fmi::TimeZones itsTimeZones;
  boost::atomic<bool> itsConnectionsOK{false};
  bool itsQuiet{true};
  std::unique_ptr<DatabaseStations> itsDatabaseStations{nullptr};
  bool itsLoadStations{false};
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet