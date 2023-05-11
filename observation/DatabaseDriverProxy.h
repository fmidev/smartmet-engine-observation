#pragma once

#include "DatabaseDriverBase.h"
#include "DatabaseDriverContainer.h"
#include "DatabaseDriverInterface.h"
#include "Engine.h"
#include <macgyver/AsyncTaskGroup.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class PostgreSQLDatabaseDriverForMobileData;
class OracleDatabaseDriver;

class DatabaseDriverProxy : public DatabaseDriverInterface
{
 public:
  DatabaseDriverProxy(const EngineParametersPtr &p, Spine::ConfigBase &cfg);
  ~DatabaseDriverProxy() override;

  void init(Engine *obsengine) override;
  TS::TimeSeriesVectorPtr values(Settings &settings) override;
  TS::TimeSeriesVectorPtr values(Settings &settings,
                                 const TS::TimeSeriesGeneratorOptions &timeSeriesOptions) override;
  Spine::TaggedFMISIDList translateToFMISID(const boost::posix_time::ptime &starttime,
                                            const boost::posix_time::ptime &endtime,
                                            const std::string &stationtype,
                                            const StationSettings &stationSettings) const override;
  void makeQuery(QueryBase *qb) override;
  FlashCounts getFlashCount(const boost::posix_time::ptime &starttime,
                            const boost::posix_time::ptime &endtime,
                            const Spine::TaggedLocationList &locations) const override;
  std::shared_ptr<std::vector<ObservableProperty>> observablePropertyQuery(
      std::vector<std::string> &parameters, const std::string language) override;

  void reloadStations() override;
  void getStations(Spine::Stations &stations, const Settings &settings) const override;
  void getStationsByArea(Spine::Stations &stations,
                         const std::string &stationtype,
                         const boost::posix_time::ptime &starttime,
                         const boost::posix_time::ptime &endtime,
                         const std::string &wkt) const override;
  void getStationsByBoundingBox(Spine::Stations &stations, const Settings &settings) const override;

  DatabaseDriverBase *resolveDatabaseDriver(const Settings &settings) const;
  DatabaseDriverBase *resolveDatabaseDriverByProducer(const std::string &producer) const;
  DatabaseDriverBase *resolveDatabaseDriverByTable(const std::string &table) const;

  void shutdown() override;
  MetaData metaData(const std::string &producer) const override;
  std::string id() const override;
  std::string name() const override;

  Fmi::Cache::CacheStatistics getCacheStats() const override;
  void getStationGroups(StationGroups &sg) const override;
  void getProducerGroups(ProducerGroups &pg) const override;
  MeasurandInfo getMeasurandInfo() const override;

 private:
  const StationtypeConfig &itsStationtypeConfig;
  DatabaseDriverContainer itsDatabaseDriverContainer;
  std::set<DatabaseDriverBase *> itsDatabaseDriverSet;
  PostgreSQLDatabaseDriverForMobileData *itsPostgreSQLMobileDataDriver{nullptr};
  DatabaseDriverBase *itsOracleDriver{nullptr};
  DatabaseDriverBase *itsStationsDriver{nullptr};
  DatabaseDriverBase *itsTranslateToFMISIDDriver{nullptr};
  DatabaseDriverBase *createOracleDriver(const std::string &driver_id,
                                         const EngineParametersPtr &p,
                                         Spine::ConfigBase &cfg) const;
  Fmi::AsyncTaskGroup init_tasks;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
