#pragma once

#include "DatabaseDriverBase.h"
#include "DatabaseDriverContainer.h"
#include "DatabaseDriverInterface.h"
#include "Engine.h"

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
  ~DatabaseDriverProxy();

  void init(Engine *obsengine);
  Spine::TimeSeries::TimeSeriesVectorPtr values(Settings &settings);
  Spine::TimeSeries::TimeSeriesVectorPtr values(
      Settings &settings, const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions);
  Spine::TaggedFMISIDList translateToFMISID(const boost::posix_time::ptime &starttime,
                                            const boost::posix_time::ptime &endtime,
                                            const std::string &stationtype,
                                            const StationSettings &stationSettings) const;
  void makeQuery(QueryBase *qb);
  FlashCounts getFlashCount(const boost::posix_time::ptime &starttime,
                            const boost::posix_time::ptime &endtime,
                            const Spine::TaggedLocationList &locations) const;
  std::shared_ptr<std::vector<ObservableProperty>> observablePropertyQuery(
      std::vector<std::string> &parameters, const std::string language);

  void reloadStations();
  void getStations(Spine::Stations &stations, const Settings &settings) const;
  void getStationsByArea(Spine::Stations &stations,
                         const std::string &stationtype,
                         const boost::posix_time::ptime &starttime,
                         const boost::posix_time::ptime &endtime,
                         const std::string &wkt) const;
  void getStationsByBoundingBox(Spine::Stations &stations, const Settings &settings) const;

  DatabaseDriverBase *resolveDatabaseDriver(const Settings &settings) const;
  DatabaseDriverBase *resolveDatabaseDriverByProducer(const std::string &producer) const;
  DatabaseDriverBase *resolveDatabaseDriverByTable(const std::string &table) const;

  void shutdown();
  MetaData metaData(const std::string &producer) const;
  std::string id() const;
  std::string name() const;

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
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
