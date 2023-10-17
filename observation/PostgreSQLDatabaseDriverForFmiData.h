#pragma once

#include "Engine.h"
#include "PostgreSQLDatabaseDriver.h"
#include "PostgreSQLObsDB.h"
#include <memory>
#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class PostgreSQLDatabaseDriverForFmiData : public PostgreSQLDatabaseDriver
{
 public:
  ~PostgreSQLDatabaseDriverForFmiData() override = default;

  PostgreSQLDatabaseDriverForFmiData(const std::string &name,
                                     const EngineParametersPtr &p,
                                     Spine::ConfigBase &cfg);

  PostgreSQLDatabaseDriverForFmiData() = delete;
  PostgreSQLDatabaseDriverForFmiData(const PostgreSQLDatabaseDriverForFmiData &other) = delete;
  PostgreSQLDatabaseDriverForFmiData(PostgreSQLDatabaseDriverForFmiData &&other) = delete;
  PostgreSQLDatabaseDriverForFmiData &operator=(const PostgreSQLDatabaseDriverForFmiData &other) =
      delete;
  PostgreSQLDatabaseDriverForFmiData &operator=(PostgreSQLDatabaseDriverForFmiData &&other) =
      delete;

  void init(Engine *obsengine) override;
  std::string id() const override;
  void makeQuery(QueryBase *qb) override;

  void getStationGroups(StationGroups &sg) const override;
  void getProducerGroups(ProducerGroups &pg) const override;

  TS::TimeSeriesVectorPtr values(Settings &settings) override;

  TS::TimeSeriesVectorPtr values(Settings &settings,
                                 const TS::TimeSeriesGeneratorOptions &timeSeriesOptions) override;

  std::shared_ptr<std::vector<ObservableProperty>> observablePropertyQuery(
      std::vector<std::string> &parameters, const std::string &language) override;
  void getStations(Spine::Stations &stations, const Settings &settings) const override;
  void getStationsByArea(Spine::Stations &stations,
                         const Settings &settings,
                         const std::string &wkt) const override;
  void getMovingStationsByArea(Spine::Stations &stations,
                               const Settings &settings,
                               const std::string &wkt) const override;
  void getStationsByBoundingBox(Spine::Stations &stations, const Settings &settings) const override;
  FlashCounts getFlashCount(const boost::posix_time::ptime &starttime,
                            const boost::posix_time::ptime &endtime,
                            const Spine::TaggedLocationList &locations) const override;
  MeasurandInfo getMeasurandInfo() const override;
  boost::posix_time::ptime getLatestDataUpdateTime(
      const std::string &producer,
      const boost::posix_time::ptime &from,
      const MeasurandInfo &measurand_info) const override;

 private:
  void readConfig(Spine::ConfigBase &cfg);
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
