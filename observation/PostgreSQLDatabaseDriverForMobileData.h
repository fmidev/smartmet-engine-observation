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
class PostgreSQLDatabaseDriverForMobileData : public PostgreSQLDatabaseDriver
{
 public:
  ~PostgreSQLDatabaseDriverForMobileData() override = default;

  PostgreSQLDatabaseDriverForMobileData(const std::string &name,
                                        const EngineParametersPtr &p,
                                        Spine::ConfigBase &cfg);

  void init(Engine *obsengine) override;
  std::string id() const override;
  void makeQuery(QueryBase *qb) override;

  TS::TimeSeriesVectorPtr values(Settings &settings) override;

  TS::TimeSeriesVectorPtr values(Settings &settings,
                                 const TS::TimeSeriesGeneratorOptions &timeSeriesOptions) override;

  std::shared_ptr<std::vector<ObservableProperty> > observablePropertyQuery(
      std::vector<std::string> &parameters, const std::string &language) override;
  void getStations(Spine::Stations &stations, const Settings &settings) const override;
  void getStationsByArea(Spine::Stations &stations,
                         const Settings &settings,
                         const std::string &wkt) const override;
  void getMovingStationsByArea(Spine::Stations &stations,
                               const Settings &settings,
                               const std::string &wkt) const override;
  void getStationsByBoundingBox(Spine::Stations &stations, const Settings &settings) const override;
  std::shared_ptr<FmiIoTStations> &getFmiIoTStations() { return itsParameters.fmiIoTStations; }

 private:
  void readConfig(Spine::ConfigBase &cfg);
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
