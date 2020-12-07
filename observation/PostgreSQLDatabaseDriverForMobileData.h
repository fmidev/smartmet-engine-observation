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
  ~PostgreSQLDatabaseDriverForMobileData() = default;

  PostgreSQLDatabaseDriverForMobileData(const std::string &name,
                                        const EngineParametersPtr &p,
                                        Spine::ConfigBase &cfg);

  void init(Engine *obsengine);
  std::string id() const;
  void makeQuery(QueryBase *qb);

  Spine::TimeSeries::TimeSeriesVectorPtr values(Settings &settings);

  Spine::TimeSeries::TimeSeriesVectorPtr values(
      Settings &settings, const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions);

  std::shared_ptr<std::vector<ObservableProperty> > observablePropertyQuery(
      std::vector<std::string> &parameters, const std::string language);
  void getStations(Spine::Stations &stations, const Settings &settings) const;
  void getStationsByArea(Spine::Stations &stations,
                         const std::string &stationtype,
                         const boost::posix_time::ptime &starttime,
                         const boost::posix_time::ptime &endtime,
                         const std::string &wkt) const;
  void getStationsByBoundingBox(Spine::Stations &stations, const Settings &settings) const;

 private:
  void setSettings(Settings &settings, PostgreSQLObsDB &db);

  void readConfig(Spine::ConfigBase &cfg);
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
