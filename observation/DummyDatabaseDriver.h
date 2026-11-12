#pragma once

#include "DatabaseDriverBase.h"
#include "EngineParameters.h"
#include <macgyver/DateTime.h>
#include <spine/Location.h>
#include <spine/Station.h>
#include <timeseries/TimeSeries.h>
#include <timeseries/TimeSeriesInclude.h>
#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{

class DummyDatabaseDriver : public DatabaseDriverBase
{
 public:
  DummyDatabaseDriver(const std::string& name, const EngineParametersPtr& /*unused*/)
      : DatabaseDriverBase(name)
  {
  }

  void init(Engine *obsengine) override;
  TS::TimeSeriesVectorPtr values(Settings &settings) override;
  TS::TimeSeriesVectorPtr values(Settings &settings,
                                 const TS::TimeSeriesGeneratorOptions &timeSeriesOptions) override;
  Spine::TaggedFMISIDList translateToFMISID(const Settings &settings,
                                            const StationSettings &stationSettings) const override;
  void getMovingStationsByArea(Spine::Stations &stations,
                               const Settings &settings,
                               const std::string &wkt) const override;
  void makeQuery(QueryBase* /*qb*/) override {}
  FlashCounts getFlashCount(const Fmi::DateTime &starttime,
                            const Fmi::DateTime &endtime,
                            const Spine::TaggedLocationList &locations) const override;
  std::shared_ptr<std::vector<ObservableProperty>> observablePropertyQuery(
      std::vector<std::string> &parameters, const std::string &language) override;
  void reloadStations() override {}
  void getStations(Spine::Stations &stations, const Settings &settings) const override {}
  void getStationsByArea(Spine::Stations &stations,
                         const Settings &settings,
                         const std::string &wkt) const override
  {
  }
  void getStationsByBoundingBox(Spine::Stations &stations, const Settings &settings) const override
  {
  }

  void shutdown() override {}
  std::string id() const override { return "dummy"; }
  std::string name() const { return "dummy"; }
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
