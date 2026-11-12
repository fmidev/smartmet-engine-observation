#pragma once

#include "DatabaseDriverBase.h"
#include "DatabaseDriverParameters.h"
#include "EngineParameters.h"
#include "Sensors.h"
#include <macgyver/AtomicSharedPtr.h>
#include <macgyver/DateTime.h>
#include <spine/Location.h>
#include <spine/Station.h>
#include <timeseries/TimeSeries.h>
#include <timeseries/TimeSeriesInclude.h>
#include <set>
#include <string>

namespace SmartMet::Engine::Observation
{
class ObservationCacheAdminSpatiaLite;
}  // namespace SmartMet::Engine::Observation
namespace SmartMet::Spine
{
class ConfigBase;
}  // namespace SmartMet::Spine

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class SpatiaLiteDatabaseDriver : public DatabaseDriverBase
{
 public:
  ~SpatiaLiteDatabaseDriver() override = default;

  SpatiaLiteDatabaseDriver(const std::string &name,
                           const EngineParametersPtr &p,
                           Spine::ConfigBase &cfg);

  void init(Engine *obsengine) override;
  std::string id() const override;
  void makeQuery(QueryBase *qb) override;

  TS::TimeSeriesVectorPtr values(Settings &settings) override;

  // The sensors found in the cached observation data (not virtual, see Engine::getSensors)
  SensorResult getSensors(const SensorOptions &options,
                          const std::set<int> &measurands,
                          const std::set<int> &producers) const;

  TS::TimeSeriesVectorPtr values(Settings &settings,
                                 const TS::TimeSeriesGeneratorOptions &timeSeriesOptions) override;

  std::shared_ptr<std::vector<ObservableProperty>> observablePropertyQuery(
      std::vector<std::string> &parameters, const std::string &language) override;

  void getMovingStationsByArea(Spine::Stations &stations,
                               const Settings &settings,
                               const std::string &wkt) const override;

  FlashCounts getFlashCount(const Fmi::DateTime &starttime,
                            const Fmi::DateTime &endtime,
                            const Spine::TaggedLocationList &locations) const override;

  void shutdown() override;

 private:
  void readConfig(Spine::ConfigBase &cfg);

  DatabaseDriverParameters itsParameters;
  Fmi::AtomicSharedPtr<ObservationCacheAdminSpatiaLite> itsObservationCacheAdminSpatiaLite;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
