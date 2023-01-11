#pragma once

#include "DatabaseDriverBase.h"
#include "Engine.h"
#include "ObservationCacheAdminSpatiaLite.h"
#include <boost/smart_ptr/atomic_shared_ptr.hpp>
#include <memory>
#include <string>

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

  TS::TimeSeriesVectorPtr values(Settings &settings,
                                 const TS::TimeSeriesGeneratorOptions &timeSeriesOptions) override;

  std::shared_ptr<std::vector<ObservableProperty>> observablePropertyQuery(
      std::vector<std::string> &parameters, const std::string language) override;

  void getMovingStationsByArea(Spine::Stations &stations,
							   const std::string &stationtype,
							   const boost::posix_time::ptime &starttime,
							   const boost::posix_time::ptime &endtime,
							   const std::string &wkt) const override;

  FlashCounts getFlashCount(const boost::posix_time::ptime &starttime,
                            const boost::posix_time::ptime &endtime,
                            const Spine::TaggedLocationList &locations) const override;

  void shutdown() override;

 private:
  void readConfig(Spine::ConfigBase &cfg);

  DatabaseDriverParameters itsParameters;
  boost::atomic_shared_ptr<ObservationCacheAdminSpatiaLite> itsObservationCacheAdminSpatiaLite;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
