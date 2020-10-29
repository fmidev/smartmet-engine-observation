#pragma once

#include "DatabaseDriverBase.h"
#include "ObservationCacheAdminSpatiaLite.h"
#include "Engine.h"
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
  ~SpatiaLiteDatabaseDriver() = default;

  SpatiaLiteDatabaseDriver(const std::string &name,
						   const EngineParametersPtr &p,
						   Spine::ConfigBase &cfg);

  void init(Engine *obsengine);
  std::string id() const;
  void makeQuery(QueryBase *qb);

  Spine::TimeSeries::TimeSeriesVectorPtr values(Settings &settings);

  Spine::TimeSeries::TimeSeriesVectorPtr values(
      Settings &settings,
      const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions);

  boost::shared_ptr<std::vector<ObservableProperty>>
  observablePropertyQuery(std::vector<std::string> &parameters, const std::string language);

  FlashCounts getFlashCount(
      const boost::posix_time::ptime &starttime,
      const boost::posix_time::ptime &endtime,
      const Spine::TaggedLocationList &locations) const;

  void shutdown();

 private:
  void readConfig(Spine::ConfigBase &cfg);

  DatabaseDriverParameters itsParameters;
  boost::shared_ptr<ObservationCacheAdminSpatiaLite> itsObservationCacheAdminSpatiaLite;

};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
