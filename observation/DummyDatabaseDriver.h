#pragma once

#include "DatabaseDriverInterface.h"
#include "EngineParameters.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class Engine;
struct ObservableProperty;

class DummyDatabaseDriver : public DatabaseDriverInterface
{
 public:
  DummyDatabaseDriver(const EngineParametersPtr &p) : itsParameters(p) {}

  void init(Engine *obsengine);
  Spine::TimeSeries::TimeSeriesVectorPtr values(Settings &settings);
  Spine::TimeSeries::TimeSeriesVectorPtr values(
      Settings &settings, const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions);
  boost::shared_ptr<Spine::Table> makeQuery(
      Settings &settings, boost::shared_ptr<Spine::ValueFormatter> &valueFormatter);
  void makeQuery(QueryBase *) {}
  FlashCounts getFlashCount(const boost::posix_time::ptime &starttime,
                            const boost::posix_time::ptime &endtime,
                            const Spine::TaggedLocationList &locations);
  boost::shared_ptr<std::vector<ObservableProperty> > observablePropertyQuery(
      std::vector<std::string> &parameters, const std::string language);
  void getStations(Spine::Stations &, Settings &) {}

  void shutdown() {}
  MetaData metaData(const std::string &) { return MetaData(); }
  std::string id() const { return "dummy"; }

 private:
  const EngineParametersPtr &itsParameters;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
