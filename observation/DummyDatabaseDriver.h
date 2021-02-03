#pragma once

#include "DatabaseDriverBase.h"
#include "EngineParameters.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class Engine;
struct ObservableProperty;

class DummyDatabaseDriver : public DatabaseDriverBase
{
 public:
  DummyDatabaseDriver(const std::string &name, const EngineParametersPtr &p) : DatabaseDriverBase(name), itsParameters(p) {}

  void init(Engine *obsengine);
  Spine::TimeSeries::TimeSeriesVectorPtr values(Settings &settings);
  Spine::TimeSeries::TimeSeriesVectorPtr values(
      Settings &settings, const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions);
  Spine::TaggedFMISIDList translateToFMISID(const boost::posix_time::ptime &starttime,
                                            const boost::posix_time::ptime &endtime,
                                            const std::string &stationtype,
                                            const StationSettings &stationSettings) const;
  void makeQuery(QueryBase *) {}
  FlashCounts getFlashCount(const boost::posix_time::ptime &starttime,
                            const boost::posix_time::ptime &endtime,
                            const Spine::TaggedLocationList &locations) const;
  std::shared_ptr<std::vector<ObservableProperty>> observablePropertyQuery(
      std::vector<std::string> &parameters, const std::string language);
  void reloadStations() {}
  void getStations(Spine::Stations &stations, const Settings &settings) const {}
  void getStationsByArea(Spine::Stations &stations,
						 const std::string &stationtype,
						 const boost::posix_time::ptime &starttime,
						 const boost::posix_time::ptime &endtime,
						 const std::string &wkt) const
  {
  }
  void getStationsByBoundingBox(Spine::Stations &stations, const Settings &settings) const {}

  void shutdown() {}
  MetaData metaData(const std::string &) const { return MetaData(); }
  std::string id() const { return "dummy"; }
  std::string name() const { return "dummy"; }

 private:
  const EngineParametersPtr &itsParameters;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
