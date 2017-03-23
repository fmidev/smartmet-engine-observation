#pragma once

#include "DatabaseDriver.h"

namespace SmartMet {
namespace Engine {
namespace Observation {
class DummyDatabaseDriver : public DatabaseDriver {
public:
  DummyDatabaseDriver(boost::shared_ptr<DatabaseDriverParameters> p);
  void initializeConnectionPool();

  boost::shared_ptr<Spine::Table>
  makeQuery(Settings &settings,
            boost::shared_ptr<Spine::ValueFormatter> &valueFormatter);
  void makeQuery(QueryBase *qb) {}
  Spine::TimeSeries::TimeSeriesVectorPtr values(Settings &settings);
  Spine::TimeSeries::TimeSeriesVectorPtr
  values(Settings &settings,
         const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions);
  FlashCounts getFlashCount(const boost::posix_time::ptime &starttime,
                            const boost::posix_time::ptime &endtime,
                            const Spine::TaggedLocationList &locations);
  boost::shared_ptr<std::vector<ObservableProperty> >
  observablePropertyQuery(std::vector<std::string> &parameters,
                          const std::string language);
  void getStations(Spine::Stations &stations, Settings &settings) {}
  void getStationsByBoundingBox(Spine::Stations &stations,
                                const Settings &settings) {}

  void updateFlashCache() {}
  void updateObservationCache() {}
  void updateWeatherDataQCCache() {}
  void locationsFromDatabase() {}
  void preloadStations(const std::string &serializedStationsFile) {}
  void shutdown() {}
  std::string id() const { return "dummy"; }
};

} // namespace Observation
} // namespace Engine
} // namespace SmartMet
