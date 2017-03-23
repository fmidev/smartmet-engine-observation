#pragma once

#include "Settings.h"
#include "Utils.h"
#include "QueryBase.h"
#include "QueryResultBase.h"
#include <engines/geonames/Engine.h>
#include <spine/Station.h>
#include <spine/TimeSeries.h>
#include <spine/TimeSeriesGeneratorOptions.h>

namespace SmartMet {
namespace Engine {
namespace Observation {
class ObservationCache;
class ObservableProperty;
class StationInfo;
class StationtypeConfig;
class DatabaseDriverParameters;

class DatabaseDriver {
public:
  virtual ~DatabaseDriver();
  virtual std::string id() const = 0;
  virtual void initializeConnectionPool() = 0;

  virtual boost::shared_ptr<Spine::Table>
  makeQuery(Settings &settings,
            boost::shared_ptr<Spine::ValueFormatter> &valueFormatter) = 0;
  virtual void makeQuery(QueryBase *qb) = 0;
  virtual Spine::TimeSeries::TimeSeriesVectorPtr values(Settings &settings) = 0;
  virtual Spine::TimeSeries::TimeSeriesVectorPtr
  values(Settings &settings,
         const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions) = 0;
  virtual FlashCounts
  getFlashCount(const boost::posix_time::ptime &starttime,
                const boost::posix_time::ptime &endtime,
                const Spine::TaggedLocationList &locations) = 0;

  virtual boost::shared_ptr<std::vector<ObservableProperty> >
  observablePropertyQuery(std::vector<std::string> &parameters,
                          const std::string language) = 0;

  virtual void getStations(Spine::Stations &stations, Settings &settings) = 0;
  virtual void updateFlashCache() = 0;
  virtual void updateObservationCache() = 0;
  virtual void updateWeatherDataQCCache() = 0;
  virtual void locationsFromDatabase() = 0;
  virtual void preloadStations(const std::string &serializedStationsFile) = 0;
  virtual void shutdown() = 0;

protected:
  DatabaseDriver(boost::shared_ptr<DatabaseDriverParameters> p)
      : itsDriverParameters(p) {}

  boost::shared_ptr<DatabaseDriverParameters> itsDriverParameters;
};

} // namespace Observation
} // namespace Engine
} // namespace SmartMet
