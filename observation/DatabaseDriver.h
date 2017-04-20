#pragma once

#include "MetaData.h"
#include "QueryBase.h"
#include "QueryResultBase.h"
#include "Settings.h"
#include "Utils.h"
#include <boost/date_time/posix_time/posix_time.hpp>
#include <engines/geonames/Engine.h>
#include <spine/Station.h>
#include <spine/TimeSeries.h>
#include <spine/TimeSeriesGeneratorOptions.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class ObservationCache;
class ObservableProperty;
class StationInfo;
class StationtypeConfig;

class DatabaseDriver
{
 public:
  virtual ~DatabaseDriver();
  virtual void init(Geonames::Engine *geonames) = 0;
  virtual std::string id() const = 0;

  virtual boost::shared_ptr<Spine::Table> makeQuery(
      Settings &settings, boost::shared_ptr<Spine::ValueFormatter> &valueFormatter) = 0;
  virtual void makeQuery(QueryBase *qb) = 0;
  virtual Spine::TimeSeries::TimeSeriesVectorPtr values(Settings &settings) = 0;
  virtual Spine::TimeSeries::TimeSeriesVectorPtr values(
      Settings &settings, const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions) = 0;
  virtual FlashCounts getFlashCount(const boost::posix_time::ptime &starttime,
                                    const boost::posix_time::ptime &endtime,
                                    const Spine::TaggedLocationList &locations) = 0;

  virtual boost::shared_ptr<std::vector<ObservableProperty> > observablePropertyQuery(
      std::vector<std::string> &parameters, const std::string language) = 0;

  virtual void getStations(Spine::Stations &stations, Settings &settings) = 0;
  virtual void updateFlashCache() = 0;
  virtual void updateObservationCache() = 0;
  virtual void updateWeatherDataQCCache() = 0;
  virtual void locationsFromDatabase() = 0;
  virtual void preloadStations(const std::string &serializedStationsFile) = 0;
  virtual void shutdown() = 0;
  virtual MetaData metaData(const std::string &producer) = 0;

 protected:
  DatabaseDriver() {}

 private:
  // Pointer to dymanically loaded database driver
  void *itsHandle = nullptr;
  friend class DatabaseDriverFactory;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
