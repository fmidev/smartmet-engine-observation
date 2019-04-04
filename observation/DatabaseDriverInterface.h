#pragma once

#include "MetaData.h"
#include "QueryBase.h"
#include "QueryResultBase.h"
#include "Settings.h"
#include "Utils.h"
#include <boost/atomic.hpp>
#include <boost/thread/condition.hpp>
#include <spine/Station.h>
#include <spine/TimeSeries.h>
#include <spine/TimeSeriesGeneratorOptions.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class Engine;
struct ObservableProperty;

class DatabaseDriverInterface
{
 public:
  virtual ~DatabaseDriverInterface();

  virtual void init(Engine *obsengine) = 0;
  virtual Spine::TimeSeries::TimeSeriesVectorPtr values(Settings &settings) = 0;
  virtual Spine::TimeSeries::TimeSeriesVectorPtr values(
      Settings &settings, const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions) = 0;
  virtual boost::shared_ptr<Spine::Table> makeQuery(
      Settings &settings, boost::shared_ptr<Spine::ValueFormatter> &valueFormatter) = 0;
  virtual void makeQuery(QueryBase *qb) = 0;
  virtual FlashCounts getFlashCount(const boost::posix_time::ptime &starttime,
                                    const boost::posix_time::ptime &endtime,
                                    const Spine::TaggedLocationList &locations) = 0;
  virtual boost::shared_ptr<std::vector<ObservableProperty> > observablePropertyQuery(
      std::vector<std::string> &parameters, const std::string language) = 0;

  virtual void getStations(Spine::Stations &stations, Settings &settings) = 0;
  virtual void shutdown() = 0;
  virtual MetaData metaData(const std::string &producer) = 0;
  virtual std::string id() const = 0;

 protected:
  DatabaseDriverInterface() {}

 private:
  // Pointer to dymanically loaded database driver
  void *itsHandle = nullptr;
  friend class DatabaseDriverFactory;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
