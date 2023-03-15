#pragma once

#include "MetaData.h"
#include "ProducerGroups.h"
#include "QueryBase.h"
#include "QueryResultBase.h"
#include "Settings.h"
#include "StationGroups.h"
#include "StationSettings.h"
#include "Utils.h"
#include <boost/atomic.hpp>
#include <boost/thread/condition.hpp>
#include <macgyver/CacheStats.h>
#include <spine/Station.h>
#include <timeseries/TimeSeriesInclude.h>

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

  DatabaseDriverInterface(const DatabaseDriverInterface &other) = delete;
  DatabaseDriverInterface(DatabaseDriverInterface &&other) = delete;
  DatabaseDriverInterface &operator=(const DatabaseDriverInterface &other) = delete;
  DatabaseDriverInterface &operator=(DatabaseDriverInterface &&other) = delete;

  virtual void init(Engine *obsengine) = 0;
  virtual TS::TimeSeriesVectorPtr values(Settings &settings) = 0;
  virtual TS::TimeSeriesVectorPtr values(
      Settings &settings, const TS::TimeSeriesGeneratorOptions &timeSeriesOptions) = 0;
  virtual Spine::TaggedFMISIDList translateToFMISID(
      const boost::posix_time::ptime &starttime,
      const boost::posix_time::ptime &endtime,
      const std::string &stationtype,
      const StationSettings &stationSettings) const = 0;
  virtual void makeQuery(QueryBase *qb) = 0;
  virtual FlashCounts getFlashCount(const boost::posix_time::ptime &starttime,
                                    const boost::posix_time::ptime &endtime,
                                    const Spine::TaggedLocationList &locations) const = 0;
  virtual std::shared_ptr<std::vector<ObservableProperty>> observablePropertyQuery(
      std::vector<std::string> &parameters, const std::string language) = 0;

  virtual void getStations(Spine::Stations &stations, const Settings &settings) const = 0;
  virtual void getStationsByArea(Spine::Stations &stations,
                                 const std::string &stationtype,
                                 const boost::posix_time::ptime &starttime,
                                 const boost::posix_time::ptime &endtime,
                                 const std::string &areaWkt) const = 0;
  virtual void getStationsByBoundingBox(Spine::Stations &stations,
                                        const Settings &settings) const = 0;

  virtual void shutdown() = 0;
  virtual MetaData metaData(const std::string &producer) const = 0;
  virtual void reloadStations() = 0;
  virtual std::string id() const = 0;
  virtual std::string name() const = 0;
  virtual Fmi::Cache::CacheStatistics getCacheStats() const = 0;
  virtual void getStationGroups(StationGroups &sg) const = 0;
  virtual void getProducerGroups(ProducerGroups &pg) const = 0;

 protected:
  DatabaseDriverInterface() = default;

 private:
  // Pointer to dymanically loaded database driver
  void *itsHandle = nullptr;
  friend class DatabaseDriverFactory;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
