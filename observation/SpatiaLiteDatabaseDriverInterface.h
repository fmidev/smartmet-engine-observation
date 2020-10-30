#pragma once

#include "DatabaseDriverInterface.h"
#include "SpatiaLiteDatabaseDriver.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{

class SpatiaLiteDatabaseDriverInterface : public DatabaseDriverInterface
{
 public:
  SpatiaLiteDatabaseDriverInterface(SpatiaLiteDatabaseDriver* dbDriver) : itsDatabaseDriver(dbDriver) {}
  ~SpatiaLiteDatabaseDriverInterface();

  void init(Engine *obsengine);
  Spine::TimeSeries::TimeSeriesVectorPtr values(Settings &settings);
  Spine::TimeSeries::TimeSeriesVectorPtr values(Settings &settings, const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions);
  Spine::TaggedFMISIDList translateToFMISID(
      const boost::posix_time::ptime &starttime,
      const boost::posix_time::ptime &endtime,
      const std::string &stationtype,
      const StationSettings &stationSettings) const;
  void makeQuery(QueryBase *qb);
  FlashCounts getFlashCount(const boost::posix_time::ptime &starttime,
                                    const boost::posix_time::ptime &endtime,
                                    const Spine::TaggedLocationList &locations) const;
  boost::shared_ptr<std::vector<ObservableProperty>> observablePropertyQuery(
      std::vector<std::string> &parameters, const std::string language);

  void getStations(Spine::Stations &stations, const Settings &settings) const;
  void getStationsByArea(Spine::Stations &stations,
                                 const std::string &stationtype,
                                 const boost::posix_time::ptime &starttime,
                                 const boost::posix_time::ptime &endtime,
                                 const std::string &areaWkt) const;
  void getStationsByBoundingBox(Spine::Stations &stations,
                                        const Settings &settings) const;

  void shutdown();
  MetaData metaData(const std::string &producer) const;
  void reloadStations();
  std::string id() const;
  std::string name() const;

private:
  SpatiaLiteDatabaseDriver* itsDatabaseDriver;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
