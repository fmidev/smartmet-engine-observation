#pragma once

#include "DatabaseDriver.h"
#include "SpatiaLiteDriverParameters.h"
#include <macgyver/DateTimeParser.h>
#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class Engine;

class SpatiaLiteDatabaseDriver : public DatabaseDriver
{
 public:
  SpatiaLiteDatabaseDriver(boost::shared_ptr<EngineParameters> p, Spine::ConfigBase &cfg);

  void init(Engine *obsengine);

  Spine::TimeSeries::TimeSeriesVectorPtr values(Settings &settings);

  Spine::TimeSeries::TimeSeriesVectorPtr values(
      Settings &settings, const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions);

  boost::shared_ptr<Spine::Table> makeQuery(
      Settings &settings, boost::shared_ptr<Spine::ValueFormatter> &valueFormatter);

  void makeQuery(QueryBase *qb);

  FlashCounts getFlashCount(const boost::posix_time::ptime &starttime,
                            const boost::posix_time::ptime &endtime,
                            const Spine::TaggedLocationList &locations);

  boost::shared_ptr<std::vector<ObservableProperty> > observablePropertyQuery(
      std::vector<std::string> &parameters, const std::string language);

  void getStations(Spine::Stations &stations, Settings &settings);

  void updateFlashCache();
  void updateObservationCache();
  void updateWeatherDataQCCache();
  void locationsFromDatabase();
  void preloadStations(const std::string &serializedStationsFile);
  void shutdown();
  MetaData metaData(const std::string &producer);
  std::string id() const;

  ~SpatiaLiteDatabaseDriver() {}

 private:
  bool isParameter(const std::string &alias, const std::string &stationType) const;
  bool isParameterVariant(const std::string &name) const;
  void readConfig(Spine::ConfigBase &cfg);

  Fmi::TimeZones itsTimeZones;
  SpatiaLiteDriverParameters itsParameters;
  std::map<std::string, MetaData> itsMetaData;
  Engine *itsObsEngine = nullptr;
  Fmi::DateTimeParser itsDateTimeParser;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
