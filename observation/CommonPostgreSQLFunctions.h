#pragma once

#include "CommonDatabaseFunctions.h"
#include <macgyver/PostgreSQLConnection.h>
#include <macgyver/TimeFormatter.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class CommonPostgreSQLFunctions : public CommonDatabaseFunctions
{
 public:
  CommonPostgreSQLFunctions(const Fmi::Database::PostgreSQLConnectionOptions &connectionOptions,
                            const StationtypeConfig &stc,
                            const ParameterMapPtr &pm);
  ~CommonPostgreSQLFunctions() override;

  void shutdown();

  TS::TimeSeriesVectorPtr getObservationDataForMovingStations(
      const Settings &settings,
      const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
      const Fmi::TimeZones &timezones) override;

  TS::TimeSeriesVectorPtr getObservationData(
      const Spine::Stations &stations,
      const Settings &settings,
      const StationInfo &stationInfo,
      const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
      const Fmi::TimeZones &timezones,
      const std::unique_ptr<ObservationMemoryCache> &observationMemoryCache) override;

  TS::TimeSeriesVectorPtr getFlashData(const Settings &settings,
                                       const Fmi::TimeZones &timezones) override;

  FlashCounts getFlashCount(const boost::posix_time::ptime &starttime,
                            const boost::posix_time::ptime &endtime,
                            const Spine::TaggedLocationList &locations) override;

  bool isConnected();
  void reConnect();
  void setConnectionId(int connectionId) { itsConnectionId = connectionId; }
  int connectionId() { return static_cast<int>(itsConnectionId); }
  Fmi::Database::PostgreSQLConnection &getConnection() { return itsDB; }
  const std::shared_ptr<Fmi::TimeFormatter> &getTimeFormatter() const { return itsTimeFormatter; }
  const std::shared_ptr<Fmi::TimeFormatter> &resetTimeFormatter(const std::string &format);
  TS::TimeSeriesVectorPtr getMagnetometerData(
      const Spine::Stations &stations,
      const Settings &settings,
      const StationInfo &stationInfo,
      const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
      const Fmi::TimeZones &timezones) override;

 protected:
  Fmi::Database::PostgreSQLConnection itsDB;

  std::size_t itsConnectionId;
  std::map<unsigned int, std::string> itsPostgreDataTypes;
  bool itsIsCacheDatabase{false};

  std::shared_ptr<Fmi::TimeFormatter> itsTimeFormatter;

 private:
  LocationDataItems readObservationDataFromDB(
      const Spine::Stations &stations,
      const Settings &settings,
      const StationInfo &stationInfo,
      const QueryMapping &qmap,
      const std::set<std::string> &stationgroup_codes) const;

  LocationDataItems readObservationDataOfMovingStationsFromDB(
      const Settings &settings,
      const QueryMapping &qmap,
      const std::set<std::string> &stationgroup_codes) const;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
