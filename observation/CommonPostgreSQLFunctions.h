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
  virtual ~CommonPostgreSQLFunctions();

  void shutdown();

  Spine::TimeSeries::TimeSeriesVectorPtr getObservationData(
      const Spine::Stations &stations,
      const SmartMet::Engine::Observation::Settings &settings,
      const SmartMet::Engine::Observation::StationInfo &stationInfo,
      const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions,
      const Fmi::TimeZones &timezones);
  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr getFlashData(
      const SmartMet::Engine::Observation::Settings &settings, const Fmi::TimeZones &timezones);
  FlashCounts getFlashCount(const boost::posix_time::ptime &starttime,
                            const boost::posix_time::ptime &endtime,
                            const SmartMet::Spine::TaggedLocationList &locations);
  bool isConnected();
  void reConnect();
  void setConnectionId(int connectionId) { itsConnectionId = connectionId; }
  int connectionId() { return static_cast<int>(itsConnectionId); }
  Fmi::Database::PostgreSQLConnection &getConnection() { return itsDB; }
  const std::shared_ptr<Fmi::TimeFormatter> &getTimeFormatter() const { return itsTimeFormatter; }
  const std::shared_ptr<Fmi::TimeFormatter> &resetTimeFormatter(const std::string &format);

 protected:
  Fmi::Database::PostgreSQLConnection itsDB;

  std::size_t itsConnectionId;
  std::map<unsigned int, std::string> itsPostgreDataTypes;
  std::atomic<bool> itsShutdownRequested{false};
  bool itsIsCacheDatabase{false};

  std::shared_ptr<Fmi::TimeFormatter> itsTimeFormatter;

 private:
  SmartMet::Engine::Observation::LocationDataItems readObservationDataFromDB(
      const Spine::Stations &stations,
      const SmartMet::Engine::Observation::Settings &settings,
      const SmartMet::Engine::Observation::StationInfo &stationInfo,
      const SmartMet::Engine::Observation::QueryMapping &qmap,
      const std::set<std::string> &stationgroup_codes) const;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
