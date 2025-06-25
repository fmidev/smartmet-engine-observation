#pragma once

#include "CommonPostgreSQLFunctions.h"
#include "DataItem.h"
#include "EngineParameters.h"
#include "FlashDataItem.h"
#include "MagnetometerDataItem.h"
#include "MobileExternalDataItem.h"
#include "MovingLocationItem.h"
#include "ParameterMap.h"
#include "ProducerGroups.h"
#include "QueryResultBase.h"
#include "Settings.h"
#include "StationGroups.h"
#include "StationtypeConfig.h"
#include "DataItem.h"
#include <boost/atomic.hpp>
#include <macgyver/PostgreSQLConnection.h>
#include <macgyver/TimeZones.h>
#include <macgyver/ValueFormatter.h>
#include <spine/Station.h>
#include <timeseries/TimeSeriesInclude.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class PostgreSQLDatabaseDriver;

class PostgreSQLObsDB : public CommonPostgreSQLFunctions
{
 public:
  PostgreSQLObsDB(const Fmi::Database::PostgreSQLConnectionOptions &connectionOptions,
                  const StationtypeConfig &stc,
                  const ParameterMapPtr &pm);

  PostgreSQLObsDB() = delete;
  PostgreSQLObsDB(const PostgreSQLObsDB &other) = delete;
  PostgreSQLObsDB &operator=(const PostgreSQLObsDB &other) = delete;
  PostgreSQLObsDB(PostgreSQLObsDB &&other) = delete;
  PostgreSQLObsDB &operator=(PostgreSQLObsDB &&other) = delete;

  /**
   *  @brief Execute SQL statement and get the result.
   *  @param[in] sqlStatement SQL statement to execute.
   *  @param[out] qrb Pointer to the container to store the result data.
   *  @exception Obs_EngineException::OPERATION_PROCESSING_FAILED
   *  @exception Obs_EngineException::INVALID_PARAMETER_VALUE
   */
  void get(const std::string &sqlStatement,
           const std::shared_ptr<QueryResultBase> &qrb,
           const Fmi::TimeZones &timezones);

  void readMobileCacheDataFromPostgreSQL(const std::string &producer,
                                         std::vector<MobileExternalDataItem> &cacheData,
                                         Fmi::DateTime lastTime,
                                         Fmi::DateTime lastCreatedTime,
                                         const Fmi::TimeZones &timezones);
  void readCacheDataFromPostgreSQL(DataItems &cacheData,
                                   const Fmi::TimePeriod &dataPeriod,
                                   const std::string &fmisid,
                                   const std::string &measurandId,
                                   const Fmi::TimeZones &timezones);
  void readFlashCacheDataFromPostgreSQL(std::vector<FlashDataItem> &flashCacheData,
                                        const Fmi::TimePeriod &dataPeriod,
                                        const Fmi::TimeZones &timezones);
  void readWeatherDataQCCacheDataFromPostgreSQL(std::vector<DataItem> &cacheData,
                                                const Fmi::TimePeriod &dataPeriod,
                                                const std::string &fmisid,
                                                const std::string &measurandId,
                                                const Fmi::TimeZones &timezones);
  void readMovingStationsCacheDataFromPostgreSQL(std::vector<MovingLocationItem> &cacheData,
                                                 const Fmi::DateTime &startTime,
                                                 const Fmi::DateTime &lastModifiedTime,
                                                 const Fmi::TimeZones &timezones);
  void readCacheDataFromPostgreSQL(DataItems &cacheData,
                                   const Fmi::DateTime &startTime,
                                   const Fmi::DateTime &lastModifiedTime,
                                   const Fmi::TimeZones &timezones);
  void readFlashCacheDataFromPostgreSQL(std::vector<FlashDataItem> &flashCacheData,
                                        const Fmi::DateTime &startTime,
                                        const Fmi::DateTime &lastStrokeTime,
                                        const Fmi::DateTime &lastModifiedTime,
                                        const Fmi::TimeZones &timezones);
  void readWeatherDataQCCacheDataFromPostgreSQL(std::vector<DataItem> &cacheData,
                                                Fmi::DateTime lastTime,
                                                Fmi::DateTime lastModifiedTime,
                                                const Fmi::TimeZones &timezones);
  void readMagnetometerCacheDataFromPostgreSQL(std::vector<MagnetometerDataItem> &cacheData,
                                               Fmi::DateTime lastTime,
                                               Fmi::DateTime lastModifiedTime,
                                               const Fmi::TimeZones &timezones);

  Fmi::DateTime startTime;
  Fmi::DateTime endTime;
  std::optional<Fmi::DateTime> wantedTime;
  std::string timeFormat;
  int timeStep = 0;
  double maxDistance = 0;
  std::string stationType;
  std::string timeZone;
  bool allPlaces{false};

  void resetTimeSeries() { itsTimeSeriesColumns.reset(); }
  void setTimeInterval(const Fmi::DateTime &starttime, const Fmi::DateTime &endtime, int timestep);
  void fetchLocationDataItems(const std::string &sqlStmt,
                              const StationInfo &stationInfo,
                              const std::set<std::string> &stationgroup_codes,
                              const TS::RequestLimits &requestLimits,
                              LocationDataItems &weatherDataQCData) override;
  std::string sqlSelectFromLocationDataItems(const Settings &settings,
                                             const std::string &params,
                                             const std::string &station_ids) const override;

  void getStations(Spine::Stations &stations) const;
  void getStationGroups(StationGroups &sg) const;
  void getProducerGroups(ProducerGroups &pg) const;

  void getMovingStations(Spine::Stations &stations,
                         const Settings &settings,
                         const std::string &wkt) const;

  MeasurandInfo getMeasurandInfo(const EngineParametersPtr &engineParameters) const;

  Fmi::DateTime getLatestDataUpdateTime(const std::string &tablename,
                                        const Fmi::DateTime &from,
                                        const std::string &producer_ids,
                                        const std::string &measurand_ids) const;

 private:
  TS::TimeSeriesVectorPtr itsTimeSeriesColumns;

  void readCacheDataFromPostgreSQL(DataItems &cacheData,
                                   const std::string &sqlStmt,
                                   const Fmi::TimeZones &timezones);
  void readWeatherDataQCCacheDataFromPostgreSQL(std::vector<DataItem> &cacheData,
                                                const std::string &sqlStmt,
                                                const Fmi::TimeZones &timezones);
  void readFlashCacheDataFromPostgreSQL(std::vector<FlashDataItem> &flashCacheData,
                                        const std::string &sqlStmt,
                                        const Fmi::TimeZones &timezones);
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
