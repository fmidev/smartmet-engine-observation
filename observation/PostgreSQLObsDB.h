#pragma once

#include "CommonPostgreSQLFunctions.h"
#include "DataItem.h"
#include "FlashDataItem.h"
#include "MobileExternalDataItem.h"
#include "MagnetometerDataItem.h"
#include "ParameterMap.h"
#include "QueryResultBase.h"
#include "Settings.h"
#include "StationtypeConfig.h"
#include "WeatherDataQCItem.h"
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

class PostgreSQLObsDB : public CommonPostgreSQLFunctions, private boost::noncopyable
{
 public:
  PostgreSQLObsDB(const Fmi::Database::PostgreSQLConnectionOptions &connectionOptions,
                  const StationtypeConfig &stc,
                  const ParameterMapPtr &pm);

  /**
   *  @brief Execute SQL statement and get the result.
   *  @param[in] sqlStatement SQL statement to execute.
   *  @param[out] qrb Pointer to the container to store the result data.
   *  @exception Obs_EngineException::OPERATION_PROCESSING_FAILED
   *  @exception Obs_EngineException::INVALID_PARAMETER_VALUE
   */
  void get(const std::string &sqlStatement,
           std::shared_ptr<QueryResultBase> qrb,
           const Fmi::TimeZones &timezones);

  void readMobileCacheDataFromPostgreSQL(const std::string &producer,
                                         std::vector<MobileExternalDataItem> &cacheData,
                                         boost::posix_time::ptime lastTime,
                                         boost::posix_time::ptime lastCreatedTime,
                                         const Fmi::TimeZones &timezones);

  void readCacheDataFromPostgreSQL(std::vector<DataItem> &cacheData,
                                   const boost::posix_time::time_period &dataPeriod,
                                   const std::string &fmisid,
                                   const std::string &measurandId,
                                   const Fmi::TimeZones &timezones);
  void readFlashCacheDataFromPostgreSQL(std::vector<FlashDataItem> &flashCacheData,
                                        const boost::posix_time::time_period &dataPeriod,
                                        const Fmi::TimeZones &timezones);
  void readWeatherDataQCCacheDataFromPostgreSQL(std::vector<WeatherDataQCItem> &cacheData,
                                                const boost::posix_time::time_period &dataPeriod,
                                                const std::string &fmisid,
                                                const std::string &measurandId,
                                                const Fmi::TimeZones &timezones);

  void readCacheDataFromPostgreSQL(std::vector<DataItem> &cacheData,
                                   const boost::posix_time::ptime &startTime,
                                   const boost::posix_time::ptime &lastModifiedTime,
                                   const Fmi::TimeZones &timezones);
  void readFlashCacheDataFromPostgreSQL(std::vector<FlashDataItem> &flashCacheData,
                                        const boost::posix_time::ptime &startTime,
                                        const boost::posix_time::ptime &lastStrokeTime,
                                        const boost::posix_time::ptime &lastModifiedTime,
                                        const Fmi::TimeZones &timezones);
  void readWeatherDataQCCacheDataFromPostgreSQL(std::vector<WeatherDataQCItem> &cacheData,
                                                boost::posix_time::ptime lastTime,
                                                boost::posix_time::ptime lastModifiedTime,
                                                const Fmi::TimeZones &timezones);
  void readMagnetometerCacheDataFromPostgreSQL(std::vector<MagnetometerDataItem> &cacheData,
											   boost::posix_time::ptime lastTime,
											   boost::posix_time::ptime lastModifiedTime,
											   const Fmi::TimeZones &timezones);

  boost::posix_time::ptime startTime;
  boost::posix_time::ptime endTime;
  std::string timeFormat;
  int timeStep;
  double maxDistance;
  std::string stationType;
  std::string timeZone;
  bool allPlaces{false};
  bool latest{false};
  bool bigFlashRequestReported{false};

  void resetTimeSeries() { itsTimeSeriesColumns.reset(); }
  void setTimeInterval(const boost::posix_time::ptime &starttime,
                       const boost::posix_time::ptime &endtime,
                       const int timestep);
  void fetchWeatherDataQCData(const std::string &sqlStmt,
                              const StationInfo &stationInfo,
                              const std::set<std::string> &stationgroup_codes,
                              const QueryMapping &qmap,
                              WeatherDataQCData &weatherDataQCData) override;
  std::string sqlSelectFromWeatherDataQCData(const Settings &settings,
                                             const std::string &params,
                                             const std::string &station_ids) const override;

  // Station id manipulators
  void translateToIdFunction(Spine::Stations &stations, int net_id) const;
  void translateToLPNN(Spine::Stations &stations) const;
  void translateToWMO(Spine::Stations &stations) const;
  void translateToRWSID(Spine::Stations &stations) const;

  void getStations(Spine::Stations &stations) const;
  void readStationLocations(StationLocations &stationLocations) const;
 private:
  TS::TimeSeriesVectorPtr itsTimeSeriesColumns;
  LocationDataItems readObservations(const Spine::Stations &stations,
                                     const Settings &settings,
                                     const StationInfo &stationInfo,
                                     const QueryMapping &qmap,
                                     const std::set<std::string> &stationgroup_codes);

  void readCacheDataFromPostgreSQL(std::vector<DataItem> &cacheData,
                                   const std::string &sqlStmt,
                                   const Fmi::TimeZones &timezones);
  void readWeatherDataQCCacheDataFromPostgreSQL(std::vector<WeatherDataQCItem> &cacheData,
                                                const std::string &sqlStmt,
                                                const Fmi::TimeZones &timezones);
  void readFlashCacheDataFromPostgreSQL(std::vector<FlashDataItem> &flashCacheData,
                                        const std::string &sqlStmt,
                                        const Fmi::TimeZones &timezones);
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
