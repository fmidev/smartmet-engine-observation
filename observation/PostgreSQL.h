#pragma once

#include "CommonDatabaseFunctions.h"
#include "DataItem.h"
#include "ExternalAndMobileDBInfo.h"
#include "ExternalAndMobileProducerConfig.h"
#include "FlashDataItem.h"
#include "InsertStatus.h"
#include "LocationItem.h"
#include "MobileExternalDataItem.h"
#include "Utils.h"
#include "WeatherDataQCItem.h"

#include <macgyver/PostgreSQLConnection.h>
#include <spine/Value.h>
#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
using ResultSetRow = std::map<std::string, SmartMet::Spine::TimeSeries::Value>;
using ResultSetRows = std::vector<ResultSetRow>;

struct PostgreSQLCacheParameters;

struct cached_data
{
  std::vector<boost::optional<int>> fmisidsAll;
  std::vector<boost::posix_time::ptime> obstimesAll;
  std::vector<boost::optional<double>> longitudesAll;
  std::vector<boost::optional<double>> latitudesAll;
  std::vector<boost::optional<double>> elevationsAll;
  std::vector<boost::optional<std::string>> parametersAll;
  std::vector<boost::optional<int>> measurand_idsAll;
  std::vector<boost::optional<double>> data_valuesAll;
  std::vector<boost::optional<int>> data_sourcesAll;
  std::vector<boost::optional<double>> sensor_nosAll;
};

class PostgreSQL : public CommonDatabaseFunctions, private boost::noncopyable
{
 public:
  PostgreSQL(const PostgreSQLCacheParameters &options);
  ~PostgreSQL();

  /**
   * @brief Get the time of the newest observation in observation_data table
   * @return boost::posix_time::ptime The time of the newest observation
   */

  boost::posix_time::ptime getLatestObservationTime();

  /**
   * @brief Get the last modified time in observation_data table
   * @return boost::posix_time::ptime The time of the last modification
   */

  boost::posix_time::ptime getLatestObservationModifiedTime();

  /**
   * @brief Get the time of the newest observation in flash_data table
   * @return boost::posix_time::ptime The time of the newest observation
   */

  boost::posix_time::ptime getLatestFlashTime();

  /**
   * @brief Get the time of the newest observation in weather_data_qc table
   * @return boost::posix_time::ptime The time of the newest observation
   */
  boost::posix_time::ptime getLatestWeatherDataQCTime();

  /**
   * @brief Get the time of the oldest observation in observation_data table
   * @return boost::posix_time::ptime The time of the oldest observation
   */

  boost::posix_time::ptime getOldestObservationTime();

  /**
   * @brief Get the time of the oldest observation in flash_data table
   * @return boost::posix_time::ptime The time of the oldest observation
   */

  boost::posix_time::ptime getOldestFlashTime();

  /**
   * @brief Get the time of the oldest observation in weather_data_qc table
   * @return boost::posix_time::ptime The time of the oldest observation
   */
  boost::posix_time::ptime getOldestWeatherDataQCTime();

  /**
   * @brief Create the PostgreSQL tables from scratch
   */
  void createTables(const std::set<std::string> &tables);

  void setConnectionId(int connectionId) { itsConnectionId = connectionId; }
  int connectionId() { return static_cast<int>(itsConnectionId); }

  /**
   * @brief Update observation_data with data from Oracle's
   *        observation_data table which is used to store data
   *        from stations maintained by FMI.
   * @param[in] cacheData Data from observation_data.
   */
  std::size_t fillDataCache(const DataItems &cacheData);

  /**
   * @brief Update weather_data_qc with data from Oracle's respective table
   *        which is used to store data from road and foreign stations
   * @param[in] cacheData Data from weather_data_qc.
   */
  std::size_t fillWeatherDataQCCache(const WeatherDataQCItems &cacheData);

  /**
   * @brief Insert cached observations into observation_data table
   * @param flashCacheData Observation data to be inserted into the table
   */
  std::size_t fillFlashDataCache(const FlashDataItems &flashCacheData);

  /**
   * @brief Delete old observation data from tablename table using time_column
   * time field
   * @param tablename The name of the table from which the data will be deleted
   * @param time_column Indicates the time field which is used in WHERE
   * statement
   * @param last_time Delete everything from tablename which is older than
   * last_time
   */
  void cleanCacheTable(const std::string tablename,
                       const std::string time_column,
                       const boost::posix_time::ptime last_time);

  /**
   * @brief Delete everything from observation_data table which is
   *        older than the given duration
   * @param[in] newstarttime
   */
  void cleanDataCache(const boost::posix_time::ptime &newstarttime);

  /**
   * @brief Delete everything from weather_data_qc table which
   *        is older than given duration
   * @param[in] newstarttime
   */
  void cleanWeatherDataQCCache(const boost::posix_time::ptime &newstarttime);

  /**
   * @brief Delete old flash observation data from flash_data table
   * @param newstarttime Delete everything from flash_data which is older than given time
   */
  void cleanFlashDataCache(const boost::posix_time::ptime &newstarttime);

  /**
   * @brief Get the time of the oldest RoadCloud observation in ext_obsdata table
   * @return boost::posix_time::ptime The time of the oldest observation
   */

  boost::posix_time::ptime getOldestRoadCloudDataTime();

  /**
   * @brief Get the latest creation time of RoadCloud observation in ext_obsdata table
   * @return boost::posix_time::ptime The latest creation time road cloud observation
   */

  boost::posix_time::ptime getLatestRoadCloudCreatedTime();

  /**
   * @brief Get the time of the newest RoadCloud observation in ext_obsdata table
   * @return boost::posix_time::ptime The time of the newest observation
   */

  boost::posix_time::ptime getLatestRoadCloudDataTime();

  /**
   * @brief Delete old RoadCloud data from ext_obsdata table
   * @param newstarttime Delete RoadCloud data from ext_obsdata which is older than given time
   */
  void cleanRoadCloudCache(const boost::posix_time::ptime &newstarttime);

  /**
   * @brief Insert cached RoadCloud observations into ext_obsdata table
   * @param RoadCloud observation data to be inserted into the table
   */
  std::size_t fillRoadCloudCache(const MobileExternalDataItems &mobileExternalCacheData);

  /**
   * @brief Get the time of the oldest NetAtmo observation in ext_obsdata table
   * @return boost::posix_time::ptime The time of the oldest observation
   */

  boost::posix_time::ptime getOldestNetAtmoDataTime();

  /**
   * @brief Get the time of the newest NetAtmo observation in ext_obsdata table
   * @return boost::posix_time::ptime The time of the newest observation
   */

  boost::posix_time::ptime getLatestNetAtmoDataTime();

  /**
   * @brief Get the time of the latest NetAtmo creation time in ext_obsdata table
   * @return boost::posix_time::ptime The latest creation time
   */

  boost::posix_time::ptime getLatestNetAtmoCreatedTime();

  /**
   * @brief Delete old NetAtmo data from ext_obsdata table
   * @param newstarttime Delete NetAtmo data which is older than given time
   */
  void cleanNetAtmoCache(const boost::posix_time::ptime &newstarttime);

  /**
   * @brief Insert cached NetAtmo observations into ext_obsdata table
   * @param NetAtmo observation data to be inserted into the table
   */
  std::size_t fillNetAtmoCache(const MobileExternalDataItems &mobileExternalCacheData);

  /**
   * @brief Get the time of the newest FmiIoT observation in ext_obsdata_roadcloud table
   * @return boost::posix_time::ptime The time of the newest FmiIoT observation
   */

  boost::posix_time::ptime getLatestFmiIoTDataTime();

  /**
   * @brief Get the time of the latest FmiIoT creation time in ext_obsdata table
   * @return boost::posix_time::ptime The latest creation time
   */

  boost::posix_time::ptime getLatestFmiIoTCreatedTime();

  /**
   * @brief Get the time of the oldest FmiIoT observation in ext_obsdata_roadcloud table

   * @return boost::posix_time::ptime The time of the oldest FmiIoT observation
   */

  boost::posix_time::ptime getOldestFmiIoTDataTime();

  /**
   * @brief Insert cached observations into ext_obsdata_roadcloud table
   * @param FmiIoT observation data to be inserted into the table
   */
  std::size_t fillFmiIoTCache(const MobileExternalDataItems &mobileExternalCacheData);

  /**
   * @brief Delete old FmiIoT observation data from ext_obsdata_roadcloud table
   * @param timetokeep Delete FmiIoT data which is older than given duration
   */
  void cleanFmiIoTCache(const boost::posix_time::ptime &newstarttime);

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr getRoadCloudData(
      const Settings &settings,
      const ParameterMapPtr &parameterMap,
      const Fmi::TimeZones &timezones);

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr getNetAtmoData(
      const Settings &settings,
      const ParameterMapPtr &parameterMap,
      const Fmi::TimeZones &timezones);

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr getFmiIoTData(
      const Settings &settings,
      const ParameterMapPtr &parameterMap,
      const Fmi::TimeZones &timezones);

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr getFlashData(const Settings &settings,
                                                                const ParameterMapPtr &parameterMap,
                                                                const Fmi::TimeZones &timezones);

  virtual Spine::TimeSeries::TimeSeriesVectorPtr getData(
      const Spine::Stations &stations,
      const Settings &settings,
      const StationInfo &stationInfo,
      const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions,
      const Fmi::TimeZones &timezones);

  void shutdown();

  /**
   * @brief Get count of flashes are in the time interval
   * @param starttime Start of the time interval
   * @param endtime End of the time interval
   * @param locations Locations
   * @return FlashCounts The number of flashes in the interval
   */
  FlashCounts getFlashCount(const boost::posix_time::ptime &starttime,
                            const boost::posix_time::ptime &endtime,
                            const SmartMet::Spine::TaggedLocationList &locations);

  size_t selectCount(const std::string &queryString);

  static ResultSetRows getResultSetForMobileExternalData(
      const pqxx::result &pgResultSet, const std::map<unsigned int, std::string> &pgDataTypes);

  void fetchWeatherDataQCData(const std::string &sqlStmt,
                              const StationInfo &stationInfo,
                              const std::set<std::string> &stationgroup_codes,
                              const QueryMapping &qmap,
                              std::map<int, std::map<int, int>> &default_sensors,
                              WeatherDataQCData &cacheData);
  std::string sqlSelectFromWeatherDataQCData(const Settings &settings,
                                             const std::string &params,
                                             const std::string &station_ids) const;

 private:
  // Private members

  std::string srid;
  std::size_t itsConnectionId;
  std::size_t itsMaxInsertSize;

  InsertStatus itsDataInsertCache;
  InsertStatus itsWeatherQCInsertCache;
  InsertStatus itsFlashInsertCache;
  InsertStatus itsRoadCloudInsertCache;
  InsertStatus itsNetAtmoInsertCache;
  InsertStatus itsFmiIoTInsertCache;
  const ExternalAndMobileProducerConfig &itsExternalAndMobileProducerConfig;
  std::map<unsigned int, std::string> itsPostgreDataTypes;
  boost::atomic<bool> itsShutdownRequested;

  // Private methods
  Fmi::Database::PostgreSQLConnection itsDB;
  std::string stationType(const std::string &type);
  std::string stationType(SmartMet::Spine::Station &station);

  void addSmartSymbolToTimeSeries(
      const int pos,
      const Spine::Station &s,
      const boost::local_time::local_date_time &time,
      const ParameterMapPtr &parameterMap,
      const std::string &stationtype,
      const std::map<int,
                     std::map<boost::local_time::local_date_time,
                              std::map<int, SmartMet::Spine::TimeSeries::Value>>> &data,
      const Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns);

  void addSpecialParameterToTimeSeries(
      const std::string &paramname,
      SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns,
      const SmartMet::Spine::Station &station,
      const int pos,
      const std::string stationtype,
      const boost::local_time::local_date_time &obstime);

  void addParameterToTimeSeries(
      SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns,
      const std::pair<boost::local_time::local_date_time,
                      std::map<std::string, SmartMet::Spine::TimeSeries::Value>> &dataItem,
      const std::map<std::string, int> &specialPositions,
      const std::map<std::string, std::string> &parameterNameMap,
      const std::map<std::string, int> &timeseriesPositions,
      const ParameterMapPtr &parameterMap,
      const std::string &stationtype,
      const SmartMet::Spine::Station &station,
      const std::string &missingtext);

  void addEmptyValuesToTimeSeries(
      SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns,
      const boost::local_time::local_date_time &obstime,
      const std::map<std::string, int> &specialPositions,
      const std::map<std::string, std::string> &parameterNameMap,
      const std::map<std::string, int> &timeseriesPositions,
      const std::string &stationtype,
      const SmartMet::Spine::Station &station);

  boost::posix_time::ptime getLatestTimeFromTable(std::string tablename, std::string time_field);
  boost::posix_time::ptime getOldestTimeFromTable(std::string tablename, std::string time_field);

  void createStationTable();
  void createStationGroupsTable();
  void createGroupMembersTable();
  void createLocationsTable();
  void createObservationDataTable();
  void createWeatherDataQCTable();
  void createFlashDataTable();
  void createRoadCloudDataTable();
  void createNetAtmoDataTable();
  void createFmiIoTDataTable();
  void fetchCachedDataFromDB(const std::string &sqlStmt,
                             struct cached_data &data,
                             bool measurand = false);
  void createIndex(const std::string &table,
                   const std::string &column,
                   const std::string &idx_name,
                   bool transaction = false) const;
  void dropIndex(const std::string &idx_name, bool transaction = false) const;

  boost::posix_time::ptime getTime(const std::string &timeQuery) const;
  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr getMobileAndExternalData(
      const Settings &settings,
      const ParameterMapPtr &parameterMap,
      const Fmi::TimeZones &timezones);
  LocationDataItems readObservations(const Spine::Stations &stations,
                                     const Settings &settings,
                                     const StationInfo &stationInfo,
                                     const QueryMapping &qmap,
                                     const std::set<std::string> &stationgroup_codes);
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
