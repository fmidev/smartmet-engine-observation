#pragma once

#include "CommonPostgreSQLFunctions.h"
#include "DataItem.h"
#include "ExternalAndMobileDBInfo.h"
#include "ExternalAndMobileProducerConfig.h"
#include "FlashDataItem.h"
#include "InsertStatus.h"
#include "MobileExternalDataItem.h"
#include "MovingLocationItem.h"
#include "Utils.h"
#include <macgyver/PostgreSQLConnection.h>
#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
using ResultSetRow = std::map<std::string, TS::Value>;
using ResultSetRows = std::vector<ResultSetRow>;

struct PostgreSQLCacheParameters;

#if 0
struct cached_data
{
  std::vector<std::optional<int>> fmisidsAll;
  std::vector<Fmi::DateTime> obstimesAll;
  std::vector<std::optional<double>> longitudesAll;
  std::vector<std::optional<double>> latitudesAll;
  std::vector<std::optional<double>> elevationsAll;
  std::vector<std::optional<std::string>> parametersAll;
  std::vector<std::optional<int>> measurand_idsAll;
  std::vector<std::optional<double>> data_valuesAll;
  std::vector<std::optional<int>> data_sourcesAll;
  std::vector<std::optional<double>> sensor_nosAll;
};
#endif

class PostgreSQLCacheDB : public CommonPostgreSQLFunctions
{
 public:
  explicit PostgreSQLCacheDB(const PostgreSQLCacheParameters &options);
  PostgreSQLCacheDB() = delete;
  PostgreSQLCacheDB(const PostgreSQLCacheDB &other) = delete;
  PostgreSQLCacheDB &operator=(const PostgreSQLCacheDB &other) = delete;
  PostgreSQLCacheDB(PostgreSQLCacheDB &&other) = delete;
  PostgreSQLCacheDB &operator=(PostgreSQLCacheDB &&other) = delete;

  /**
   * @brief Get the time of the newest observation in observation_data table
   * @return Fmi::DateTime The time of the newest observation
   */

  Fmi::DateTime getLatestObservationTime();

  /**
   * @brief Get the last modified time in observation_data table
   * @return Fmi::DateTime The time of the last modification
   */

  Fmi::DateTime getLatestObservationModifiedTime();

  /**
   * @brief Get the time of the latest modified flash obervation
   * @retval Fmi::DateTime The time of the last modification
   */

  Fmi::DateTime getLatestFlashModifiedTime();

  /**
   * @brief Get the time of the newest observation in flash_data table
   * @return Fmi::DateTime The time of the newest observation
   */

  Fmi::DateTime getLatestFlashTime();

  /**
   * @brief Get the time of the newest observation in weather_data_qc table
   * @return Fmi::DateTime The time of the newest observation
   */
  Fmi::DateTime getLatestWeatherDataQCTime();

  /**
   * @brief Get the time of the newest observation in weather_data_qc table
   * @return Fmi::DateTime The time of the newest observation
   */
  Fmi::DateTime getLatestWeatherDataQCModifiedTime();

  /**
   * @brief Get the time of the oldest observation in observation_data table
   * @return Fmi::DateTime The time of the oldest observation
   */

  Fmi::DateTime getOldestObservationTime();

  /**
   * @brief Get the time of the oldest observation in flash_data table
   * @return Fmi::DateTime The time of the oldest observation
   */

  Fmi::DateTime getOldestFlashTime();

  /**
   * @brief Get the time of the oldest observation in weather_data_qc table
   * @return Fmi::DateTime The time of the oldest observation
   */
  Fmi::DateTime getOldestWeatherDataQCTime();

  /**
   * @brief Create the PostgreSQL cache tables from scratch
   */
  void createTables(const std::set<std::string> &tables);

  /**
   * @brief Update observation_data with data from Oracle's
   *        observation_data table which is used to store data
   *        from stations maintained by FMI.
   * @param[in] cacheData Data from observation_data.
   */
  std::size_t fillDataCache(const DataItems &cacheData);

  /**
   * @brief Update moving_locations with data from Oracle's
   *        moving_locations table which is used to store data
   *        from stations maintained by FMI.
   * @param[in] cacheData Data from moving_locations
   */
  static std::size_t fillMovingLocationsCache(const MovingLocationItems &cacheData);

  /**
   * @brief Update weather_data_qc with data from Oracle's respective table
   *        which is used to store data from road and foreign stations
   * @param[in] cacheData Data from weather_data_qc.
   */
  std::size_t fillWeatherDataQCCache(const DataItems &cacheData);

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
  void cleanCacheTable(const std::string &tablename,
                       const std::string &time_column,
                       const Fmi::DateTime &last_time);

  /**
   * @brief Delete everything from observation_data table which is
   *        older than the given duration
   * @param[in] newstarttime
   */
  void cleanDataCache(const Fmi::DateTime &newstarttime);

  /**
   * @brief Delete everything from weather_data_qc table which
   *        is older than given duration
   * @param[in] newstarttime
   */
  void cleanWeatherDataQCCache(const Fmi::DateTime &newstarttime);

  /**
   * @brief Delete old flash observation data from flash_data table
   * @param newstarttime Delete everything from flash_data which is older than given time
   */
  void cleanFlashDataCache(const Fmi::DateTime &newstarttime);

  /**
   * @brief Get oldest RoadCloud observation in ext_obsdata_roadcloud table
   * @return Fmi::DateTime The time of the oldest observation
   */

  Fmi::DateTime getOldestRoadCloudDataTime();

  /**
   * @brief Get the latest creation time in ext_obsdata_roadcloud table
   * @return Fmi::DateTime The latest creation time road cloud observation
   */

  Fmi::DateTime getLatestRoadCloudCreatedTime();

  /**
   * @brief Get newest observation in ext_obsdata_roadcloud table
   * @return Fmi::DateTime The time of the newest observation
   */

  Fmi::DateTime getLatestRoadCloudDataTime();

  /**
   * @brief Delete old data from ext_obsdata_roadcloud table
   * @param newstarttime Delete data from ext_obsdata_roadcloud table which is older than given time
   */
  void cleanRoadCloudCache(const Fmi::DateTime &newstarttime);

  /**
   * @brief Insert cached RoadCloud observations into ext_obsdata table
   * @param RoadCloud observation data to be inserted into the table
   */
  std::size_t fillRoadCloudCache(const MobileExternalDataItems &mobileExternalCacheData);

  /**
   * @brief Get oldest observation in ext_obsdata_netatmo table
   * @return Fmi::DateTime The time of the oldest observation
   */

  Fmi::DateTime getOldestNetAtmoDataTime();

  /**
   * @brief Get newest observation in ext_obsdata_netatmo table
   * @return Fmi::DateTime The time of the newest observation
   */

  Fmi::DateTime getLatestNetAtmoDataTime();

  /**
   * @brief Get latest creation time in ext_obsdata_netatmo table
   * @return Fmi::DateTime The latest creation time
   */

  Fmi::DateTime getLatestNetAtmoCreatedTime();

  /**
   * @brief Delete old data from ext_obsdata_netatmo table
   * @param newstarttime Delete NetAtmo data which is older than given time
   */
  void cleanNetAtmoCache(const Fmi::DateTime &newstarttime);

  /**
   * @brief Insert cached NetAtmo observations into ext_obsdata_netatmo table
   * @param NetAtmo observation data to be inserted into the table
   */
  std::size_t fillNetAtmoCache(const MobileExternalDataItems &mobileExternalCacheData);

  /**
   * @brief Get the time of the newest FmiIoT observation in ext_obsdata_roadcloud table
   * @return Fmi::DateTime The time of the newest FmiIoT observation
   */

  Fmi::DateTime getLatestFmiIoTDataTime();

  /**
   * @brief Get the time of the latest FmiIoT creation time in ext_obsdata table
   * @return Fmi::DateTime The latest creation time
   */

  Fmi::DateTime getLatestFmiIoTCreatedTime();

  /**
   * @brief Get the time of the oldest FmiIoT observation in ext_obsdata_roadcloud table

   * @return Fmi::DateTime The time of the oldest FmiIoT observation
   */

  Fmi::DateTime getOldestFmiIoTDataTime();

  /**
   * @brief Insert cached observations into ext_obsdata_roadcloud table
   * @param FmiIoT observation data to be inserted into the table
   */
  static std::size_t fillFmiIoTCache(const MobileExternalDataItems &mobileExternalCacheData);

  /**
   * @brief Delete old FmiIoT observation data from ext_obsdata_roadcloud table
   * @param timetokeep Delete FmiIoT data which is older than given duration
   */
  void cleanFmiIoTCache(const Fmi::DateTime &newstarttime);

  /**
   * @brief Get the time of the newest TapsiQc observation in ext_obsdata table
   * @return Fmi::DateTime The time of the newest TapsiQc observation
   */

  Fmi::DateTime getLatestTapsiQcDataTime();

  /**
   * @brief Get the time of the latest TapsiQc creation time in ext_obsdata table
   * @return Fmi::DateTime The latest creation time
   */

  Fmi::DateTime getLatestTapsiQcCreatedTime();

  /**
   * @brief Get the time of the oldest TapsiQc observation in ext_obsdata table

   * @return Fmi::DateTime The time of the oldest TapsiQc observation
   */

  Fmi::DateTime getOldestTapsiQcDataTime();

  /**
   * @brief Insert cached observations into ext_obsdata table
   * @param TapsiQc observation data to be inserted into the table
   */
  static std::size_t fillTapsiQcCache(const MobileExternalDataItems &mobileExternalCacheData);

  /**
   * @brief Delete old TapsiQc observation data from ext_obsdata table
   * @param timetokeep Delete TapsiQc data which is older than given duration
   */
  void cleanTapsiQcCache(const Fmi::DateTime &newstarttime);

  TS::TimeSeriesVectorPtr getRoadCloudData(const Settings &settings,
                                           const ParameterMapPtr &parameterMap,
                                           const Fmi::TimeZones &timezones);

  TS::TimeSeriesVectorPtr getNetAtmoData(const Settings &settings,
                                         const ParameterMapPtr &parameterMap,
                                         const Fmi::TimeZones &timezones);

  TS::TimeSeriesVectorPtr getFmiIoTData(const Settings &settings,
                                        const ParameterMapPtr &parameterMap,
                                        const Fmi::TimeZones &timezones);

  TS::TimeSeriesVectorPtr getTapsiQcData(const Settings &settings,
                                         const ParameterMapPtr &parameterMap,
                                         const Fmi::TimeZones &timezones);

  void shutdown();

  /**
   * @brief Get count of flashes are in the time interval
   * @param starttime Start of the time interval
   * @param endtime End of the time interval
   * @param locations Locations
   * @return FlashCounts The number of flashes in the interval
   */
  /*
  FlashCounts getFlashCount(const Fmi::DateTime &starttime,
                            const Fmi::DateTime &endtime,
                            const Spine::TaggedLocationList &locations);
  */

  size_t selectCount(const std::string &queryString);

  static ResultSetRows getResultSetForMobileExternalData(
      const pqxx::result &pgResultSet, const std::map<unsigned int, std::string> &pgDataTypes);

  void fetchLocationDataItems(const std::string &sqlStmt,
                              const StationInfo &stationInfo,
                              const std::set<std::string> &stationgroup_codes,
                              const TS::RequestLimits &requestLimits,
                              LocationDataItems &cacheData) override;
  std::string sqlSelectFromLocationDataItems(const Settings &settings,
                                             const std::string &params,
                                             const std::string &station_ids) const override;

 private:
  // Private members

  std::string srid;
  std::size_t itsMaxInsertSize;

  InsertStatus itsDataInsertCache;
  InsertStatus itsWeatherQCInsertCache;
  InsertStatus itsFlashInsertCache;
  InsertStatus itsRoadCloudInsertCache;
  InsertStatus itsNetAtmoInsertCache;
  InsertStatus itsFmiIoTInsertCache;
  InsertStatus itsTapsiQcInsertCache;
  const ExternalAndMobileProducerConfig &itsExternalAndMobileProducerConfig;

  // Private methods
  std::string stationType(const std::string &type);
  std::string stationType(Spine::Station &station);

  static void addSpecialParameterToTimeSeries(const std::string &paramname,
                                              TS::TimeSeriesVectorPtr &timeSeriesColumns,
                                              const Spine::Station &station,
                                              int pos,
                                              const std::string &stationtype,
                                              const Fmi::LocalDateTime &obstime);

  void addParameterToTimeSeries(
      TS::TimeSeriesVectorPtr &timeSeriesColumns,
      const std::pair<Fmi::LocalDateTime, std::map<std::string, TS::Value>> &dataItem,
      const std::map<std::string, int> &specialPositions,
      const std::map<std::string, std::string> &parameterNameMap,
      const std::map<std::string, int> &timeseriesPositions,
      const ParameterMapPtr &parameterMap,
      const std::string &stationtype,
      const Spine::Station &station,
      const std::string &missingtext);

  Fmi::DateTime getLatestTimeFromTable(const std::string &tablename, const std::string &time_field);
  Fmi::DateTime getOldestTimeFromTable(const std::string &tablename, const std::string &time_field);

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
  void createTapsiQcDataTable();
  void createIndex(const std::string &table,
                   const std::string &column,
                   const std::string &idx_name,
                   bool transaction = false) const;
  void dropIndex(const std::string &idx_name, bool transaction = false) const;

  Fmi::DateTime getTime(const std::string &timeQuery) const;
  TS::TimeSeriesVectorPtr getMobileAndExternalData(const Settings &settings,
                                                   const ParameterMapPtr &parameterMap,
                                                   const Fmi::TimeZones &timezones);
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
