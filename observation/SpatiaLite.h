#pragma once

#include "CommonDatabaseFunctions.h"
#include "ExternalAndMobileProducerConfig.h"
#include "FlashDataItem.h"
#include "InsertStatus.h"
#include "MobileExternalDataItem.h"
#include "ObservationMemoryCache.h"
#include "Utils.h"
#include "WeatherDataQCItem.h"
#include <spine/Value.h>

#ifdef __llvm__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wweak-vtables"
#endif
#include "sqlite3pp.h"
#ifdef __llvm__
#pragma clang diagnostic pop
#endif

// clang-format off
namespace sqlite_api
{
#include <sqlite3.h>
#include <spatialite.h>
}
// clang-format on

#define DATABASE_VERSION "2"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class SpatiaLiteCacheParameters;

struct ObservationsMap;
struct QueryMapping;

class SpatiaLite : public CommonDatabaseFunctions, private boost::noncopyable
{
 public:
  SpatiaLite(const std::string &spatialiteFile, const SpatiaLiteCacheParameters &options);

  ~SpatiaLite();

  /**
   * @brief Get the time of the last modified  observation in observation_data table
   * @retval boost::posix_time::ptime The time of the last modification
   */

  boost::posix_time::ptime getLatestObservationModifiedTime();

  /**
   * @brief Get the time of the newest observation in observation_data table
   * @retval boost::posix_time::ptime The time of the newest observation
   */

  boost::posix_time::ptime getLatestObservationTime();

  /**
   * @brief Get the time of the latest modified flash obervation
   * @retval boost::posix_time::ptime The time of the last modification
   */

  boost::posix_time::ptime getLatestFlashModifiedTime();

  /**
   * @brief Get the time of the newest flash observation
   * @retval boost::posix_time::ptime The time of the newest observation
   */

  boost::posix_time::ptime getLatestFlashTime();

  /**
   * @brief Get the time of the latest modified observation in weather_data_qc table
   * @retval boost::posix_time::ptime The time of the last modification
   */

  boost::posix_time::ptime getLatestWeatherDataQCModifiedTime();

  /**
   * @brief Get the time of the newest observation in weather_data_qc table
   * @retval boost::posix_time::ptime The time of the newest observation
   */
  boost::posix_time::ptime getLatestWeatherDataQCTime();

  /**
   * @brief Get the time of the oldest observation in observation_data table
   * @retval boost::posix_time::ptime The time of the oldest observation
   */

  boost::posix_time::ptime getOldestObservationTime();
  boost::posix_time::ptime getOldestFlashTime();

  /**
   * @brief Get the time of the oldest observation in weather_data_qc table
   * @retval boost::posix_time::ptime The time of the oldest observation
   */
  boost::posix_time::ptime getOldestWeatherDataQCTime();

  /**
   * @brief Create the SpatiaLite tables from scratch
   */

  void createTables(const std::set<std::string> &tables);

  void setConnectionId(int connectionId) { itsConnectionId = connectionId; }
  int connectionId() { return itsConnectionId; }

  /**
   * @brief Update observation_data with data from Oracle's
   *        observation_data table which is used to store data
   *        from stations maintained by FMI.
   * @param[in] cacheData Data from observation_data.
   */
  std::size_t fillDataCache(const DataItems &cacheData, InsertStatus &insertStatus);

  /**
   * @brief Update weather_data_qc with data from Oracle's respective table
   *        which is used to store data from road and foreign stations
   * @param[in] cacheData Data from weather_data_qc.
   */
  std::size_t fillWeatherDataQCCache(const WeatherDataQCItems &cacheData,
                                     InsertStatus &insertStatus);

  /**
   * @brief Insert cached observations into observation_data table
   * @param cacheData Observation data to be inserted into the table
   */
  std::size_t fillFlashDataCache(const FlashDataItems &flashCacheData, InsertStatus &insertStatus);

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
                       const boost::posix_time::ptime &last_time);

  /**
   * @brief Delete everything from observation_data table which is older than the given duration
   * @param[in] newstarttime
   * @param[in] newstarttime_memory
   */
  void cleanDataCache(const boost::posix_time::ptime &newstarttime);
  void cleanMemoryDataCache(const boost::posix_time::ptime &newstarttime);

  /**
   * @brief Delete everything from weather_data_qc table which
   *        is older than given duration
   * @param[in] newstarttime
   */
  void cleanWeatherDataQCCache(const boost::posix_time::ptime &newstarttime);

  /*
  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr getWeatherDataQCData(
      const SmartMet::Spine::Stations &stations,
      const Settings &settings,
      const StationInfo &stationInfo,
      const Fmi::TimeZones &timezones);
  */
  /**
   * @brief Delete old flash observation data from flash_data table
   * @param newstarttime Delete everything from flash_data which is older than given time
   */
  void cleanFlashDataCache(const boost::posix_time::ptime &newstarttime);

  /**
   * @brief Get the time of the newest RoadCloud observation in ext_obsdata_roadcloud table
   * @return boost::posix_time::ptime The time of the newest RoadCloud observation
   */

  boost::posix_time::ptime getLatestRoadCloudDataTime();

  /**
   * @brief Get the time of the latest RoadCloud creation in ext_obsdata_roadcloud table
   * @return boost::posix_time::ptime The latest creation time of RoadCloud observation
   */

  boost::posix_time::ptime getLatestRoadCloudCreatedTime();

  /**
   * @brief Get the time of the oldest RoadCloud observation in ext_obsdata_roadcloud table
   * @return boost::posix_time::ptime The time of the oldest RoadCloud observation
   */

  boost::posix_time::ptime getOldestRoadCloudDataTime();

  /**
   * @brief Insert cached observations into ext_obsdata_roadcloud table
   * @param RoadCloud observation data to be inserted into the table
   */
  std::size_t fillRoadCloudCache(const MobileExternalDataItems &mobileExternalCacheData,
                                 InsertStatus &insertStatus);

  /**
   * @brief Delete old RoadCloud observation data from ext_obsdata_roadcloud table
   * @param timetokeep Delete RoadCloud data which is older than given duration
   */
  void cleanRoadCloudCache(const boost::posix_time::ptime &newstarttime);

  /**
   * @brief Get the time of the newest NetAtmo observation in ext_obsdata_roadcloud table
   * @return boost::posix_time::ptime The time of the newest NetAtmo observation
   */

  boost::posix_time::ptime getLatestNetAtmoDataTime();

  /**
   * @brief Get the time of the latest NetAtmo creation time in ext_obsdata table
   * @return boost::posix_time::ptime The latest creation time
   */

  boost::posix_time::ptime getLatestNetAtmoCreatedTime();

  /**
   * @brief Get the time of the oldest NetAtmo observation in ext_obsdata_roadcloud table

   * @return boost::posix_time::ptime The time of the oldest NetAtmo observation
   */

  boost::posix_time::ptime getOldestNetAtmoDataTime();

  /**
   * @brief Insert cached observations into ext_obsdata_roadcloud table
   * @param NetAtmo observation data to be inserted into the table
   */
  std::size_t fillNetAtmoCache(const MobileExternalDataItems &mobileExternalCacheData,
                               InsertStatus &insertStatus);

  /**
   * @brief Delete old NetAtmo observation data from ext_obsdata_roadcloud table
   * @param timetokeep Delete NetAtmo data which is older than given duration
   */
  void cleanNetAtmoCache(const boost::posix_time::ptime &newstarttime);

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
  std::size_t fillFmiIoTCache(const MobileExternalDataItems &mobileExternalCacheData,
                              InsertStatus &insertStatus);

  /**
   * @brief Delete old FmiIoT observation data from ext_obsdata_roadcloud table
   * @param timetokeep Delete FmiIoT data which is older than given duration
   */
  void cleanFmiIoTCache(const boost::posix_time::ptime &newstarttime);

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr getRoadCloudData(
      const Settings &settings, const Fmi::TimeZones &timezones);

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr getNetAtmoData(const Settings &settings,
                                                                  const Fmi::TimeZones &timezones);

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr getFmiIoTData(const Settings &settings,
                                                                 const Fmi::TimeZones &timezones);

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr getFlashData(const Settings &settings,
                                                                const Fmi::TimeZones &timezones);

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr getObservationData(
      const SmartMet::Spine::Stations &stations,
      const Settings &settings,
      const StationInfo &stationInfo,
      const SmartMet::Spine::TimeSeriesGeneratorOptions &timeSeriesOptions,
      const Fmi::TimeZones &timezones);

  void shutdown();

  /**
   * @brief Get count of flashes are in the time interval
   * @param starttime Start of the time interval
   * @param endtime End of the time interval
   * @param boundingBox The bounding box. Must have crs EPSG:4326.
   * @retval FlashCounts The number of flashes in the interval
   */
  FlashCounts getFlashCount(const boost::posix_time::ptime &starttime,
                            const boost::posix_time::ptime &endtime,
                            const SmartMet::Spine::TaggedLocationList &locations);

  std::size_t selectCount(const std::string &queryString);

  /**
   * \brief Return latest flash data in a FlashDataItem vector
   * \param starttime Start time for the read
   * @retval Vector of FlashDataItems
   */

  FlashDataItems readFlashCacheData(const boost::posix_time::ptime &starttime);

  /**
   * \brief Init the internal memory cache from SpatiaLite
   * \param starttime Start time for the update
   */

  void initObservationMemoryCache(const boost::posix_time::ptime &starttime);

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
  sqlite3pp::database itsDB;
  std::string srid;
  std::size_t itsConnectionId;
  std::size_t itsMaxInsertSize;
  const ExternalAndMobileProducerConfig &itsExternalAndMobileProducerConfig;

  std::unique_ptr<ObservationMemoryCache> itsObservationMemoryCache;
  boost::atomic<bool> itsShutdownRequested;

  boost::posix_time::ptime getLatestTimeFromTable(const std::string &tablename,
                                                  const std::string &time_field);
  boost::posix_time::ptime getOldestTimeFromTable(const std::string &tablename,
                                                  const std::string &time_field);

  void initSpatialMetaData();
  void createObservationDataTable();
  void createWeatherDataQCTable();
  void createFlashDataTable();
  void createRoadCloudDataTable();
  void createNetAtmoDataTable();
  void createFmiIoTDataTable();

  boost::posix_time::ptime parseSqliteTime(sqlite3pp::query::iterator &iter, int column) const;
  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr getMobileAndExternalData(
      const Settings &settings, const Fmi::TimeZones &timezones);

  LocationDataItems readObservationDataFromDB(const Spine::Stations &stations,
                                              const Settings &settings,
                                              const StationInfo &stationInfo,
                                              const QueryMapping &qmap,
                                              const std::set<std::string> &stationgroup_codes);
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
