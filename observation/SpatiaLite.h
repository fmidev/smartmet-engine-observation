#pragma once

#include "CommonDatabaseFunctions.h"
#include "ExternalAndMobileProducerConfig.h"
#include "MovingLocationItem.h"
#include "FlashDataItem.h"
#include "InsertStatus.h"
#include "MagnetometerDataItem.h"
#include "MobileExternalDataItem.h"
#include "Utils.h"
#include "WeatherDataQCItem.h"

#ifdef __llvm__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wweak-vtables"
#endif
#include <sqlite3pp/sqlite3pp.h>
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

namespace Fmi
{
class DateTimeParser;
}

#define DATABASE_VERSION "2"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
struct SpatiaLiteCacheParameters;
class ObservationMemoryCache;

struct QueryMapping;

class SpatiaLite : public CommonDatabaseFunctions
{
 public:
  SpatiaLite() = delete;
  SpatiaLite(const SpatiaLite &other) = delete;
  SpatiaLite &operator=(const SpatiaLite &other) = delete;
  SpatiaLite(const std::string &spatialiteFile, const SpatiaLiteCacheParameters &options);

  ~SpatiaLite() override;

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
   * @brief Get the maximum value of flash_id field
   * @retval int Maximum value of flash_id_field
   */

  int getMaxFlashId();

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
   * @brief Update moving_locations with data from Oracle's
   *        moving_locations table which is used to store data
   *        from stations maintained by FMI.
   * @param[in] cacheData Data from moving_locations
   */
  std::size_t fillMovingLocationsCache(const MovingLocationItems &cacheData, InsertStatus &insertStatus);


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
  std::size_t fillFlashDataCache(const FlashDataItems &cacheData, InsertStatus &insertStatus);

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

  /**
   * @brief Delete everything from moving_locations table which (edate) is older than the given duration
   * @param[in] newstarttime
   * @param[in] newstarttime_memory
   */
  void cleanMovingLocationsCache(const boost::posix_time::ptime &newstarttime);

  /**
   * @brief Delete everything from weather_data_qc table which
   *        is older than given duration
   * @param[in] newstarttime
   */
  void cleanWeatherDataQCCache(const boost::posix_time::ptime &newstarttime);

  /*
  TS::TimeSeriesVectorPtr getWeatherDataQCData(
      const Spine::Stations &stations,
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
   * @brief Get the time of the newest observation in ext_obsdata_roadcloud table
   * @return boost::posix_time::ptime The time of the newest RoadCloud observation
   */

  boost::posix_time::ptime getLatestRoadCloudDataTime();

  /**
   * @brief Get latest creation time in ext_obsdata_roadcloud table
   * @return boost::posix_time::ptime The latest creation time of RoadCloud observation
   */

  boost::posix_time::ptime getLatestRoadCloudCreatedTime();

  /**
   * @brief Get oldest observation in ext_obsdata_roadcloud table
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
   * @brief Delete old observations from ext_obsdata_roadcloud table
   * @param timetokeep Delete RoadCloud data which is older than given duration
   */
  void cleanRoadCloudCache(const boost::posix_time::ptime &newstarttime);

  /**
   * @brief Get newest observation in ext_obsdata_netatmo table
   * @return boost::posix_time::ptime The time of the newest NetAtmo observation
   */

  boost::posix_time::ptime getLatestNetAtmoDataTime();

  /**
   * @brief Get latest creation time in ext_obsdata_netatmo table
   * @return boost::posix_time::ptime The latest creation time
   */

  boost::posix_time::ptime getLatestNetAtmoCreatedTime();

  /**
   * @brief Get oldest observation in ext_obsdata_netatmo table

   * @return boost::posix_time::ptime The time of the oldest NetAtmo observation
   */

  boost::posix_time::ptime getOldestNetAtmoDataTime();

  /**
   * @brief Insert cached observations into ext_obsdata_netatmo table
   * @param NetAtmo observation data to be inserted into the table
   */
  std::size_t fillNetAtmoCache(const MobileExternalDataItems &mobileExternalCacheData,
                               InsertStatus &insertStatus);

  /**
   * @brief Delete old observations from ext_obsdata_netatmo table
   * @param timetokeep Delete NetAtmo data which is older than given duration
   */
  void cleanNetAtmoCache(const boost::posix_time::ptime &newstarttime);

  /**
   * @brief Get the newest observation in ext_obsdata_bk_hydrometa table
   * @return boost::posix_time::ptime The time of the newest bk_hydrometa observation
   */

  boost::posix_time::ptime getLatestBKHydrometaDataTime();

  /**
   * @brief Get the latest creation time in ext_obsdata_bk_hydromate table
   * @return boost::posix_time::ptime The latest creation time
   */

  boost::posix_time::ptime getLatestBKHydrometaCreatedTime();

  /**
   * @brief Get the oldest observation in ext_obsdata_bk_hydrometa table

   * @return boost::posix_time::ptime The time of the oldest bk_hydrometa observation
   */

  boost::posix_time::ptime getOldestBKHydrometaDataTime();

  /**
   * @brief Insert cached observations into ext_obsdata_bk_hydrometa table
   * @param bk_hydrometa observation data to be inserted into the table
   */
  std::size_t fillBKHydrometaCache(const MobileExternalDataItems &mobileExternalCacheData,
                                   InsertStatus &insertStatus);

  /**
   * @brief Delete old bk_hydrometa observation data from ext_obsdata_roadcloud table
   * @param timetokeep Delete bk_hydrometa data which is older than given duration
   */
  void cleanBKHydrometaCache(const boost::posix_time::ptime &newstarttime);

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
   * @brief Get the time of the oldest Magnetometer in observation_data table

   * @return boost::posix_time::ptime The time of the oldest FmiIoT observation
   */

  boost::posix_time::ptime getOldestMagnetometerDataTime();

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

  /**
   * @brief Insert cached observations into magnetometer_data table
   * @param Magnetometer observation data to be inserted into the table
   */
  std::size_t fillMagnetometerDataCache(const MagnetometerDataItems &magnetometerCacheData,
                                        InsertStatus &insertStatus);

  /**
   * @brief Delete old Magnetometer observation data from magnetometer_data table
   * @param timetokeep Delete magnetometer data which is older than given duration
   */
  void cleanMagnetometerCache(const boost::posix_time::ptime &newstarttime);

  TS::TimeSeriesVectorPtr getRoadCloudData(const Settings &settings,
                                           const Fmi::TimeZones &timezones);

  TS::TimeSeriesVectorPtr getNetAtmoData(const Settings &settings, const Fmi::TimeZones &timezones);

  TS::TimeSeriesVectorPtr getBKHydrometaData(const Settings &settings,
                                             const Fmi::TimeZones &timezones);

  TS::TimeSeriesVectorPtr getFmiIoTData(const Settings &settings, const Fmi::TimeZones &timezones);

  TS::TimeSeriesVectorPtr getFlashData(const Settings &settings,
                                       const Fmi::TimeZones &timezones) override;

  TS::TimeSeriesVectorPtr getObservationData(
											 const Spine::Stations &stations,
											 const Settings &settings,
											 const StationInfo &stationInfo,
											 const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
											 const Fmi::TimeZones &timezones,
											 const std::unique_ptr<ObservationMemoryCache> &observationMemoryCache) override;

  TS::TimeSeriesVectorPtr getObservationDataForMovingStations(
															  const Settings &settings,
															  const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
															  const Fmi::TimeZones &timezones) override;

  LocationDataItems readObservationDataOfMovingStationsFromDB(
															  const Settings &settings,
															  const QueryMapping &qmap,
															  const std::set<std::string> &stationgroup_codes);


  /**
   * @brief Get the time of the last modified observation in magnetometer_data table
   * @retval boost::posix_time::ptime The time of the last modification
   */

  boost::posix_time::ptime getLatestMagnetometerModifiedTime();

  /**
   * @brief Get the time of the newest observation in magnetometer_data table
   * @retval boost::posix_time::ptime The time of the newest observation
   */

  boost::posix_time::ptime getLatestMagnetometerDataTime();

  /**
   * @brief Get the maximum value of flash_id field
   * @retval int Maximum value of flash_id_field
   */

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
                            const Spine::TaggedLocationList &locations) override;

  std::size_t selectCount(const std::string &queryString);

  /**
   * \brief Return latest flash data in a FlashDataItem vector
   * \param starttime Start time for the read
   * @retval Vector of FlashDataItems
   */

  FlashDataItems readFlashCacheData(const boost::posix_time::ptime &starttime);

  void fetchWeatherDataQCData(const std::string &sqlStmt,
                              const StationInfo &stationInfo,
                              const std::set<std::string> &stationgroup_codes,
							  const Spine::RequestLimits& requestLimits,									  
                              WeatherDataQCData &cacheData) override;
  std::string sqlSelectFromWeatherDataQCData(const Settings &settings,
                                             const std::string &params,
                                             const std::string &station_ids) const override;

  std::string getWeatherDataQCParams(const std::set<std::string> &param_set) const override;

  void initObservationMemoryCache(
      const boost::posix_time::ptime &starttime,
      const std::unique_ptr<ObservationMemoryCache> &observationMemoryCache);

  TS::TimeSeriesVectorPtr getMagnetometerData(
      const Spine::Stations &stations,
      const Settings &settings,
      const StationInfo &stationInfo,
      const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
      const Fmi::TimeZones &timezones) override;

  void getMovingStations(Spine::Stations &stations,
						 const std::string &stationtype,
						 const boost::posix_time::ptime &startTime,
						 const boost::posix_time::ptime &endTime,
						 const std::string &wkt);
 private:
  // Private members
  sqlite3pp::database itsDB;
  std::string srid;
  std::size_t itsConnectionId;
  std::size_t itsMaxInsertSize;
  const ExternalAndMobileProducerConfig &itsExternalAndMobileProducerConfig;

  bool itsReadOnly = false;

  boost::posix_time::ptime getLatestTimeFromTable(const std::string &tablename,
                                                  const std::string &time_field);
  boost::posix_time::ptime getOldestTimeFromTable(const std::string &tablename,
                                                  const std::string &time_field);

  void initSpatialMetaData();
  void createMovingLocationsDataTable();
  void createObservationDataTable();
  void createWeatherDataQCTable();
  void createFlashDataTable();
  void createRoadCloudDataTable();
  void createNetAtmoDataTable();
  void createFmiIoTDataTable();
  void createBKHydrometaDataTable();
  void createMagnetometerDataTable();

  TS::TimeSeriesVectorPtr getMobileAndExternalData(const Settings &settings,
                                                   const Fmi::TimeZones &timezones);

  LocationDataItems readObservationDataFromDB(const Spine::Stations &stations,
                                              const Settings &settings,
                                              const StationInfo &stationInfo,
                                              const QueryMapping &qmap,
                                              const std::set<std::string> &stationgroup_codes);
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
