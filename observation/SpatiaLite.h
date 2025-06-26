#pragma once

#include "CommonDatabaseFunctions.h"
#include "ExternalAndMobileProducerConfig.h"
#include "FlashDataItem.h"
#include "InsertStatus.h"
#include "MagnetometerDataItem.h"
#include "MobileExternalDataItem.h"
#include "MovingLocationItem.h"
#include "Utils.h"
#include "DataItem.h"

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
  SpatiaLite(const std::string &spatialiteFile, const SpatiaLiteCacheParameters &options);

  SpatiaLite() = delete;
  SpatiaLite(const SpatiaLite &other) = delete;
  SpatiaLite &operator=(const SpatiaLite &other) = delete;
  SpatiaLite(SpatiaLite &&other) = delete;
  SpatiaLite &operator=(SpatiaLite &&other) = delete;

  ~SpatiaLite() override;

  /**
   * @brief Get the time of the last modified  observation in observation_data table
   * @retval Fmi::DateTime The time of the last modification
   */

  Fmi::DateTime getLatestObservationModifiedTime();

  /**
   * @brief Get the time of the newest observation in observation_data table
   * @retval Fmi::DateTime The time of the newest observation
   */

  Fmi::DateTime getLatestObservationTime();

  /**
   * @brief Get the maximum value of flash_id field
   * @retval int Maximum value of flash_id_field
   */

  int getMaxFlashId();

  /**
   * @brief Get the time of the latest modified flash obervation
   * @retval Fmi::DateTime The time of the last modification
   */

  Fmi::DateTime getLatestFlashModifiedTime();

  /**
   * @brief Get the time of the newest flash observation
   * @retval Fmi::DateTime The time of the newest observation
   */

  Fmi::DateTime getLatestFlashTime();

  /**
   * @brief Get the time of the latest modified observation in weather_data_qc table
   * @retval Fmi::DateTime The time of the last modification
   */

  Fmi::DateTime getLatestWeatherDataQCModifiedTime();

  /**
   * @brief Get the time of the newest observation in weather_data_qc table
   * @retval Fmi::DateTime The time of the newest observation
   */
  Fmi::DateTime getLatestWeatherDataQCTime();

  /**
   * @brief Get the time of the oldest observation in observation_data table
   * @retval Fmi::DateTime The time of the oldest observation
   */

  Fmi::DateTime getOldestObservationTime();
  Fmi::DateTime getOldestFlashTime();

  /**
   * @brief Get the time of the oldest observation in weather_data_qc table
   * @retval Fmi::DateTime The time of the oldest observation
   */
  Fmi::DateTime getOldestWeatherDataQCTime();

  /**
   * @brief Create the SpatiaLite tables from scratch
   */

  void createTables(const std::set<std::string> &tables);

  void setConnectionId(int connectionId) { itsConnectionId = connectionId; }
  int connectionId() const { return itsConnectionId; }

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
  std::size_t fillMovingLocationsCache(const MovingLocationItems &cacheData,
                                       InsertStatus &insertStatus);

  /**
   * @brief Update weather_data_qc with data from Oracle's respective table
   *        which is used to store data from road and foreign stations
   * @param[in] cacheData Data from weather_data_qc.
   */
  std::size_t fillWeatherDataQCCache(const DataItems &cacheData,
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
                       const Fmi::DateTime &last_time);

  /**
   * @brief Delete everything from observation_data table which is older than the given duration
   * @param[in] newstarttime
   * @param[in] newstarttime_memory
   */
  void cleanDataCache(const Fmi::DateTime &newstarttime);

  /**
   * @brief Delete everything from moving_locations table which (edate) is older than the given
   * duration
   * @param[in] newstarttime
   * @param[in] newstarttime_memory
   */
  void cleanMovingLocationsCache(const Fmi::DateTime &newstarttime);

  /**
   * @brief Delete everything from weather_data_qc table which
   *        is older than given duration
   * @param[in] newstarttime
   */
  void cleanWeatherDataQCCache(const Fmi::DateTime &newstarttime);

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
  void cleanFlashDataCache(const Fmi::DateTime &newstarttime);

  /**
   * @brief Get the time of the newest observation in ext_obsdata_roadcloud table
   * @return Fmi::DateTime The time of the newest RoadCloud observation
   */

  Fmi::DateTime getLatestRoadCloudDataTime();

  /**
   * @brief Get latest creation time in ext_obsdata_roadcloud table
   * @return Fmi::DateTime The latest creation time of RoadCloud observation
   */

  Fmi::DateTime getLatestRoadCloudCreatedTime();

  /**
   * @brief Get oldest observation in ext_obsdata_roadcloud table
   * @return Fmi::DateTime The time of the oldest RoadCloud observation
   */

  Fmi::DateTime getOldestRoadCloudDataTime();

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
  void cleanRoadCloudCache(const Fmi::DateTime &newstarttime);

  /**
   * @brief Get newest observation in ext_obsdata_netatmo table
   * @return Fmi::DateTime The time of the newest NetAtmo observation
   */

  Fmi::DateTime getLatestNetAtmoDataTime();

  /**
   * @brief Get latest creation time in ext_obsdata_netatmo table
   * @return Fmi::DateTime The latest creation time
   */

  Fmi::DateTime getLatestNetAtmoCreatedTime();

  /**
   * @brief Get oldest observation in ext_obsdata_netatmo table

   * @return Fmi::DateTime The time of the oldest NetAtmo observation
   */

  Fmi::DateTime getOldestNetAtmoDataTime();

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
  void cleanNetAtmoCache(const Fmi::DateTime &newstarttime);

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
   * @brief Get the time of the oldest Magnetometer in observation_data table

   * @return Fmi::DateTime The time of the oldest Magnetometer observation
   */
  Fmi::DateTime getOldestMagnetometerDataTime();

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
  void cleanFmiIoTCache(const Fmi::DateTime &newstarttime);

  /**
   * @brief Insert cached observations into ext_obsdata table
   * @param TapsiQc observation data to be inserted into the table
   */
  std::size_t fillTapsiQcCache(const MobileExternalDataItems &mobileExternalCacheData,
                               InsertStatus &insertStatus);

  /**
   * @brief Delete old TapsiQc observation data from ext_obsdata table
   * @param timetokeep Delete TapsiQc data which is older than given duration
   */
  void cleanTapsiQcCache(const Fmi::DateTime &newstarttime);

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
  void cleanMagnetometerCache(const Fmi::DateTime &newstarttime);

  TS::TimeSeriesVectorPtr getRoadCloudData(const Settings &settings,
                                           const Fmi::TimeZones &timezones);

  TS::TimeSeriesVectorPtr getNetAtmoData(const Settings &settings, const Fmi::TimeZones &timezones);

  TS::TimeSeriesVectorPtr getFmiIoTData(const Settings &settings, const Fmi::TimeZones &timezones);

  TS::TimeSeriesVectorPtr getTapsiQcData(const Settings &settings, const Fmi::TimeZones &timezones);

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
   * @retval Fmi::DateTime The time of the last modification
   */

  Fmi::DateTime getLatestMagnetometerModifiedTime();

  /**
   * @brief Get the time of the newest observation in magnetometer_data table
   * @retval Fmi::DateTime The time of the newest observation
   */

  Fmi::DateTime getLatestMagnetometerDataTime();

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
  FlashCounts getFlashCount(const Fmi::DateTime &starttime,
                            const Fmi::DateTime &endtime,
                            const Spine::TaggedLocationList &locations) override;

  std::size_t selectCount(const std::string &queryString);

  /**
   * \brief Return latest flash data in a FlashDataItem vector
   * \param starttime Start time for the read
   * @retval Vector of FlashDataItems
   */

  FlashDataItems readFlashCacheData(const Fmi::DateTime &starttime);

  void fetchWeatherDataQCData(const std::string &sqlStmt,
                              const StationInfo &stationInfo,
                              const std::set<std::string> &stationgroup_codes,
                              const TS::RequestLimits &requestLimits,
                              LocationDataItems &cacheData) override;
  std::string sqlSelectFromWeatherDataQCData(const Settings &settings,
                                             const std::string &params,
                                             const std::string &station_ids) const override;

  std::string getWeatherDataQCParams(const std::set<std::string> &param_set) const override;

  void initObservationMemoryCache(
      const Fmi::DateTime &starttime,
      const std::unique_ptr<ObservationMemoryCache> &observationMemoryCache);

  TS::TimeSeriesVectorPtr getMagnetometerData(
      const Spine::Stations &stations,
      const Settings &settings,
      const StationInfo &stationInfo,
      const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
      const Fmi::TimeZones &timezones) override;

  void getMovingStations(Spine::Stations &stations,
                         const Settings &settings,
                         const std::string &wkt);

  Fmi::DateTime getLatestDataUpdateTime(const std::string &tablename,
                                        const Fmi::DateTime &starttime,
                                        const std::string &producer_ids,
                                        const std::string &measurand_ids);

 private:
  // Private members
  sqlite3pp::database itsDB;
  std::string srid;
  std::size_t itsConnectionId;
  std::size_t itsMaxInsertSize;
  void *cache;
  const ExternalAndMobileProducerConfig &itsExternalAndMobileProducerConfig;

  bool itsReadOnly = false;

  Fmi::DateTime getLatestTimeFromTable(const std::string &tablename, const std::string &time_field);
  Fmi::DateTime getOldestTimeFromTable(const std::string &tablename, const std::string &time_field);

  void initSpatialMetaData();
  void createMovingLocationsDataTable();
  void createObservationDataTable();
  void createWeatherDataQCTable();
  void createFlashDataTable();
  void createRoadCloudDataTable();
  void createNetAtmoDataTable();
  void createFmiIoTDataTable();
  void createTapsiQcDataTable();
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
