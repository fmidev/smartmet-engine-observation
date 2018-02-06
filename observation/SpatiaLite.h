#pragma once

#include "DataItem.h"
#include "FlashDataItem.h"
#include "InsertStatus.h"
#include "LocationItem.h"
#include "Settings.h"
#include "SpatiaLiteOptions.h"
#include "StationInfo.h"
#include "Utils.h"
#include "WeatherDataQCItem.h"
#include "sqlite3pp.h"

// clang-format off
namespace sqlite_api
{
#include <sqlite3.h>
#include <spatialite.h>
}
// clang-format on

#define DATABASE_VERSION "2"

#include <macgyver/TimeFormatter.h>
#include <macgyver/TimeZones.h>
#include <spine/Location.h>
#include <spine/Station.h>
#include <spine/Thread.h>
#include <spine/TimeSeries.h>
#include <spine/TimeSeriesGenerator.h>
#include <spine/TimeSeriesGeneratorOptions.h>
#include <spine/Value.h>
#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class ObservableProperty;
class SpatiaLiteCacheParameters;

class SpatiaLite : private boost::noncopyable
{
 public:
  using ParameterMap = std::map<std::string, std::map<std::string, std::string>>;

  SpatiaLite(const std::string &spatialiteFile, const SpatiaLiteCacheParameters &options);

  ~SpatiaLite();

  /**
   * @brief Get the time of the newest observation in observation_data table
   * @retval boost::posix_time::ptime The time of the newest observation
   */

  boost::posix_time::ptime getLatestObservationTime();
  boost::posix_time::ptime getLatestFlashTime();

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

  void createTables();

  /**
   * @brief Return the number of rows in the stations table
   */

  size_t getStationCount();

  void updateStationsAndGroups(const StationInfo &info);

  SmartMet::Spine::Stations findAllStationsFromGroups(
      const std::set<std::string> stationgroup_codes,
      const StationInfo &info,
      const boost::posix_time::ptime &starttime,
      const boost::posix_time::ptime &endtime);

  // Deprecated methods. These are way too slow, use the respective methods in
  // Engine instead.
  SmartMet::Spine::Stations findNearestStations(
      double latitude,
      double longitude,
      const std::map<int, SmartMet::Spine::Station> &stationIndex,
      int maxdistance,
      int numberofstations,
      const std::set<std::string> &stationgroup_codes,
      const boost::posix_time::ptime &starttime,
      const boost::posix_time::ptime &endtime);

  SmartMet::Spine::Stations findNearestStations(
      const SmartMet::Spine::LocationPtr &location,
      const std::map<int, SmartMet::Spine::Station> &stationIndex,
      const int maxdistance,
      const int numberofstations,
      const std::set<std::string> &stationgroup_codes,
      const boost::posix_time::ptime &starttime,
      const boost::posix_time::ptime &endtime);

  void setConnectionId(int connectionId) { itsConnectionId = connectionId; }
  int connectionId() { return itsConnectionId; }
  /**
   * @brief Insert new stations or update old ones in locations table.
   * @param[in] Vector of locations
   */
  void fillLocationCache(const std::vector<LocationItem> &locations);

  /**
   * @brief Update observation_data with data from Oracle's
   *        observation_data table which is used to store data
   *        from stations maintained by FMI.
   * @param[in] cacheData Data from observation_data.
   */
  std::size_t fillDataCache(const std::vector<DataItem> &cacheData);

  /**
   * @brief Update weather_data_qc with data from Oracle's respective table
   *        which is used to store data from road and foreign stations
   * @param[in] cacheData Data from weather_data_qc.
   */
  std::size_t fillWeatherDataQCCache(const std::vector<WeatherDataQCItem> &cacheData);

  /**
   * @brief Insert cached observations into observation_data table
   * @param cacheData Observation data to be inserted into the table
   */
  std::size_t fillFlashDataCache(const std::vector<FlashDataItem> &flashCacheData);

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
   *        older than last_time
   * @param[in] last_time
   */
  void cleanDataCache(const boost::posix_time::ptime &last_time);

  /**
   * @brief Delete everything from weather_data_qc table which
   *        is older than last_time
   * @param[in] last_time
   */
  void cleanWeatherDataQCCache(const boost::posix_time::ptime &last_time);

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr getCachedWeatherDataQCData(
      const SmartMet::Spine::Stations &stations,
      const Settings &settings,
      const ParameterMap &parameterMap,
      const Fmi::TimeZones &timezones);

  /**
   * @brief Delete old flash observation data from flash_data table
   * @param timetokeep Delete everything from flash_data which is older than
   * last_time
   */
  void cleanFlashDataCache(const boost::posix_time::ptime &timetokeep);

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr getCachedData(
      const SmartMet::Spine::Stations &stations,
      const Settings &settings,
      const ParameterMap &parameterMap,
      const Fmi::TimeZones &timezones);

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr getCachedFlashData(
      const Settings &settings, const ParameterMap &parameterMap, const Fmi::TimeZones &timezones);

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr getCachedWeatherDataQCData(
      const SmartMet::Spine::Stations &stations,
      const Settings &settings,
      const ParameterMap &parameterMap,
      const SmartMet::Spine::TimeSeriesGeneratorOptions &timeSeriesOptions,
      const Fmi::TimeZones &timezones);

  SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr getCachedData(
      SmartMet::Spine::Stations &stations,
      Settings &settings,
      ParameterMap &parameterMap,
      const SmartMet::Spine::TimeSeriesGeneratorOptions &timeSeriesOptions,
      const Fmi::TimeZones &timezones);

  SmartMet::Spine::Stations findStationsInsideArea(const Settings &settings,
                                                   const std::string &areaWkt,
                                                   const StationInfo &info);

  SmartMet::Spine::Stations findStationsInsideBox(const Settings &settings,
                                                  const StationInfo &info);

  SmartMet::Spine::Stations findStationsByWMO(const Settings &settings, const StationInfo &info);
  SmartMet::Spine::Stations findStationsByLPNN(const Settings &settings, const StationInfo &info);

  /**
   * @brief Fill station_id, fmisid, wmo, geoid, lpnn, longitude_out and
   * latitude_out into the
   *        station object if value is missing.
   *        Some id  (station_id, fmisid, wmo, lpnn or geoid) must be defined in
   * the Station object.
   * @param[in,out] s Data is filled to this object if some id is present.
   * @param[in] stationgroup_codes Station match requires a correct station
   * group
   *        If the stationgroup_codes list is empty the station group is not
   * used.
   * @retval true If the data is filled successfully.
   * @retval false If the data is not filled at all.
   */
  bool fillMissing(SmartMet::Spine::Station &s, const std::set<std::string> &stationgroup_codes);

  /**
   * @brief Get the station odered by \c station_id.
   * @param station_id Primary identity of the requested station.
   *        \c station_id is the same value as the station fmisid value.
   * @param[in] stationgroup_codes Station match requires a correct station
   * group
   *        If the stationgroup_codes list is empty the station group is not
   * used.
   * @retval true If the station is found and data stored into the given object.
   * @retval false If the station is no found.
   */
  bool getStationById(SmartMet::Spine::Station &station,
                      int station_id,
                      const std::set<std::string> &stationgroup_codes);

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

  /**
   * @brief Get observable properties
   * @param parameters
   * @param language
   * @param parameterMap
   * @param stationType
   * @retval Shared pointer to vector of observable properties
   */
  boost::shared_ptr<std::vector<ObservableProperty>> getObservableProperties(
      std::vector<std::string> &parameters,
      const std::string language,
      const std::map<std::string, std::map<std::string, std::string>> &parameterMap,
      const std::string &stationType);

  size_t selectCount(const std::string &queryString);

 private:
  // Private members
  sqlite3pp::database itsDB;
  std::string srid;
  bool itsShutdownRequested;
  std::size_t itsConnectionId;
  std::size_t itsMaxInsertSize;
  std::map<std::string, std::string> stationTypeMap;

  InsertStatus itsDataInsertCache;
  InsertStatus itsWeatherQCInsertCache;
  InsertStatus itsFlashInsertCache;

  // Private methods

  std::string stationType(const std::string &type);
  std::string stationType(SmartMet::Spine::Station &station);

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
      const ParameterMap &parameterMap,
      const std::string &stationtype,
      const SmartMet::Spine::Station &station);

  void addEmptyValuesToTimeSeries(
      SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns,
      const boost::local_time::local_date_time &obstime,
      const std::map<std::string, int> &specialPositions,
      const std::map<std::string, std::string> &parameterNameMap,
      const std::map<std::string, int> &timeseriesPositions,
      const std::string &stationtype,
      const SmartMet::Spine::Station &station);

  void updateStations(const SmartMet::Spine::Stations &stations);
  void updateStationGroups(const StationInfo &info);

  boost::posix_time::ptime getLatestTimeFromTable(std::string tablename, std::string time_field);
  boost::posix_time::ptime getOldestTimeFromTable(std::string tablename, std::string time_field);

  void initSpatialMetaData();
  void createStationTable();
  void createStationGroupsTable();
  void createGroupMembersTable();
  void createLocationsTable();
  void createObservationDataTable();
  void createWeatherDataQCTable();
  void createFlashDataTable();
  void createObservablePropertyTable();
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
