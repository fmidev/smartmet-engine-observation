#pragma once

#include "DatabaseDriverInterface.h"
#include "EngineParameters.h"
#include "ObservationCache.h"
#include "StationOptions.h"
#include <spine/Table.h>
#include <spine/Value.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
using ContentTable = std::pair<boost::shared_ptr<Spine::Table>, Spine::TableFormatter::Names>;

class DBRegistry;
class QueryBase;

class Engine : public SmartMet::Spine::SmartMetEngine
{
 public:
  Engine() = delete;
  Engine(const std::string &configfile);

  Spine::TimeSeries::TimeSeriesVectorPtr values(Settings &settings);
  Spine::TimeSeries::TimeSeriesVectorPtr values(
      Settings &settings, const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions);

  void makeQuery(QueryBase *qb);

  FlashCounts getFlashCount(const boost::posix_time::ptime &starttime,
                            const boost::posix_time::ptime &endtime,
                            const Spine::TaggedLocationList &locations);
  std::shared_ptr<std::vector<ObservableProperty>> observablePropertyQuery(
      std::vector<std::string> &parameters, const std::string language);

  bool ready() const;

  Geonames::Engine *getGeonames() const;

  const std::shared_ptr<DBRegistry> dbRegistry() const { return itsDatabaseRegistry; }
  void reloadStations();
  void getStations(Spine::Stations &stations, Settings &settings);

  void getStationsByArea(Spine::Stations &stations,
                         const std::string &stationtype,
                         const boost::posix_time::ptime &starttime,
                         const boost::posix_time::ptime &endtime,
                         const std::string &areaWkt);

  void getStationsByBoundingBox(Spine::Stations &stations, const Settings &settings);

  /* \brief Test if the given alias name is configured and it has a field for
   * the stationType.
   * \param[in] alias Alias name of a meteorological parameter.
   * \param[in] stationType Station type to use for the alias.
   * \retval true The alias exist and it has configuration for the stationType.
   * \retval false The alias is not configured or there isn't a field for the
   * stationType inside of
   * the alias configuration block.
   */

  bool isParameter(const std::string &alias, const std::string &stationType = "unknown") const;

  /* \brief Test if the given alias name is configured
   * \param[in] name Alias name of a meteorological parameter.
   * \retval true The alias exist and it has configuration for the stationType.
   * \retval false The alias is not configured or there isn't a field for the
   * stationType inside of
   * the alias configuration block.
   */

  bool isParameterVariant(const std::string &name) const;

  /* \brief Get a numerical identity for an given alias name.
   * \param[in] alias Alias name of a meteorological parameter (case insensitive).
   * \param[in] stationType Station type to use for the alias (case
   * insensitive).
   * \return Positive integer in success and zero if there is no a match.
   */
  uint64_t getParameterId(const std::string &alias,
                          const std::string &stationType = "unknown") const;

  /* \brief Get parameter id as string for an given alias name.
   * \param[in] alias Alias name of a meteorological parameter (case insensitive).
   * \param[in] stationType Station type to use for the alias (case
   * insensitive).
   * \return String
   */

  std::string getParameterIdAsString(const std::string &alias,
                                     const std::string &stationType = "unknown") const;

  /* \brief Get valid parameter names
   * \return Set of parameter names
   */

  std::set<std::string> getValidStationTypes() const;

  /* \brief Get detailed info of producer(s)
   * \param[in] producer If producer is given return info only of that producer, otherwise of all
   * producers \return Info of producer(s)
   */
  ContentTable getProducerInfo(boost::optional<std::string> producer) const;

  /* \brief Get parameter info of producer(s)
   * \param[in] producer If producer is given return info only of that producer, otherwise of all
   * producers \return Parameter info of producer(s)
   */
  ContentTable getParameterInfo(boost::optional<std::string> producer) const;

  /* \brief Get station info
   * \param[in] StationOptions Defines query options
   * \return The requested station info
   */
  ContentTable getStationInfo(const StationOptions &options) const;

  MetaData metaData(const std::string &producer) const;

  // Translates WMO,RWID,LPNN,GEOID,Bounding box to FMISID
  Spine::TaggedFMISIDList translateToFMISID(const boost::posix_time::ptime &starttime,
                                            const boost::posix_time::ptime &endtime,
                                            const std::string &stationtype,
                                            const StationSettings &stationSettings) const;

 protected:
  void init();
  void shutdown();

 private:
  ~Engine() = default;

  void initializeCache();
  bool stationHasRightType(const Spine::Station &station, const Settings &settings);
  void unserializeStations();
  Settings beforeQuery(const Settings &settings,
                       std::vector<unsigned int> &unknownParameterIndexes) const;
  void afterQuery(Spine::TimeSeries::TimeSeriesVectorPtr &tsvPtr,
                  const Settings &settings,
                  const std::vector<unsigned int> &unknownParameterIndexes) const;

  std::string itsConfigFile;

  EngineParametersPtr itsEngineParameters;

  std::shared_ptr<DBRegistry> itsDatabaseRegistry;

#ifdef TODO_CAUSES_SEGFAULT_AT_EXIT
  std::unique_ptr<DatabaseDriverInterface> itsDatabaseDriver;
#else
  DatabaseDriverInterface *itsDatabaseDriver{nullptr};
#endif
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
