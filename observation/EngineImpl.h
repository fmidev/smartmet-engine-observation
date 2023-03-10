#pragma once

#include "DatabaseDriverInterface.h"
#include "Engine.h"
#include "EngineParameters.h"
#include "ObservationCache.h"
#include "StationOptions.h"
#include <spine/Table.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class EngineImpl : public Engine
{
 public:
  EngineImpl() = delete;
  EngineImpl(const std::string &configfile);

  TS::TimeSeriesVectorPtr values(Settings &settings) override;
  TS::TimeSeriesVectorPtr values(Settings &settings,
                                 const TS::TimeSeriesGeneratorOptions &timeSeriesOptions) override;

  void makeQuery(QueryBase *qb) override;

  FlashCounts getFlashCount(const boost::posix_time::ptime &starttime,
                            const boost::posix_time::ptime &endtime,
                            const Spine::TaggedLocationList &locations) override;
  std::shared_ptr<std::vector<ObservableProperty>> observablePropertyQuery(
      std::vector<std::string> &parameters, const std::string language) override;

  bool ready() const override;

  Geonames::Engine *getGeonames() const override;

  const std::shared_ptr<DBRegistry> dbRegistry() const override { return itsDatabaseRegistry; }
  void reloadStations() override;
  void getStations(Spine::Stations &stations, Settings &settings) override;

  void getStationsByArea(Spine::Stations &stations,
                         const std::string &stationtype,
                         const boost::posix_time::ptime &starttime,
                         const boost::posix_time::ptime &endtime,
                         const std::string &areaWkt) override;

  void getStationsByBoundingBox(Spine::Stations &stations, const Settings &settings) override;

  /* \brief Test if the given alias name is configured and it has a field for
   * the stationType.
   * \param[in] alias Alias name of a meteorological parameter.
   * \param[in] stationType Station type to use for the alias.
   * \retval true The alias exist and it has configuration for the stationType.
   * \retval false The alias is not configured or there isn't a field for the
   * stationType inside of
   * the alias configuration block.
   */

  bool isParameter(const std::string &alias,
                   const std::string &stationType = "unknown") const override;

  /* \brief Test if the given alias name is configured
   * \param[in] name Alias name of a meteorological parameter.
   * \retval true The alias exist and it has configuration for the stationType.
   * \retval false The alias is not configured or there isn't a field for the
   * stationType inside of
   * the alias configuration block.
   */

  bool isParameterVariant(const std::string &name) const override;

  /* \brief Get a numerical identity for an given alias name.
   * \param[in] alias Alias name of a meteorological parameter (case insensitive).
   * \param[in] stationType Station type to use for the alias (case
   * insensitive).
   * \return Positive integer in success and zero if there is no a match.
   */
  uint64_t getParameterId(const std::string &alias,
                          const std::string &stationType = "unknown") const override;

  /* \brief Get parameter id as string for an given alias name.
   * \param[in] alias Alias name of a meteorological parameter (case insensitive).
   * \param[in] stationType Station type to use for the alias (case
   * insensitive).
   * \return String
   */

  std::string getParameterIdAsString(const std::string &alias,
                                     const std::string &stationType = "unknown") const override;

  /* \brief Get valid parameter names
   * \return Set of parameter names
   */

  std::set<std::string> getValidStationTypes() const override;

  /* \brief Get detailed info of producer(s)
   * \param[in] producer If producer is given return info only of that producer, otherwise of all
   * producers \return Info of producer(s)
   */
  ContentTable getProducerInfo(boost::optional<std::string> producer) const override;

  /* \brief Get parameter info of producer(s)
   * \param[in] producer If producer is given return info only of that producer, otherwise of all
   * producers \return Parameter info of producer(s)
   */
  ContentTable getParameterInfo(boost::optional<std::string> producer) const override;

  /* \brief Get station info
   * \param[in] StationOptions Defines query options
   * \return The requested station info
   */
  ContentTable getStationInfo(const StationOptions &options) const override;

  MetaData metaData(const std::string &producer) const override;

  /* \brief Translates WMO,RWID,LPNN,GEOID,Bounding box to FMISID
   * \return List of FMISIDs
   */
  Spine::TaggedFMISIDList translateToFMISID(const boost::posix_time::ptime &starttime,
                                            const boost::posix_time::ptime &endtime,
                                            const std::string &stationtype,
                                            const StationSettings &stationSettings) const override;

 protected:
  void init() override;
  void shutdown() override;

 private:
  ~EngineImpl() override = default;

  void initializeCache();
  bool stationHasRightType(const Spine::Station &station, const Settings &settings);
  void unserializeStations();
  Settings beforeQuery(const Settings &settings,
                       std::vector<unsigned int> &unknownParameterIndexes) const;
  void afterQuery(TS::TimeSeriesVectorPtr &tsvPtr,
                  const Settings &settings,
                  const std::vector<unsigned int> &unknownParameterIndexes) const;

  Fmi::Cache::CacheStatistics getCacheStats() const override;

  std::string itsConfigFile;

  EngineParametersPtr itsEngineParameters;

  std::shared_ptr<DBRegistry> itsDatabaseRegistry;

  std::unique_ptr<DatabaseDriverInterface> itsDatabaseDriver;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
