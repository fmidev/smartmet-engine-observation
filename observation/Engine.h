#pragma once

#include "DatabaseDriver.h"
#include "EngineParameters.h"
#include "ObservationCache.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class DBRegistry;
class QueryBase;

class Engine : public SmartMet::Spine::SmartMetEngine
{
 public:
  Engine(const std::string &configfile);

  Spine::TimeSeries::TimeSeriesVectorPtr values(Settings &settings);
  Spine::TimeSeries::TimeSeriesVectorPtr values(
      Settings &settings, const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions);

  boost::shared_ptr<Spine::Table> makeQuery(
      Settings &settings, boost::shared_ptr<Spine::ValueFormatter> &valueFormatter);

  void makeQuery(QueryBase *qb);

  FlashCounts getFlashCount(const boost::posix_time::ptime &starttime,
                            const boost::posix_time::ptime &endtime,
                            const Spine::TaggedLocationList &locations);
  boost::shared_ptr<std::vector<ObservableProperty> > observablePropertyQuery(
      std::vector<std::string> &parameters, const std::string language);

  Spine::Parameter makeParameter(const std::string &name) const;

  bool ready() const;

  void setGeonames(Geonames::Engine *geonames);

  const std::shared_ptr<DBRegistry> dbRegistry() const { return itsDatabaseRegistry; }
  void getStations(Spine::Stations &stations, Settings &settings);

  Spine::Stations getStationsByArea(const Settings &settings, const std::string &areaWkt);

  void getStationsByBoundingBox(Spine::Stations &stations, const Settings &settings);

  void getStationsByRadius(Spine::Stations &stations,
                           const Settings &settings,
                           double longitude,
                           double latitude);

  /* \brief Test if the given alias name is configured and it has a field for
* the stationType.
   * \param[in] alias Alias name of a meteorologal parameter.
   * \param[in] stationType Station type to use for the alias.
   * \retval true The alias exist and it has configuration for the stationType.
* \retval false The alias is not configured or there isn't a field for the
* stationType inside of
* the alias configuration block.
   */

  bool isParameter(const std::string &alias, const std::string &stationType = "unknown") const;

  /* \brief Test if the given alias name is configured
   * \param[in] name Alias name of a meteorologal parameter.
   * \retval true The alias exist and it has configuration for the stationType.
* \retval false The alias is not configured or there isn't a field for the
* stationType inside of
* the alias configuration block.
   */

  bool isParameterVariant(const std::string &name) const;

  /* \brief Get a numerical identity for an given alias name.
   * \param[in] alias Alias name of a meteorologal parameter (case insensitive).
   * \param[in] stationType Station type to use for the alias (case
   * insensitive).
   * \return Positive integer in success and zero if there is no a match.
   */
  uint64_t getParameterId(const std::string &alias,
                          const std::string &stationType = "unknown") const;

  std::set<std::string> getValidStationTypes() const;

 protected:
  void init();
  void shutdown();

 private:
  Engine();

  ~Engine() {}

  void initializeCache();
  bool stationHasRightType(const Spine::Station &station, const Settings &settings);
  void unserializeStations();

  std::string itsConfigFile;
  bool itsReady = false;

  boost::mutex itsSetGeonamesMutex;

  std::shared_ptr<DBRegistry> itsDatabaseRegistry;

  std::unique_ptr<DatabaseDriver> itsDatabaseDriver;
  boost::shared_ptr<EngineParameters> itsEngineParameters;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
