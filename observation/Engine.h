#pragma once

#include "EngineParameters.h"
#include "MeasurandInfo.h"
#include "MetaData.h"
#include "ObservableProperty.h"
#include "StationOptions.h"
#include "StationSettings.h"
#include <spine/Table.h>
#include <spine/TableFormatter.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
using ContentTable = std::unique_ptr<Spine::Table>;

class DBRegistry;
class QueryBase;

class Engine : public SmartMet::Spine::SmartMetEngine
{
 protected:
  Engine();

 public:
  ~Engine() override = default;

  static Engine *create(const std::string &config_name);

  virtual TS::TimeSeriesVectorPtr values(Settings &settings) = 0;
  virtual TS::TimeSeriesVectorPtr values(Settings &settings,
                                         const TS::TimeSeriesGeneratorOptions &timeSeriesOptions) = 0;

  virtual void makeQuery(QueryBase *qb) = 0;

  virtual FlashCounts getFlashCount(const Fmi::DateTime &starttime,
                                    const Fmi::DateTime &endtime,
                                    const Spine::TaggedLocationList &locations) = 0;
  virtual std::shared_ptr<std::vector<ObservableProperty>> observablePropertyQuery(
      std::vector<std::string> &parameters, const std::string &language) = 0;

  virtual bool ready() const = 0;

  virtual Geonames::Engine *getGeonames() const = 0;

  virtual std::shared_ptr<DBRegistry> dbRegistry() const = 0;
  virtual void reloadStations() = 0;
  virtual void getStations(Spine::Stations &stations, const Settings &settings) = 0;

  virtual void getStationsByArea(Spine::Stations &stations,
                                 const Settings &settings,
                                 const std::string &areaWkt) = 0;

  virtual void getStationsByBoundingBox(Spine::Stations &stations, const Settings &settings) = 0;

  /* \brief Test if the given alias name is configured and it has a field for
   * the stationType.
   * \param[in] alias Alias name of a meteorological parameter.
   * \param[in] stationType Station type to use for the alias.
   * \retval true The alias exist and it has configuration for the stationType.
   * \retval false The alias is not configured or there isn't a field for the
   * stationType inside of
   * the alias configuration block.
   */

  virtual bool isParameter(const std::string &alias,
                           const std::string &stationType = "unknown") const = 0;

  /* \brief Test if the given alias name is configured
   * \param[in] name Alias name of a meteorological parameter.
   * \retval true The alias exist and it has configuration for the stationType.
   * \retval false The alias is not configured or there isn't a field for the
   * stationType inside of
   * the alias configuration block.
   */

  virtual bool isParameterVariant(const std::string &name) const = 0;

  /* \brief Get a numerical identity for an given alias name.
   * \param[in] alias Alias name of a meteorological parameter (case insensitive).
   * \param[in] stationType Station type to use for the alias (case
   * insensitive).
   * \return Positive integer in success and zero if there is no a match.
   */
  virtual uint64_t getParameterId(const std::string &alias,
                                  const std::string &stationType = "unknown") const = 0;

  /* \brief Get parameter id as string for an given alias name.
   * \param[in] alias Alias name of a meteorological parameter (case insensitive).
   * \param[in] stationType Station type to use for the alias (case
   * insensitive).
   * \return String
   */

  virtual std::string getParameterIdAsString(const std::string &alias,
                                             const std::string &stationType = "unknown") const = 0;

  /* \brief Get valid parameter names
   * \return Set of parameter names
   */

  virtual std::set<std::string> getValidStationTypes() const = 0;

  /* \brief Get detailed info of producer(s)
   * \param[in] producer If producer is given return info only of that producer, otherwise of all
   * producers \return Info of producer(s)
   */
  virtual ContentTable getProducerInfo(const std::optional<std::string> &producer) const = 0;

  /* \brief Get parameter info of producer(s)
   * \param[in] producer If producer is given return info only of that producer, otherwise of all
   * producers \return Parameter info of producer(s)
   */
  virtual ContentTable getParameterInfo(const std::optional<std::string> &producer) const = 0;

  /* \brief Get station info
   * \param[in] StationOptions Defines query options
   * \return The requested station info
   */
  virtual ContentTable getStationInfo(const StationOptions &options) const = 0;

  virtual MetaData metaData(const std::string &producer,
                            const Settings &settings = Settings()) const = 0;

  /* \brief Translates WMO,RWID,LPNN,GEOID,Bounding box to FMISID
   * \return List of FMISIDs
   */
  virtual Spine::TaggedFMISIDList translateToFMISID(const Settings &settings,
                                                    const StationSettings &stationSettings) const = 0;

  /* \brief Get measurand info
   * \return ProducerMeasurand info
   */
  virtual const ProducerMeasurandInfo &getMeasurandInfo() const = 0;

  /* \brief Get latest data update time of given producer
   * \return Time when data of a producer was last time updated
   */
  virtual Fmi::DateTime getLatestDataUpdateTime(const std::string &producer,
                                                const Fmi::DateTime &from) const = 0;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
