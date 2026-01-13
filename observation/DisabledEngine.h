#pragma once

#include "Engine.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class DisabledEngine : public Engine
{
 public:
  DisabledEngine();
  ~DisabledEngine() override = default;

  TS::TimeSeriesVectorPtr values(Settings &settings) override;
  TS::TimeSeriesVectorPtr values(Settings &settings,
                                 const TS::TimeSeriesGeneratorOptions &timeSeriesOptions) override;

  void makeQuery(QueryBase *qb) override;

  FlashCounts getFlashCount(const Fmi::DateTime &starttime,
                            const Fmi::DateTime &endtime,
                            const Spine::TaggedLocationList &locations) override;
  std::shared_ptr<std::vector<ObservableProperty>> observablePropertyQuery(
      std::vector<std::string> &parameters, const std::string &language) override;

  bool ready() const override;

  Geonames::Engine *getGeonames() const override;

  std::shared_ptr<DBRegistry> dbRegistry() const override;
  void reloadStations() override;
  void getStations(Spine::Stations &stations, const Settings &settings) override;

  void getStationsByArea(Spine::Stations &stations,
                         const Settings &settings,
                         const std::string &areaWkt) override;

  void getStationsByBoundingBox(Spine::Stations &stations, const Settings &settings) override;

  bool isParameter(const std::string &alias,
                   const std::string &stationType = "unknown") const override;

  bool isParameterVariant(const std::string &name) const override;

  uint64_t getParameterId(const std::string &alias,
                          const std::string &stationType = "unknown") const override;

  std::string getParameterIdAsString(const std::string &alias,
                                     const std::string &stationType = "unknown") const override;

  std::set<std::string> getValidStationTypes() const override;

  ContentTable getProducerInfo(const std::optional<std::string> &producer) const override;

  ContentTable getParameterInfo(const std::optional<std::string> &producer) const override;

  ContentTable getStationInfo(const StationOptions &options) const override;

  MetaData metaData(const std::string &producer,
                    const Settings &settings = Settings()) const override;

  Spine::TaggedFMISIDList translateToFMISID(const Settings &settings,
                                            const StationSettings &stationSettings) const override;

  const ProducerMeasurandInfo &getMeasurandInfo() const override;

  Fmi::DateTime getLatestDataUpdateTime(const std::string &producer,
                                        const Fmi::DateTime &from) const override;

  void init() override;
  void shutdown() override;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
