
#pragma once

#include "EngineParameters.h"
#include "StationSettings.h"
#include <spine/Station.h>
#include <set>
#include <string>

namespace SmartMet::Engine::Geonames
{
class Engine;
}  // namespace SmartMet::Engine::Geonames
namespace SmartMet::Engine::Observation
{
class Settings;
class StationInfo;
}  // namespace SmartMet::Engine::Observation

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class ObservationCache;

class DatabaseStations
{
 public:
  DatabaseStations(const EngineParametersPtr& oep, SmartMet::Engine::Geonames::Engine* gn)
      : itsObservationEngineParameters(oep), itsGeonames(gn)
  {
  }

  Spine::TaggedFMISIDList translateToFMISID(const Settings& settings,
                                            const StationSettings& stationSettings) const;

  void getStations(Spine::Stations& stations, const Settings& settings) const;
  void getStationsByArea(Spine::Stations& stations,
                         const Settings& settings,
                         const std::string& wkt) const;

  void getStationsByBoundingBox(Spine::Stations& stations,
                                const Settings& settings,
                                const BoundingBoxSettings& boundingBox) const;

  static std::string getTag(const BoundingBoxSettings& bboxSettings);
  static std::string getTag(const NearestStationSettings& nearestStationSettings);

 private:
  Spine::TaggedFMISIDList translateGeoIdsToFMISID(const Settings& settings,
                                                  const GeoIdSettings& geoidSettings) const;
  void addNearestStations(Spine::TaggedFMISIDList& result,
                          const Settings& settings,
                          const NearestStationSettings& nss,
                          const std::set<int>& named_fmisids,
                          const StationInfo& info) const;
  void addNearestStationsWithData(Spine::TaggedFMISIDList& result,
                                  const Settings& settings,
                                  const NearestStationSettings& nss,
                                  const std::string& nssTag,
                                  const std::set<std::string>& stationgroup_codes,
                                  const StationInfo& info,
                                  const ObservationCache& cache,
                                  const std::string& measurandIds,
                                  const std::string& cacheTableName) const;
  void getStationGroups(std::set<std::string>& stationgroup_codes,
                        const std::string& stationtype,
                        const std::set<std::string>& stationgroups) const;

  const EngineParametersPtr& itsObservationEngineParameters;

  SmartMet::Engine::Geonames::Engine* itsGeonames;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
