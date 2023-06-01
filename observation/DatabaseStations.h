
#pragma once

#include "Engine.h"
#include <engines/geonames/Engine.h>
#include <set>
#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class DatabaseStations
{
 public:
  DatabaseStations(const EngineParametersPtr &oep, SmartMet::Engine::Geonames::Engine *gn)
      : itsObservationEngineParameters(oep), itsGeonames(gn)
  {
  }

  Spine::TaggedFMISIDList translateToFMISID(const Settings &settings,
                                            const StationSettings &stationSettings) const;

  void getStations(Spine::Stations &stations, const Settings &settings) const;
  void getStationsByArea(Spine::Stations &stations,
                         const Settings &settings,
                         const std::string &wkt) const;

  void getStationsByBoundingBox(Spine::Stations &stations,
                                const Settings &settings,
                                const BoundingBoxSettings &boundingBox) const;

  static std::string getTag(const BoundingBoxSettings &bboxSettings);
  static std::string getTag(const NearestStationSettings &nearestStationSettings);

 private:
  Spine::TaggedFMISIDList translateGeoIdsToFMISID(const Settings &settings,
                                                  const GeoIdSettings &geoidSettings) const;
  void getStationGroups(std::set<std::string> &stationgroup_codes,
                        const std::string &stationtype,
                        const std::set<std::string> &stationgroups) const;

  const EngineParametersPtr &itsObservationEngineParameters;

  SmartMet::Engine::Geonames::Engine *itsGeonames;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
