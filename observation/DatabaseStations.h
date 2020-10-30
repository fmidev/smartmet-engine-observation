
#pragma once

#include <engines/geonames/Engine.h>
#include "Engine.h"
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
  DatabaseStations(const EngineParametersPtr &oep,
                   SmartMet::Engine::Geonames::Engine *gn)
      : itsObservationEngineParameters(oep), itsGeonames(gn)
  {
  }

  Spine::TaggedFMISIDList translateToFMISID(
      const boost::posix_time::ptime &starttime,
      const boost::posix_time::ptime &endtime,
      const std::string &stationtype,
      const StationSettings &stationSettings) const;

  void getStations(Spine::Stations &stations, const Settings &settings) const;
  void getStationsByArea(Spine::Stations &stations,
                         const std::string &stationtype,
                         const boost::posix_time::ptime &starttime,
                         const boost::posix_time::ptime &endtime,
                         const std::string &wkt) const;

  void getStationsByBoundingBox(Spine::Stations &stations,
                                const Settings &settings) const;

  static std::string getTag(const BoundingBoxSettings &bboxSettings);
  static std::string getTag(
      const NearestStationSettings &nearestStationSettings);

 private:
  Spine::TaggedFMISIDList translateGeoIdsToFMISID(
      const boost::posix_time::ptime &starttime,
      const boost::posix_time::ptime &endtime,
      const std::string &stationtype,
      const GeoIdSettings &geoidSettings) const;
  void getStationGroups(std::set<std::string> &stationgroup_codes,
                        const std::string &stationtype) const;
  void getStationsByBoundingBox(Spine::Stations &stations,
                                const boost::posix_time::ptime &starttime,
                                const boost::posix_time::ptime &endtime,
                                const std::string &stationtype,
                                const BoundingBoxSettings &bboxSettings) const;

  const EngineParametersPtr &itsObservationEngineParameters;

  SmartMet::Engine::Geonames::Engine *itsGeonames;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
