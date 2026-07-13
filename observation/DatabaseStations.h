
#pragma once

#include "Engine.h"
#include "StationInfo.h"
#include <engines/geonames/Engine.h>
#include <macgyver/Cache.h>
#include <spine/Location.h>
#include <memory>
#include <mutex>
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
  DatabaseStations(const EngineParametersPtr& oep, SmartMet::Engine::Geonames::Engine* gn)
      : itsObservationEngineParameters(oep),
        itsGeonames(gn),
        itsNearestCandidateCache(oep->nearestStationsCacheSize),
        itsGeoIdCache(oep->geoIdCacheSize)
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

  // Statistics of the nearest-station and geoid lookup caches
  Fmi::Cache::CacheStatistics getCacheStats() const;

 private:
  Spine::TaggedFMISIDList translateGeoIdsToFMISID(const Settings& settings,
                                                  const GeoIdSettings& geoidSettings) const;
  void getStationGroups(std::set<std::string>& stationgroup_codes,
                        const std::string& stationtype,
                        const std::set<std::string>& stationgroups) const;

  // Cached nearest-station search. Caches the time- and group-independent
  // geometric candidate list keyed on coordinates and maxdistance, then applies
  // the time and group filtering per request. The candidate lists reference
  // StationID indices of a particular StationInfo instance, so the cache is
  // cleared whenever the station data is swapped underneath us.
  Spine::Stations cachedFindNearestStations(const std::shared_ptr<StationInfo>& info,
                                            double longitude,
                                            double latitude,
                                            double maxdistance,
                                            int numberofstations,
                                            const std::set<std::string>& groups,
                                            const Fmi::DateTime& starttime,
                                            const Fmi::DateTime& endtime) const;

  const EngineParametersPtr& itsObservationEngineParameters;

  SmartMet::Engine::Geonames::Engine* itsGeonames;

  // (longitude,latitude,maxdistance) --> geometric nearest-station candidates
  mutable Fmi::Cache::Cache<std::string, NearestCandidateList> itsNearestCandidateCache;
  // (geoid,language) --> resolved locations (Locus idSearch results)
  mutable Fmi::Cache::Cache<std::string, Spine::LocationList> itsGeoIdCache;

  // Guards the identity check that ties itsNearestCandidateCache to the current
  // StationInfo instance.
  mutable std::mutex itsCacheMutex;
  mutable std::shared_ptr<StationInfo> itsCacheStationInfo;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
