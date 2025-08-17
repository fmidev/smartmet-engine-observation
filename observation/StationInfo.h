#pragma once

#include "StationGroups.h"
#include <macgyver/NearTree.h>
#include <macgyver/NearTreeLatLon.h>
#include <spine/Station.h>
#include <map>
#include <set>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
// Index into a stations vector
using StationID = unsigned int;

// Mapping from some identifier to stations
using StationIndex = std::map<unsigned int, std::set<StationID>>;
using NamedStationIndex = std::map<std::string, std::set<StationID>>;

// We store the index into a vector along with the coordinates
using StationNearTreeLatLon = Fmi::NearTreeLatLon<StationID>;

/*!
 * \brief Central holder for current station information.
 *
 * We hold the information in a smart pointer so that all relevant
 * information can be updated in a single atomic operation so that
 * threads already using the old data may still do so.
 */

class StationInfo
{
 public:
  StationInfo() = default;
  explicit StationInfo(const std::string& filename);

  Spine::Stations stations;  // all known stations
  // StationLocations stationLocations;  // all station locations

  void serialize(const std::string& filename) const;
  void unserialize(const std::string& filename);

  std::vector<int> fmisids() const;

  Spine::Stations findNearestStations(double longitude,
                                      double latitude,
                                      double maxdistance,
                                      int numberofstations,
                                      const std::set<std::string>& groups,
                                      const Fmi::DateTime& starttime,
                                      const Fmi::DateTime& endtime) const;

  Spine::Stations findWmoStations(const std::vector<int>& wmos) const;
  Spine::Stations findLpnnStations(const std::vector<int>& lpnns) const;
  Spine::Stations findFmisidStations(const std::vector<int>& fmisids) const;
  Spine::Stations findRwsidStations(const std::vector<int>& rwsids) const;
  Spine::Stations findWsiStations(const std::vector<std::string>& wsis) const;

  Spine::Stations findFmisidStations(const std::vector<int>& fmisids,
                                     const std::set<std::string>& groups,
                                     const Fmi::DateTime& starttime,
                                     const Fmi::DateTime& endtime) const;
  Spine::Stations findFmisidStations(const Spine::TaggedFMISIDList& taggedFMISIDs,
                                     const std::set<std::string>& groups,
                                     const Fmi::DateTime& starttime,
                                     const Fmi::DateTime& endtime) const;

  Spine::Stations findFmisidStations(const Spine::TaggedFMISIDList& taggedFMISIDs) const;

  Spine::Stations findWmoStations(const std::vector<int>& wmos,
                                  const std::set<std::string>& groups,
                                  const Fmi::DateTime& starttime,
                                  const Fmi::DateTime& endtime) const;

  Spine::Stations findLpnnStations(const std::vector<int>& lpnns,
                                   const std::set<std::string>& groups,
                                   const Fmi::DateTime& starttime,
                                   const Fmi::DateTime& endtime) const;

  Spine::Stations findRwsidStations(const std::vector<int>& rwsids,
                                    const std::set<std::string>& groups,
                                    const Fmi::DateTime& starttime,
                                    const Fmi::DateTime& endtime) const;

  Spine::Stations findWsiStations(const std::vector<std::string>& wsis,
                                  const std::set<std::string>& groups,
                                  const Fmi::DateTime& starttime,
                                  const Fmi::DateTime& endtime) const;

  Spine::Stations findStationsInGroup(const std::set<std::string>& groups,
                                      const Fmi::DateTime& starttime,
                                      const Fmi::DateTime& endtime) const;

  Spine::Stations findStationsInsideArea(const std::set<std::string>& groups,
                                         const Fmi::DateTime& starttime,
                                         const Fmi::DateTime& endtime,
                                         const std::string& wkt) const;

  Spine::Stations findStationsInsideBox(double minx,
                                        double miny,
                                        double maxx,
                                        double maxy,
                                        const std::set<std::string>& groups,
                                        const Fmi::DateTime& starttime,
                                        const Fmi::DateTime& endtime) const;

  const Spine::Station& getStation(unsigned int fmisid,
                                   const std::set<std::string>& groups,
                                   const Fmi::DateTime& t) const;

  bool belongsToGroup(unsigned int fmisid, const std::set<std::string>& groups) const;

  Spine::TaggedFMISIDList translateWMOToFMISID(const std::vector<int>& wmos,
                                               const Fmi::DateTime& t) const;
  Spine::TaggedFMISIDList translateRWSIDToFMISID(const std::vector<int>& rwsids,
                                                 const Fmi::DateTime& t) const;
  Spine::TaggedFMISIDList translateLPNNToFMISID(const std::vector<int>& lpnns,
                                                const Fmi::DateTime& t) const;
  Spine::TaggedFMISIDList translateWSIToFMISID(const std::vector<std::string>& wsis,
                                               const Fmi::DateTime& t) const;
  void setStationGroups(const StationGroups& sg) { itsStationGroups = sg; }

  bool isRoadStation(int fmisid) const { return roadfmisids.find(fmisid) != roadfmisids.end(); }
  bool isForeignStation(int fmisid) const { return foreignfmisids.find(fmisid) != foreignfmisids.end(); }
  
 private:
  void update() const;

  // Mapping from coordinates to stations
  using StationTree =
      Fmi::NearTree<StationNearTreeLatLon, Fmi::NearTreeLatLonDistance<StationNearTreeLatLon>>;

  // Members of station groups
  using GroupMembers = std::map<std::string, std::set<StationID>>;

  mutable StationIndex fmisidstations;    // fmisid --> indexes of stations
  mutable StationIndex wmostations;       // wmo --> indexes of stations
  mutable StationIndex lpnnstations;      // lpnn --> indexes of stations
  mutable StationIndex rwsidstations;     // rwsid --> indexes of stations
  mutable NamedStationIndex wsistations;  // wsi --> indexes of stations
  mutable StationTree stationtree;        // search tree for nearest stations
  mutable GroupMembers members;           // group id --> indexes of stations

  mutable std::set<int> roadfmisids; // all stations where isRoad=true
  mutable std::set<int> foreignfmisids; // all stations where isForeign=true

  
  StationGroups itsStationGroups;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
