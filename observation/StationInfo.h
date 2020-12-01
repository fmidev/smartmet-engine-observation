#pragma once

#include "StationLocation.h"
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
  StationInfo(const std::string& filename);

  SmartMet::Spine::Stations stations;  // all known stations
  StationLocations stationLocations;   // all station locations

  void serialize(const std::string& filename) const;
  void unserialize(const std::string& filename);

  SmartMet::Spine::Stations findNearestStations(double longitude,
                                                double latitude,
                                                double maxdistance,
                                                int numberofstations,
                                                const std::set<std::string>& groups,
                                                const boost::posix_time::ptime& starttime,
                                                const boost::posix_time::ptime& endtime) const;

  SmartMet::Spine::Stations findWmoStations(const std::vector<int>& wmos) const;
  SmartMet::Spine::Stations findLpnnStations(const std::vector<int>& lpnns) const;
  SmartMet::Spine::Stations findFmisidStations(const std::vector<int>& fmisids) const;
  SmartMet::Spine::Stations findRwsidStations(const std::vector<int>& rwsids) const;

  SmartMet::Spine::Stations findFmisidStations(const std::vector<int>& fmisids,
                                               const std::set<std::string>& groups,
                                               const boost::posix_time::ptime& starttime,
                                               const boost::posix_time::ptime& endtime) const;
  SmartMet::Spine::Stations findFmisidStations(
      const SmartMet::Spine::TaggedFMISIDList& taggedFMISIDs,
      const std::set<std::string>& groups,
      const boost::posix_time::ptime& starttime,
      const boost::posix_time::ptime& endtime) const;

  SmartMet::Spine::Stations findFmisidStations(
      const SmartMet::Spine::TaggedFMISIDList& taggedFMISIDs) const;

  SmartMet::Spine::Stations findWmoStations(const std::vector<int>& wmos,
                                            const std::set<std::string>& groups,
                                            const boost::posix_time::ptime& starttime,
                                            const boost::posix_time::ptime& endtime) const;

  SmartMet::Spine::Stations findLpnnStations(const std::vector<int>& lpnns,
                                             const std::set<std::string>& groups,
                                             const boost::posix_time::ptime& starttime,
                                             const boost::posix_time::ptime& endtime) const;

  SmartMet::Spine::Stations findRwsidStations(const std::vector<int>& rwsids,
                                              const std::set<std::string>& groups,
                                              const boost::posix_time::ptime& starttime,
                                              const boost::posix_time::ptime& endtime) const;

  SmartMet::Spine::Stations findStationsInGroup(const std::set<std::string>& groups,
                                                const boost::posix_time::ptime& starttime,
                                                const boost::posix_time::ptime& endtime) const;

  Spine::Stations findStationsInsideArea(const std::set<std::string>& groups,
                                         const boost::posix_time::ptime& starttime,
                                         const boost::posix_time::ptime& endtime,
                                         const std::string& wkt) const;

  SmartMet::Spine::Stations findStationsInsideBox(double minx,
                                                  double miny,
                                                  double maxx,
                                                  double maxy,
                                                  const std::set<std::string>& groups,
                                                  const boost::posix_time::ptime& starttime,
                                                  const boost::posix_time::ptime& endtime) const;

  const Spine::Station& getStation(unsigned int fmisid, const std::set<std::string>& groups) const;

  bool belongsToGroup(unsigned int fmisid, const std::set<std::string>& groups) const;

  Spine::TaggedFMISIDList translateWMOToFMISID(const std::vector<int>& wmos,
                                               const boost::posix_time::ptime& t) const;
  Spine::TaggedFMISIDList translateRWSIDToFMISID(const std::vector<int>& rwsids,
                                                 const boost::posix_time::ptime& t) const;
  Spine::TaggedFMISIDList translateLPNNToFMISID(const std::vector<int>& lpnns,
                                                const boost::posix_time::ptime& t) const;

 private:
  void update() const;
  // Mapping from coordinates to stations
  using StationTree =
      Fmi::NearTree<StationNearTreeLatLon, Fmi::NearTreeLatLonDistance<StationNearTreeLatLon>>;

  // Members of station groups
  using GroupMembers = std::map<std::string, std::set<StationID>>;

  mutable StationIndex fmisidstations;  // fmisid --> indexes of stations
  mutable StationIndex wmostations;     // wmo --> indexes of stations
  mutable StationIndex lpnnstations;    // lpnn --> indexes of stations
  mutable StationIndex rwsidstations;   // rwsid --> indexes of stations
  mutable StationTree stationtree;      // search tree for nearest stations
  mutable GroupMembers members;         // group id --> indexes of stations
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
