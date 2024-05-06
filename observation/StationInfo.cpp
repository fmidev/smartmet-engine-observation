#include "StationInfo.h"
#include "Utils.h"
#include <boost/algorithm/string/predicate.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/xml_iarchive.hpp>
#include <boost/archive/xml_oarchive.hpp>
#include <boost/filesystem.hpp>
#include <boost/serialization/map.hpp>
#include <boost/serialization/set.hpp>
#include <boost/serialization/vector.hpp>
#include <gis/OGR.h>
#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>
#include <fstream>

#include <boost/algorithm/string/join.hpp>
#include <fmt/format.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
// Sort based on fmisid
static bool sort_stations_function(const Spine::Station& s1, const Spine::Station& s2)
{
  return (s1.fmisid < s2.fmisid);
}

// ----------------------------------------------------------------------
/*!
 * \brief Test if the station has any observations for the time
 */
// ----------------------------------------------------------------------

bool timeok(const Spine::Station& station, const Fmi::DateTime& t)
{
  return !(t < station.station_start || t > station.station_end);
}

// ----------------------------------------------------------------------
/*!
 * \brief Test if the station has any observations for the time period
 *
 * If one time period ends before another starts, there is no overlap.
 * If one period starts after another ends, there is no overlap.
 * If neither test returns true, the ranges must overlap.
 */
// ----------------------------------------------------------------------

bool timeok(const Spine::Station& station,
            const Fmi::DateTime& starttime,
            const Fmi::DateTime& endtime)
{
  return !(endtime < station.station_start || starttime > station.station_end);
}

// ----------------------------------------------------------------------
/*!
 * \brief Test if the station belongs to any of the groups
 */
// ----------------------------------------------------------------------

bool groupok(const Spine::Station& station, const std::set<std::string>& groups)
{
  // All groups allowed?
  if (groups.empty())
    return true;

  return (groups.find(station.type) != groups.end());
}

// ----------------------------------------------------------------------
/*!
 * \brief Create the directory for the serialized stations
 */
// ----------------------------------------------------------------------

void createSerializedStationsDirectory(const std::string& filename)
{
  boost::filesystem::path path = boost::filesystem::path(filename);
  boost::filesystem::path directory = path.parent_path();

  try
  {
    if (!boost::filesystem::is_directory(directory))
      boost::filesystem::create_directories(directory);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP,
                                "Failed to create directory for serialized station information")
        .addParameter("stationfile", filename)
        .addParameter("directory", directory.string());
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Serialize the station information
 */
// -----------------------------------------------------------------------

void StationInfo::serialize(const std::string& filename) const
{
  try
  {
    // Update internal search trees too
    update();

    // Make sure the output directory exists
    createSerializedStationsDirectory(filename);

    // Serialize via a temporary file just in case the server aborts

    std::string tmpfile = filename + ".tmp";

    std::ofstream file(tmpfile);
    if (!file)
      throw Fmi::Exception(BCP, "Failed to open " + tmpfile + " for writing");

    if (boost::algorithm::iends_with(filename, ".txt"))
    {
      boost::archive::text_oarchive archive(file);
      archive& BOOST_SERIALIZATION_NVP(stations);
    }
    else if (boost::algorithm::iends_with(filename, ".xml"))
    {
      boost::archive::xml_oarchive archive(file);
      archive& BOOST_SERIALIZATION_NVP(stations);
    }
    else
    {
      boost::archive::binary_oarchive archive(file);
      archive& BOOST_SERIALIZATION_NVP(stations);
    }

    // Rename to final filename
    try
    {
      boost::filesystem::rename(tmpfile, filename);
    }
    catch (...)
    {
      throw Fmi::Exception::Trace(BCP, "Failed to rename " + tmpfile + " to " + filename);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "StationInfo serialization failed.");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Construct from serialized station information
 */
// ----------------------------------------------------------------------

StationInfo::StationInfo(const std::string& filename)
{
  unserialize(filename);
}

// ----------------------------------------------------------------------
/*!
 * \brief Unserialize station information
 */
// ----------------------------------------------------------------------

void StationInfo::unserialize(const std::string& filename)
{
  std::ifstream file(filename);

  if (boost::algorithm::iends_with(filename, ".txt"))
  {
    boost::archive::text_iarchive archive(file);
    archive& BOOST_SERIALIZATION_NVP(stations);
  }
  else if (boost::algorithm::iends_with(filename, ".xml"))
  {
    boost::archive::xml_iarchive archive(file);
    archive& BOOST_SERIALIZATION_NVP(stations);
  }
  else
  {
    boost::archive::binary_iarchive archive(file);
    archive& BOOST_SERIALIZATION_NVP(stations);
  }

  update();
}

// ----------------------------------------------------------------------
/*!
 * \brief Find the nearest stations
 */
// ----------------------------------------------------------------------

Spine::Stations StationInfo::findNearestStations(double longitude,
                                                 double latitude,
                                                 double maxdistance,
                                                 int numberofstations,
                                                 const std::set<std::string>& groups,
                                                 const Fmi::DateTime& starttime,
                                                 const Fmi::DateTime& endtime) const
{
  if (numberofstations < 1)
    throw Fmi::Exception(BCP, "Cannot search for less than 1 nearby stations");

  auto maxcount = static_cast<std::size_t>(numberofstations);

  // Find all stations within the distance limit
  StationNearTreeLatLon searchpoint{longitude, latitude};

  auto candidates = stationtree.nearestones(
      searchpoint, StationNearTreeLatLon::ChordLength(maxdistance / 1000.0));

  // Note: The candidates are stored in a multimap sorted by distance. However,
  // since road weather stations may have identical coordinates, and NearTree
  // buildup is not deterministic, the sorting is not stable for the stations
  // at identical distances, and hence regression tests may fail. We'll use
  // the station ID as an extra sorting criteria.

  using StationDistance = std::pair<double, StationID>;  // distance first for sorting!
  std::vector<StationDistance> distances;
  double previous_distance = -1;

  for (const auto& candidate : candidates)
  {
    StationID id = candidate.second.ID();
    const auto& station = stations.at(id);

    if (!timeok(station, starttime, endtime))
      continue;

    if (!groupok(station, groups))
      continue;

    double distance = StationNearTreeLatLon::SurfaceLength(candidate.first);

    // Abort if distance has changed and desired count has been reached
    if (distances.size() >= maxcount && distance > previous_distance)
      break;

    previous_distance = distance;

    distances.emplace_back(distance, id);
  }

  // The vector is already sorted by distance. We want stations at the same distance to be
  // in a deterministic order, so we sort again by distance AND name

  std::sort(distances.begin(),
            distances.end(),
            [this](const StationDistance& lhs, const StationDistance& rhs)
            {
              if (lhs.first != rhs.first)
                return lhs.first < rhs.first;
              return (this->stations.at(lhs.second).formal_name_fi <
                      this->stations.at(rhs.second).formal_name_fi);
            });

  // Accept only max count stations

  if (distances.size() > maxcount)
    distances.resize(numberofstations);

  // Build the final result

  Spine::Stations result;
  for (const auto& distance_id : distances)
  {
    double distance = distance_id.first;
    StationID id = distance_id.second;

    Spine::Station newstation = stations.at(id);

    newstation.distance =
        Fmi::to_string(std::round(distance * 10) / 10.0);  // round to 100 meter precision
    newstation.requestedLat = latitude;
    newstation.requestedLon = longitude;
    Utils::calculateStationDirection(newstation);

    result.push_back(newstation);
  }

  return result;
}

// ----------------------------------------------------------------------
/*!
 * \brief Utility for finding stations of specific type
 */
// ----------------------------------------------------------------------

template <typename IDS, typename INDEX>
Spine::Stations findStations(const Spine::Stations& stations, const IDS& ids, const INDEX& index)
{
  Spine::Stations result;

  for (const auto& id : ids)
  {
    const auto pos = index.find(id);
    if (pos != index.end())
    {
      for (const auto& sid : pos->second)
      {
        const auto& station = stations.at(sid);
        result.push_back(station);
      }
    }
  }
  return result;
}

// ----------------------------------------------------------------------
/*!
 * \brief Utility for finding stations of specific type
 */
// ----------------------------------------------------------------------

template <typename IDS, typename INDEX>
Spine::Stations findStations(const Spine::Stations& stations,
                             const std::set<std::string>& groups,
                             const IDS& ids,
                             const INDEX& index,
                             const Fmi::DateTime& starttime,
                             const Fmi::DateTime& endtime)
{
  Spine::Stations result;

  for (const auto& id : ids)
  {
    const auto pos = index.find(id);
    if (pos != index.end())
    {
      for (const auto& sid : pos->second)
      {
        const auto& station = stations.at(sid);

        // Validate timerange
        if (!timeok(station, starttime, endtime))
          continue;

        // Validate group
        if (!groupok(station, groups))
          continue;

        result.push_back(station);
      }
    }
  }
  return result;
}

// ----------------------------------------------------------------------
/*!
 * \brief Find the given FMISID stations
 */
// ----------------------------------------------------------------------

Spine::Stations StationInfo::findFmisidStations(const std::vector<int>& fmisids) const
{
  return findStations(stations, fmisids, fmisidstations);
}

// ----------------------------------------------------------------------
/*!
 * \brief Find the given FMISID stations
 */
// ----------------------------------------------------------------------

Spine::Stations StationInfo::findFmisidStations(const Spine::TaggedFMISIDList& taggedFMISIDs,
                                                const std::set<std::string>& groups,
                                                const Fmi::DateTime& starttime,
                                                const Fmi::DateTime& endtime) const
{
  std::vector<int> fmisids;
  std::map<int, const Spine::TaggedFMISID*> fmisidMap;

  for (const auto& item : taggedFMISIDs)
  {
    fmisids.push_back(item.fmisid);
    fmisidMap.insert(std::make_pair(item.fmisid, &item));
  }

  Spine::Stations ret = findFmisidStations(fmisids, groups, starttime, endtime);

  // Set direction, distance, tag
  for (auto& station : ret)
  {
    const Spine::TaggedFMISID* tfmisid = fmisidMap.at(station.fmisid);
    // Chage to >= 0
    if (tfmisid->direction > 0)
      station.stationDirection = tfmisid->direction;
    else
      station.stationDirection = -1;
    if (!tfmisid->distance.empty())
      station.distance = tfmisid->distance;
    else
      station.distance = "";

    if (!tfmisid->tag.empty())
      station.tag = tfmisid->tag;
  }

  return ret;
}

// ----------------------------------------------------------------------
/*!
 * \brief Find the given FMISID stations
 */
// ----------------------------------------------------------------------

Spine::Stations StationInfo::findFmisidStations(const Spine::TaggedFMISIDList& taggedFMISIDs) const
{
  std::vector<int> fmisids;
  std::map<int, const Spine::TaggedFMISID*> fmisidMap;

  for (const auto& item : taggedFMISIDs)
  {
    fmisids.push_back(item.fmisid);
    fmisidMap.insert(std::make_pair(item.fmisid, &item));
  }

  Spine::Stations ret = findFmisidStations(fmisids);

  // Set direction, distance, tag
  for (auto& station : ret)
  {
    const Spine::TaggedFMISID* tfmisid = fmisidMap.at(station.fmisid);
    // Chage to >= 0
    if (tfmisid->direction > 0)
      station.stationDirection = tfmisid->direction;
    else
      station.stationDirection = -1;
    if (!tfmisid->distance.empty())
      station.distance = tfmisid->distance;
    else
      station.distance = "";

    if (!tfmisid->tag.empty())
      station.tag = tfmisid->tag;
  }

  return ret;
}

// ----------------------------------------------------------------------
/*!
 * \brief Find the given WMO stations
 */
// ----------------------------------------------------------------------

Spine::Stations StationInfo::findFmisidStations(const std::vector<int>& fmisids,
                                                const std::set<std::string>& groups,
                                                const Fmi::DateTime& starttime,
                                                const Fmi::DateTime& endtime) const
{
  return findStations(stations, groups, fmisids, fmisidstations, starttime, endtime);
}

// ----------------------------------------------------------------------
/*!
 * \brief Find the given WMO stations
 */
// ----------------------------------------------------------------------

Spine::Stations StationInfo::findWmoStations(const std::vector<int>& wmos) const
{
  return findStations(stations, wmos, wmostations);
}

// ----------------------------------------------------------------------
/*!
 * \brief Find the given WMO stations
 */
// ----------------------------------------------------------------------

Spine::Stations StationInfo::findWmoStations(const std::vector<int>& wmos,
                                             const std::set<std::string>& groups,
                                             const Fmi::DateTime& starttime,
                                             const Fmi::DateTime& endtime) const
{
  return findStations(stations, groups, wmos, wmostations, starttime, endtime);
}

// ----------------------------------------------------------------------
/*!
 * \brief Find the given LPNN stations
 */
// ----------------------------------------------------------------------

Spine::Stations StationInfo::findLpnnStations(const std::vector<int>& lpnns) const
{
  return findStations(stations, lpnns, lpnnstations);
}

// ----------------------------------------------------------------------
/*!
 * \brief Find the given LPNN stations
 */
// ----------------------------------------------------------------------

Spine::Stations StationInfo::findLpnnStations(const std::vector<int>& lpnns,
                                              const std::set<std::string>& groups,
                                              const Fmi::DateTime& starttime,
                                              const Fmi::DateTime& endtime) const
{
  return findStations(stations, groups, lpnns, lpnnstations, starttime, endtime);
}

// ----------------------------------------------------------------------
/*!
 * \brief Find the given Rwsid stations
 */
// ----------------------------------------------------------------------

Spine::Stations StationInfo::findRwsidStations(const std::vector<int>& rwsids) const
{
  return findStations(stations, rwsids, rwsidstations);
}

// ----------------------------------------------------------------------
/*!
 * \brief Find the given Rwsid stations
 */
// ----------------------------------------------------------------------

Spine::Stations StationInfo::findRwsidStations(const std::vector<int>& rwsids,
                                               const std::set<std::string>& groups,
                                               const Fmi::DateTime& starttime,
                                               const Fmi::DateTime& endtime) const
{
  return findStations(stations, groups, rwsids, rwsidstations, starttime, endtime);
}

// ----------------------------------------------------------------------
/*!
 * \brief Find the given WSI stations
 */
// ----------------------------------------------------------------------

Spine::Stations StationInfo::findWsiStations(const std::vector<std::string>& wsis) const
{
  return findStations(stations, wsis, wsistations);
}

// ----------------------------------------------------------------------
/*!
 * \brief Find the given WSI stations
 */
// ----------------------------------------------------------------------

Spine::Stations StationInfo::findWsiStations(const std::vector<std::string>& wsis,
                                             const std::set<std::string>& groups,
                                             const Fmi::DateTime& starttime,
                                             const Fmi::DateTime& endtime) const
{
  return findStations(stations, groups, wsis, wsistations, starttime, endtime);
}

// ----------------------------------------------------------------------
/*!
 * \brief Find stations in the given groups
 */
// ----------------------------------------------------------------------

Spine::Stations StationInfo::findStationsInGroup(const std::set<std::string>& groups,
                                                 const Fmi::DateTime& starttime,
                                                 const Fmi::DateTime& endtime) const
{
  std::set<StationID> all_ids;

  // Collect all unique indices
  for (const auto& groupname : groups)
  {
    const auto& pos = members.find(groupname);
    if (pos != members.end())
    {
      const auto& ids = pos->second;
      all_ids.insert(ids.begin(), ids.end());
    }
  }

  // And return the respective stations
  Spine::Stations result;
  for (const auto id : all_ids)
  {
    const auto& station = stations.at(id);

    if (timeok(station, starttime, endtime))
      result.push_back(station);
  }

  return result;
}

// ----------------------------------------------------------------------
/*!
 * \brief Find stations in WKT area
 */
// ----------------------------------------------------------------------
Spine::Stations StationInfo::findStationsInsideArea(const std::set<std::string>& groups,
                                                    const Fmi::DateTime& starttime,
                                                    const Fmi::DateTime& endtime,
                                                    const std::string& wkt) const
{
  try
  {
    Spine::Stations ret;

    // Get stations belonging to the requested groups and period
    Spine::Stations groupStations = findStationsInGroup(groups, starttime, endtime);
    // Create area geometry from wkt-string
    std::shared_ptr<OGRGeometry> areaGeometry(Fmi::OGR::createFromWkt(wkt, 4326),
                                              &OGRGeometryFactory::destroyGeometry);
    // Create spatial refernce to be used below
    OGRSpatialReference srs;
    srs.importFromEPSGA(4326);

    // Iterate stations
    for (const auto& station : groupStations)
    {
      // Create Point geometry from sation coordinates
      OGRPoint stationLocation(station.longitude, station.latitude);
      stationLocation.assignSpatialReference(&srs);

      // If station is inside area accept it
      if (areaGeometry->Contains(&stationLocation))
      {
        ret.push_back(station);
      }
    }
    // Sort in ascending fmisid order
    std::sort(ret.begin(), ret.end(), sort_stations_function);

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "[StationInfo] finding stations inside area failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Get the station with the given fmisid
 */
// ----------------------------------------------------------------------

const Spine::Station& StationInfo::getStation(unsigned int fmisid,
                                              const std::set<std::string>& groups,
                                              const Fmi::DateTime& t) const
{
  const auto& ids = fmisidstations.at(fmisid);
#if 0  
  bool debug = (fmisid = 101004);
  if (debug)
  {
    std::cout << "Requested groups: " << boost::algorithm::join(groups, ", ") << "\n";
    for (const auto id : ids)
    {
      const auto& station = stations.at(id);
      std::cout << fmt::format("Candidate: {}\tfrom {} to {} at {},{}\n",
                               station.type,
                               Fmi::to_iso_string(station.station_start),
                               Fmi::to_iso_string(station.station_end),
                               station.longitude,
                               station.latitude);
    }
  }
#endif

  for (const auto id : ids)
  {
    const auto& station = stations.at(id);
    if (timeok(station, t) && groupok(station, groups))
      return station;
  }

  Fmi::Exception ex(BCP,
                    "No match found for fmisid=" + Fmi::to_string(fmisid) + " at " +
                        Fmi::to_iso_string(t) + " (" + Fmi::to_string(ids.size()) + " candidates)");
  int counter = 0;
  for (const auto id : ids)
  {
    const auto& station = stations.at(id);
    auto name = "Candidate #" + Fmi::to_string(++counter);
    auto reason = station.type + " from " + Fmi::to_iso_string(station.station_start) + " to " +
                  Fmi::to_iso_string(station.station_end);
    ex.addParameter(name.c_str(), reason);
  }
  throw ex;
}

// ----------------------------------------------------------------------
/*!
 * \brief Return true if the given station is known and belongs to at least one of the given groups
 */
// ----------------------------------------------------------------------

bool StationInfo::belongsToGroup(unsigned int fmisid, const std::set<std::string>& groups) const
{
  // Check if the station is known
  const auto pos = fmisidstations.find(fmisid);
  if (pos == fmisidstations.end())
    return false;

  // Empty group setting means any group will do
  if (groups.empty())
    return true;

  // Require at least one group match
  const auto& ids = pos->second;
  for (const auto id : ids)
  {
    const auto& station = stations.at(id);
    if (groups.find(station.type) != groups.end())
      return true;
  }

  return false;
}

// ----------------------------------------------------------------------
/*!
 * \brief Search for stations inside the given bounding box
 */
// ----------------------------------------------------------------------

std::vector<StationID> searchStations(
    const Spine::Stations& stations, double minx, double miny, double maxx, double maxy)
{
  std::vector<StationID> result;

  if (maxx > minx)
  {
    // Normal bounding box
    for (StationID id = 0; id < stations.size(); ++id)
    {
      double lon = stations[id].longitude;
      if (lon >= minx && lon <= maxx)
      {
        double lat = stations[id].latitude;
        if (lat >= miny && lat <= maxy)
          result.push_back(id);
      }
    }
  }

  else
  {
    // Bounding box spans the 180th meridian
    for (StationID id = 0; id < stations.size(); ++id)
    {
      double lon = stations[id].longitude;
      if (lon <= minx || lon >= maxx)
      {
        double lat = stations[id].latitude;
        if (lat >= miny && lat <= maxy)
          result.push_back(id);
      }
    }
  }

  return result;
}

// ----------------------------------------------------------------------
/*!
 * \brief Search for stations inside the given bounding box
 */
// ----------------------------------------------------------------------

Spine::Stations StationInfo::findStationsInsideBox(double minx,
                                                   double miny,
                                                   double maxx,
                                                   double maxy,
                                                   const std::set<std::string>& groups,
                                                   const Fmi::DateTime& starttime,
                                                   const Fmi::DateTime& endtime) const
{
  auto ids = searchStations(stations, minx, miny, maxx, maxy);

  Spine::Stations result;

  for (const auto& id : ids)
  {
    const auto& station = stations.at(id);

    if (timeok(station, starttime, endtime))
    {
      if (groupok(station, groups))
        result.push_back(station);
    }
  }

  return result;
}

// ----------------------------------------------------------------------
/*!
 * \brief Update search structures
 */
// ----------------------------------------------------------------------

void StationInfo::update() const
{
  // Make a mapping from fmisid to the indexes of respective stations

  for (std::size_t idx = 0; idx < stations.size(); ++idx)
  {
    const auto& station = stations[idx];
    if (station.fmisid > 0)
      fmisidstations[station.fmisid].insert(idx);
  }

  // Make a mapping from wmo to the indexes of respective stations

  for (std::size_t idx = 0; idx < stations.size(); ++idx)
  {
    const auto& station = stations[idx];
    if (station.wmo > 0)
      wmostations[station.wmo].insert(idx);
  }

  // Make a mapping from lpnn to the indexes of respective stations

  for (std::size_t idx = 0; idx < stations.size(); ++idx)
  {
    const auto& station = stations[idx];
    if (station.lpnn > 0)
      lpnnstations[station.lpnn].insert(idx);
  }

  // Make a mapping from rwsid to the indexes of respective stations

  for (std::size_t idx = 0; idx < stations.size(); ++idx)
  {
    const auto& station = stations[idx];
    if (station.rwsid > 0)
      rwsidstations[station.rwsid].insert(idx);
  }

  // Make a mapping from wsi to the indexes of respective stations

  for (std::size_t idx = 0; idx < stations.size(); ++idx)
  {
    const auto& station = stations[idx];
    if (!station.wsi.empty())
      wsistations[station.wsi].insert(idx);
  }

  // Map groups to sets of stations

  for (std::size_t idx = 0; idx < stations.size(); ++idx)
  {
    const auto& station = stations[idx];
    members[station.type].insert(idx);
  }

  // Create a latlon search tree for the stations

  for (StationID idx = 0; idx < stations.size(); ++idx)
  {
    const auto& station = stations[idx];
    stationtree.insert(StationNearTreeLatLon{station.longitude, station.latitude, idx});
  }

  stationtree.flush();
}

Spine::TaggedFMISIDList StationInfo::translateWMOToFMISID(const std::vector<int>& wmos,
                                                          const Fmi::DateTime& t) const
{
  Spine::TaggedFMISIDList ret;

  Spine::Stations wmostations = findWmoStations(wmos);

  std::set<int> processed;
  for (const auto& s : wmostations)
    if (t >= s.station_start && t <= s.station_end && processed.find(s.wmo) == processed.end())
    {
      ret.emplace_back(Fmi::to_string(s.wmo), s.fmisid);
      processed.insert(s.wmo);
    }

  return ret;
}

Spine::TaggedFMISIDList StationInfo::translateRWSIDToFMISID(const std::vector<int>& rwsids,
                                                            const Fmi::DateTime& t) const
{
  Spine::TaggedFMISIDList ret;

  Spine::Stations roadstations = findRwsidStations(rwsids);

  std::set<int> processed;
  for (const auto& s : roadstations)
    if (t >= s.station_start && t <= s.station_end && processed.find(s.rwsid) == processed.end())
    {
      ret.emplace_back(Fmi::to_string(s.rwsid), s.fmisid);
      processed.insert(s.rwsid);
    }

  return ret;
}

Spine::TaggedFMISIDList StationInfo::translateLPNNToFMISID(const std::vector<int>& lpnns,
                                                           const Fmi::DateTime& t) const
{
  Spine::TaggedFMISIDList ret;

  Spine::Stations lpnnstations = findLpnnStations(lpnns);

  std::set<int> processed;
  for (const auto& s : lpnnstations)
    if (t >= s.station_start && t <= s.station_end && processed.find(s.lpnn) == processed.end())
    {
      ret.emplace_back(Fmi::to_string(s.lpnn), s.fmisid);
      processed.insert(s.lpnn);
    }

  return ret;
}

Spine::TaggedFMISIDList StationInfo::translateWSIToFMISID(const std::vector<std::string>& wsis,
                                                          const Fmi::DateTime& t) const
{
  Spine::TaggedFMISIDList ret;

  Spine::Stations wsistations = findWsiStations(wsis);

  std::set<std::string> processed;
  for (const auto& s : wsistations)
    if (t >= s.station_start && t <= s.station_end && processed.find(s.wsi) == processed.end())
    {
      ret.emplace_back(s.wsi, s.fmisid);
      processed.insert(s.wsi);
    }

  return ret;
}

std::vector<int> StationInfo::fmisids() const
{
  std::vector<int> ret;
  for (const auto& fmisid_data : fmisidstations)
    ret.push_back(fmisid_data.first);
  return ret;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
