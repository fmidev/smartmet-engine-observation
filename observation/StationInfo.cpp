#include "StationInfo.h"
#include "Utils.h"
#include <boost/algorithm/string/predicate.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/xml_iarchive.hpp>
#include <boost/archive/xml_oarchive.hpp>
#include <boost/date_time/posix_time/time_serialize.hpp>
#include <boost/filesystem.hpp>
#include <boost/serialization/map.hpp>
#include <boost/serialization/set.hpp>
#include <boost/serialization/vector.hpp>
#include <gis/OGR.h>
#include <macgyver/StringConversion.h>
#include <macgyver/Exception.h>
#include <fstream>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
// Sort based on fmisid
static bool sort_stations_function(const SmartMet::Spine::Station& s1,
                                   const SmartMet::Spine::Station& s2)
{
  return (s1.fmisid < s2.fmisid);
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
            const boost::posix_time::ptime& starttime,
            const boost::posix_time::ptime& endtime)
{
  return !(endtime < station.station_start || starttime > station.station_end);
}

// ----------------------------------------------------------------------
/*!
 * \brief Test if the station belongs to any of the groups
 */
// ----------------------------------------------------------------------

bool groupok(const Spine::Station& station, const std::set<std::string> groups)
{
  // All groups allowed?
  if (groups.empty())
    return true;

  return (groups.find(station.station_type) != groups.end());
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
                                                 const boost::posix_time::ptime& starttime,
                                                 const boost::posix_time::ptime& endtime) const
{
  if (numberofstations < 1)
    throw Fmi::Exception(BCP, "Cannot search for less than 1 nearby stations");

  std::size_t maxcount = static_cast<std::size_t>(numberofstations);

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

    distances.push_back(std::make_pair(distance, id));
  }

  // Sort the candidates based on distance and id (lexicographic sort)

  std::sort(distances.begin(), distances.end());

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
    calculateStationDirection(newstation);

    result.push_back(newstation);
  }

  return result;
}

// ----------------------------------------------------------------------
/*!
 * \brief Utility for finding stations of specific type
 */
// ----------------------------------------------------------------------

Spine::Stations findStations(const Spine::Stations& stations,
                             const std::vector<int>& numbers,
                             const StationIndex& index)
{
  Spine::Stations result;

  for (const auto& number : numbers)
  {
    const auto pos = index.find(number);
    if (pos != index.end())
    {
      for (const auto& id : pos->second)
      {
        const auto& station = stations.at(id);
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

Spine::Stations findStations(const Spine::Stations& stations,
                             const std::set<std::string>& groups,
                             const std::vector<int>& numbers,
                             const StationIndex& index,
                             const boost::posix_time::ptime& starttime,
                             const boost::posix_time::ptime& endtime)
{
  Spine::Stations result;

  for (const auto& number : numbers)
  {
    const auto pos = index.find(number);
    if (pos != index.end())
    {
      for (const auto& id : pos->second)
      {
        const auto& station = stations.at(id);

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

SmartMet::Spine::Stations StationInfo::findFmisidStations(
    const SmartMet::Spine::TaggedFMISIDList& taggedFMISIDs,
    const std::set<std::string>& groups,
    const boost::posix_time::ptime& starttime,
    const boost::posix_time::ptime& endtime) const
{
  std::vector<int> fmisids;
  std::map<int, const SmartMet::Spine::TaggedFMISID*> fmisidMap;

  for (const auto& item : taggedFMISIDs)
  {
    fmisids.push_back(item.fmisid);
    fmisidMap.insert(std::make_pair(item.fmisid, &item));
  }

  SmartMet::Spine::Stations ret = findFmisidStations(fmisids, groups, starttime, endtime);

  // Set direction, distance, tag
  for (auto& station : ret)
  {
    const SmartMet::Spine::TaggedFMISID* tfmisid = fmisidMap.at(station.fmisid);
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

SmartMet::Spine::Stations StationInfo::findFmisidStations(
    const SmartMet::Spine::TaggedFMISIDList& taggedFMISIDs) const
{
  std::vector<int> fmisids;
  std::map<int, const SmartMet::Spine::TaggedFMISID*> fmisidMap;

  for (const auto& item : taggedFMISIDs)
  {
    fmisids.push_back(item.fmisid);
    fmisidMap.insert(std::make_pair(item.fmisid, &item));
  }

  SmartMet::Spine::Stations ret = findFmisidStations(fmisids);

  // Set direction, distance, tag
  for (auto& station : ret)
  {
    const SmartMet::Spine::TaggedFMISID* tfmisid = fmisidMap.at(station.fmisid);
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
                                                const boost::posix_time::ptime& starttime,
                                                const boost::posix_time::ptime& endtime) const
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
                                             const boost::posix_time::ptime& starttime,
                                             const boost::posix_time::ptime& endtime) const
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
                                              const boost::posix_time::ptime& starttime,
                                              const boost::posix_time::ptime& endtime) const
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
                                               const boost::posix_time::ptime& starttime,
                                               const boost::posix_time::ptime& endtime) const
{
  return findStations(stations, groups, rwsids, rwsidstations, starttime, endtime);
}

// ----------------------------------------------------------------------
/*!
 * \brief Find stations in the given groups
 */
// ----------------------------------------------------------------------

Spine::Stations StationInfo::findStationsInGroup(const std::set<std::string>& groups,
                                                 const boost::posix_time::ptime& starttime,
                                                 const boost::posix_time::ptime& endtime) const
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
                                                    const boost::posix_time::ptime& starttime,
                                                    const boost::posix_time::ptime& endtime,
                                                    const std::string& wkt) const
{
  try
  {
    Spine::Stations stations;

    // Get stations belonging to the requested groups and period
    SmartMet::Spine::Stations groupStations = findStationsInGroup(groups, starttime, endtime);
    // Create area geometry from wkt-string
    OGRGeometry* areaGeometry = Fmi::OGR::createFromWkt(wkt, 4326);
    // Create spatial refernce to be used below
    OGRSpatialReference srs;
    srs.importFromEPSGA(4326);

    // Iterate stations
    for (auto station : groupStations)
    {
      // Create Point geometry from sation coordinates
      OGRPoint stationLocation(station.longitude_out, station.latitude_out);
      stationLocation.assignSpatialReference(&srs);

      // If station is inside area accept it
      if (areaGeometry->Contains(&stationLocation))
      {
        stations.push_back(station);
      }
    }
    // Sort in ascending fmisid order
    std::sort(stations.begin(), stations.end(), sort_stations_function);

    return stations;
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
                                              const std::set<std::string>& groups) const
{
  const auto& ids = fmisidstations.at(fmisid);
  for (const auto id : ids)
  {
    const auto& station = stations.at(id);
    if (groupok(station, groups))
      return stations.at(id);
  }

  throw Fmi::Exception(BCP, "No match found for fmisid=" + Fmi::to_string(fmisid));
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
    if (groups.find(station.station_type) != groups.end())
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
      double lon = stations[id].longitude_out;
      if (lon >= minx && lon <= maxx)
      {
        double lat = stations[id].latitude_out;
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
      double lon = stations[id].longitude_out;
      if (lon <= minx || lon >= maxx)
      {
        double lat = stations[id].latitude_out;
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
                                                   const boost::posix_time::ptime& starttime,
                                                   const boost::posix_time::ptime& endtime) const
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

  // Map groups to sets of stations

  for (std::size_t idx = 0; idx < stations.size(); ++idx)
  {
    const auto& station = stations[idx];
    members[station.station_type].insert(idx);
  }

  // Create a latlon search tree for the stations

  for (StationID idx = 0; idx < stations.size(); ++idx)
  {
    const auto& station = stations[idx];
    stationtree.insert(StationNearTreeLatLon{station.longitude_out, station.latitude_out, idx});
  }

  stationtree.flush();
}

Spine::TaggedFMISIDList StationInfo::translateWMOToFMISID(const std::vector<int>& wmos,
                                                          const boost::posix_time::ptime& t) const
{
  Spine::TaggedFMISIDList ret;

  SmartMet::Spine::Stations stations = findWmoStations(wmos);

  std::set<int> processed;
  for (auto s : stations)
    if (t >= s.station_start && t <= s.station_end && processed.find(s.wmo) == processed.end())
    {
      ret.emplace_back(Fmi::to_string(s.wmo), s.fmisid);
      processed.insert(s.wmo);
    }

  return ret;
}

Spine::TaggedFMISIDList StationInfo::translateRWSIDToFMISID(const std::vector<int>& rwsids,
                                                            const boost::posix_time::ptime& t) const
{
  Spine::TaggedFMISIDList ret;

  SmartMet::Spine::Stations stations = findRwsidStations(rwsids);

  std::set<int> processed;
  for (auto s : stations)
    if (t >= s.station_start && t <= s.station_end && processed.find(s.rwsid) == processed.end())
    {
      ret.emplace_back(Fmi::to_string(s.rwsid), s.fmisid);
      processed.insert(s.rwsid);
    }

  return ret;
}

Spine::TaggedFMISIDList StationInfo::translateLPNNToFMISID(const std::vector<int>& lpnns,
                                                           const boost::posix_time::ptime& t) const
{
  Spine::TaggedFMISIDList ret;

  SmartMet::Spine::Stations stations = findLpnnStations(lpnns);

  std::set<int> processed;
  for (auto s : stations)
    if (t >= s.station_start && t <= s.station_end && processed.find(s.lpnn) == processed.end())
    {
      ret.emplace_back(Fmi::to_string(s.lpnn), s.fmisid);
      processed.insert(s.lpnn);
    }

  return ret;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
