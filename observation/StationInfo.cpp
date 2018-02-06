#include "StationInfo.h"

#include <spine/Exception.h>

#include <macgyver/StringConversion.h>

#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/xml_iarchive.hpp>
#include <boost/archive/xml_oarchive.hpp>

#include <boost/date_time/posix_time/time_serialize.hpp>
#include <boost/serialization/map.hpp>
#include <boost/serialization/set.hpp>
#include <boost/serialization/vector.hpp>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/filesystem.hpp>

#include <fstream>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
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
    throw Spine::Exception::Trace(BCP,
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

    std::ofstream file(filename);

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
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "StationInfo serialization failed!");
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
  // Find all stations withing the distance limit
  StationNearTreeLatLon searchpoint{longitude, latitude};
  auto candidates = stationtree.nearestones(
      searchpoint, StationNearTreeLatLon::ChordLength(maxdistance / 1000.0));

  // Validate other search conditions one by one

  Spine::Stations result;
  for (const auto& candidate : candidates)
  {
    double distance = StationNearTreeLatLon::SurfaceLength(candidate.first);
    StationID id = candidate.second.ID();

    const auto& station = stations.at(id);

    bool timeok = ((starttime >= station.station_start && starttime < station.station_end) ||
                   (endtime >= station.station_end && endtime < station.station_end));

    if (!timeok)
      continue;

    // Check whether the station belongs to the right groups
    if (groups.find(station.station_type) == groups.end())
      continue;

    // Update metadata with a new copy
    Spine::Station newstation = station;

    newstation.distance =
        Fmi::to_string(std::round(distance * 10) / 10.0);  // round to 100 meter precision
    newstation.requestedLat = latitude;
    newstation.requestedLon = longitude;

    result.push_back(newstation);

    if (static_cast<long>(result.size()) >= numberofstations)
      break;
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
        bool timeok = ((starttime >= station.station_start && starttime < station.station_end) ||
                       (endtime >= station.station_end && endtime < station.station_end));

        if (timeok)
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
 * \brief Find the given WMO stations
 */
// ----------------------------------------------------------------------

Spine::Stations StationInfo::findFmisidStations(const std::vector<int>& fmisids,
                                                const boost::posix_time::ptime& starttime,
                                                const boost::posix_time::ptime& endtime) const
{
  return findStations(stations, fmisids, fmisidstations, starttime, endtime);
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
                                             const boost::posix_time::ptime& starttime,
                                             const boost::posix_time::ptime& endtime) const
{
  return findStations(stations, wmos, wmostations, starttime, endtime);
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
                                              const boost::posix_time::ptime& starttime,
                                              const boost::posix_time::ptime& endtime) const
{
  return findStations(stations, lpnns, lpnnstations, starttime, endtime);
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
                                               const boost::posix_time::ptime& starttime,
                                               const boost::posix_time::ptime& endtime) const
{
  return findStations(stations, rwsids, rwsidstations, starttime, endtime);
}

// ----------------------------------------------------------------------
/*!
 * \brief Find stations in the given groups
 */
// ----------------------------------------------------------------------

Spine::Stations StationInfo::findStationsInGroup(const std::set<std::string> groups,
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
    bool timeok = ((starttime >= station.station_start && starttime < station.station_end) ||
                   (endtime >= station.station_end && endtime < station.station_end));

    if (timeok)
      result.push_back(station);
  }

  return result;
}

// ----------------------------------------------------------------------
/*!
 * \brief Get the station with the given fmisid
 */
// ----------------------------------------------------------------------

const Spine::Station& StationInfo::getStation(unsigned int fmisid,
                                              const std::set<std::string>& groups) const
{
  std::set<StationID> all_ids;

  const auto& ids = fmisidstations.at(fmisid);
  for (const auto id : ids)
  {
    const auto& station = stations.at(id);
    if (groups.find(station.station_type) != groups.end())
    {
      all_ids.insert(id);
    }
  }

  if (all_ids.empty())
    throw Spine::Exception(BCP, "No match found for fmisid=" + Fmi::to_string(fmisid));

  if (all_ids.size() > 1)
  {
    std::string msg = "Too many matches found for fmisid=" + Fmi::to_string(fmisid) + " :";
    for (const auto id : all_ids)
      msg += " " + stations.at(id).station_type;
    msg += ". Requested:";
    for (const auto& name : groups)
      msg += " " + name;

    throw Spine::Exception(BCP, msg);
  }

  return stations.at(*ids.begin());
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
                                                   const std::set<std::string> groups,
                                                   const boost::posix_time::ptime& starttime,
                                                   const boost::posix_time::ptime& endtime) const
{
  auto ids = searchStations(stations, minx, miny, maxx, maxy);

  Spine::Stations result;

  for (const auto& id : ids)
  {
    const auto& station = stations.at(id);
    bool timeok = ((starttime >= station.station_start && starttime < station.station_end) ||
                   (endtime >= station.station_end && endtime < station.station_end));

    if (timeok)
      if (groups.empty() || groups.find(station.station_type) != groups.end())
        result.push_back(station);
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
      lpnnstations[station.rwsid].insert(idx);
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

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
