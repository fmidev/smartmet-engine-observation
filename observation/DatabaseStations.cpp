#include "DatabaseStations.h"
#include "StationtypeConfig.h"
#include "Utils.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
namespace
{
// Sort based on fmisid
static bool sort_stations_function(const SmartMet::Spine::Station &s1,
                                   const SmartMet::Spine::Station &s2)
{
  return (s1.fmisid < s2.fmisid);
}

/*!
 * \brief Find stations close to the given coordinate with filtering
 */

Spine::Stations findNearestStations(const StationInfo &info,
                                    double longitude,
                                    double latitude,
                                    double maxdistance,
                                    int numberofstations,
                                    const std::set<std::string> &stationgroup_codes,
                                    const boost::posix_time::ptime &starttime,
                                    const boost::posix_time::ptime &endtime)
{
  return info.findNearestStations(
      longitude, latitude, maxdistance, numberofstations, stationgroup_codes, starttime, endtime);
}
/*!
 * \brief Find stations close to the given location with filtering
 */

Spine::Stations findNearestStations(const StationInfo &info,
                                    const Spine::LocationPtr &location,
                                    double maxdistance,
                                    int numberofstations,
                                    const std::set<std::string> &stationgroup_codes,
                                    const boost::posix_time::ptime &starttime,
                                    const boost::posix_time::ptime &endtime)
{
  return findNearestStations(info,
                             location->longitude,
                             location->latitude,
                             maxdistance,
                             numberofstations,
                             stationgroup_codes,
                             starttime,
                             endtime);
}
}  // namespace

void DatabaseStations::getStationsByBoundingBox(Spine::Stations &stations,
                                                const Settings &settings) const
{
  getStationsByBoundingBox(
      stations, settings.starttime, settings.endtime, settings.stationtype, settings.boundingBox);
}

void DatabaseStations::getStationsByArea(Spine::Stations &stations,
                                         const std::string &stationtype,
                                         const boost::posix_time::ptime &starttime,
                                         const boost::posix_time::ptime &endtime,
                                         const std::string &wkt) const
{
  try
  {
    StationtypeConfig::GroupCodeSetType stationgroup_codes;
    getStationGroups(stationgroup_codes, stationtype);

    auto info = boost::atomic_load(&itsObservationEngineParameters->stationInfo);

    stations = info->findStationsInsideArea(stationgroup_codes, starttime, endtime, wkt);

    // Sort in ascending fmisid order
    std::sort(stations.begin(), stations.end(), sort_stations_function);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting stations by area failed!");
  }
}

void DatabaseStations::getStationsByBoundingBox(Spine::Stations &stations,
                                                const boost::posix_time::ptime &starttime,
                                                const boost::posix_time::ptime &endtime,
                                                const std::string &stationtype,
                                                const BoundingBoxSettings &bboxSettings) const
{
  try
  {
    StationtypeConfig::GroupCodeSetType stationgroup_codes;

    getStationGroups(stationgroup_codes, stationtype);

    auto info = boost::atomic_load(&itsObservationEngineParameters->stationInfo);

    try
    {
      auto stationList = info->findStationsInsideBox(bboxSettings.at("minx"),
                                                     bboxSettings.at("miny"),
                                                     bboxSettings.at("maxx"),
                                                     bboxSettings.at("maxy"),
                                                     stationgroup_codes,
                                                     starttime,
                                                     endtime);
      for (const auto &station : stationList)
        stations.push_back(station);

      // Sort in ascending fmisid order
      std::sort(stations.begin(), stations.end(), sort_stations_function);
    }
    catch (...)
    {
      throw Fmi::Exception::Trace(BCP, "Getting stations by bounding box failed!");
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting stations by bounding box failed!");
  }
}

// Translates geoids to fmisid
Spine::TaggedFMISIDList DatabaseStations::translateGeoIdsToFMISID(
    const boost::posix_time::ptime &starttime,
    const boost::posix_time::ptime &endtime,
    const std::string &stationtype,
    const GeoIdSettings &geoidSettings) const
{
  Spine::TaggedFMISIDList ret;

  StationtypeConfig::GroupCodeSetType stationgroup_codes;

  getStationGroups(stationgroup_codes, stationtype);

  Locus::QueryOptions opts;
  opts.SetLanguage(geoidSettings.language);
  opts.SetResultLimit(1);
  opts.SetCountries("");
  opts.SetFullCountrySearch(true);

  auto info = boost::atomic_load(&itsObservationEngineParameters->stationInfo);

  for (int geoid : geoidSettings.geoids)
  {
    auto places = itsGeonames->idSearch(opts, geoid);

    for (const auto &place : places)
    {
      // If the geoid refers to a station, do not search based on distance
      if (place->fmisid)
      {
        ret.emplace_back(Fmi::to_string(geoid), *place->fmisid);
      }
      else
      {
        // Search nearest stations
        auto stations = findNearestStations(*info,
                                            place,
                                            geoidSettings.maxdistance,
                                            geoidSettings.numberofstations,
                                            stationgroup_codes,
                                            starttime,
                                            endtime);

        for (Spine::Station &s : stations)
          ret.emplace_back(Fmi::to_string(geoid), s.fmisid);
      }
    }
  }

  return ret;
}

Spine::TaggedFMISIDList DatabaseStations::translateToFMISID(
    const boost::posix_time::ptime &starttime,
    const boost::posix_time::ptime &endtime,
    const std::string &stationtype,
    const StationSettings &stationSettings) const
{
  Spine::TaggedFMISIDList result;

  if (stationtype == NETATMO_PRODUCER || stationtype == ROADCLOUD_PRODUCER ||
      stationtype == FMI_IOT_PRODUCER)
    return result;

  Spine::TaggedFMISIDList wmos;
  Spine::TaggedFMISIDList lpnns;
  Spine::TaggedFMISIDList geoids;

  auto info = boost::atomic_load(&itsObservationEngineParameters->stationInfo);

  if (stationSettings.wmos.size() > 0)
  {
    if (stationtype == "road")
    {
      wmos = info->translateRWSIDToFMISID(stationSettings.wmos, endtime);
    }
    else
    {
      wmos = info->translateWMOToFMISID(stationSettings.wmos, endtime);
    }
  }

  if (stationSettings.lpnns.size() > 0)
    lpnns = info->translateLPNNToFMISID(stationSettings.lpnns, endtime);

  if (stationSettings.geoid_settings.geoids.size() > 0)
    geoids =
        translateGeoIdsToFMISID(starttime, endtime, stationtype, stationSettings.geoid_settings);

  if (wmos.size() > 0)
  {
    result.insert(result.end(), wmos.begin(), wmos.end());
  }

  if (lpnns.size() > 0)
    result.insert(result.end(), lpnns.begin(), lpnns.end());

  if (geoids.size() > 0)
    result.insert(result.end(), geoids.begin(), geoids.end());

  // FMISIDs need no conversion
  for (auto id : stationSettings.fmisids)
    result.emplace_back(Fmi::to_string(id), id);

  // Bounding box
  if (stationSettings.bounding_box_settings.size() > 0)
  {
    Spine::Stations stations;
    getStationsByBoundingBox(
        stations, starttime, endtime, stationtype, stationSettings.bounding_box_settings);
    std::string bboxTag = DatabaseStations::getTag(stationSettings.bounding_box_settings);
    for (auto s : stations)
      result.emplace_back(bboxTag, s.fmisid);
  }

  // Find FMISIDs of nearest stations
  for (auto nss : stationSettings.nearest_station_settings)
  {
    if (nss.numberofstations > 0)
    {
      std::string nssTag = (nss.tag.empty() ? DatabaseStations::getTag(nss) : nss.tag);

      if (nss.fmisid)
      {
        // The geoid is a station, do not bother searching based on distance. We arbitrarily choose
        // direction 0.0 with distance 0.

        result.emplace_back(nssTag, *nss.fmisid, 0.0, "0");
      }
      else
      {
        StationtypeConfig::GroupCodeSetType stationgroup_codes;
        getStationGroups(stationgroup_codes, stationtype);

        auto stations = findNearestStations(*info,
                                            nss.longitude,
                                            nss.latitude,
                                            nss.maxdistance,
                                            nss.numberofstations,
                                            stationgroup_codes,
                                            starttime,
                                            endtime);

        if (!stations.empty())
        {
          for (Spine::Station &s : stations)
            result.emplace_back(nssTag, s.fmisid, s.stationDirection, s.distance);
        }
      }
    }
  }

  Spine::TaggedFMISIDList ret;

  // Remove duplicates
  std::set<int> accepted_fmisids;
  for (const auto &item : result)
  {
    if (accepted_fmisids.find(item.fmisid) != accepted_fmisids.end())
      continue;

    ret.push_back(item);
    accepted_fmisids.insert(item.fmisid);
  }

  return ret;
}

void DatabaseStations::getStationGroups(std::set<std::string> &stationgroup_codes,
                                        const std::string &stationtype) const
{
  try
  {
    auto stationgroupCodeSet =
        itsObservationEngineParameters->stationtypeConfig.getGroupCodeSetByStationtype(stationtype);
    stationgroup_codes.insert(stationgroupCodeSet->begin(), stationgroupCodeSet->end());
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void DatabaseStations::getStations(Spine::Stations &stations, const Settings &settings) const
{
  try
  {
    StationtypeConfig::GroupCodeSetType stationgroup_codes;

    try
    {
      // Convert the stationtype in the setting to station group codes. Cache
      // station search is using the codes.
      getStationGroups(stationgroup_codes, settings.stationtype);
    }
    catch (...)
    {
      return;
    }

    auto stationstarttime = day_start(settings.starttime);
    auto stationendtime = day_end(settings.endtime);

#ifdef MYDEBUG
    std::cout << "station search start" << std::endl;
#endif
    // Get all stations by different methods

    // 1) get all places for given station type or
    // get nearest stations by named locations (i.e. by its coordinates)
    // We are also getting all stations for a stationtype, don't bother to
    // continue with other means
    // to find stations.

    auto info = boost::atomic_load(&itsObservationEngineParameters->stationInfo);
    // All stations
    if (settings.allplaces)
      stations = info->findStationsInGroup(stationgroup_codes, stationstarttime, stationendtime);
    else
      stations = info->findFmisidStations(
          settings.taggedFMISIDs, stationgroup_codes, stationstarttime, stationendtime);

    stations = removeDuplicateStations(stations);

    // Sort in ascending fmisid order
    std::sort(stations.begin(), stations.end(), sort_stations_function);

#ifdef MYDEBUG
    std::cout << "total number of stations: " << stations.size() << std::endl;
    std::cout << "station search end" << std::endl;
    std::cout << "observation query start" << std::endl;
#endif
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

std::string DatabaseStations::getTag(const BoundingBoxSettings &bboxSettings)
{
  std::string ret;

  ret += Fmi::to_string(bboxSettings.at("minx"));
  ret += ",";
  ret += Fmi::to_string(bboxSettings.at("miny"));
  ret += ",";
  ret += Fmi::to_string(bboxSettings.at("maxx"));
  ret += ",";
  ret += Fmi::to_string(bboxSettings.at("maxy"));

  return ret;
}

std::string DatabaseStations::getTag(const NearestStationSettings &nearestStationSettings)
{
  std::string ret;

  ret += Fmi::to_string(nearestStationSettings.longitude);
  ret += ",";
  ret += Fmi::to_string(nearestStationSettings.latitude);
  ret += ",";
  ret += Fmi::to_string(nearestStationSettings.maxdistance);
  ret += ",";
  ret += Fmi::to_string(nearestStationSettings.numberofstations);

  return ret;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
