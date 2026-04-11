#include "DatabaseStations.h"
#include "DatabaseDriverInfo.h"
#include "StationtypeConfig.h"
#include "Utils.h"
#include <fmt/format.h>
#include <macgyver/StringConversion.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
namespace
{
using namespace Utils;

// Sort based on fmisid
bool sort_stations_function(const Spine::Station &s1, const Spine::Station &s2)
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
                                    const Fmi::DateTime &starttime,
                                    const Fmi::DateTime &endtime)
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
                                    const Fmi::DateTime &starttime,
                                    const Fmi::DateTime &endtime)
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
// Build a comma-separated list of measurand IDs for the requested observation parameters.
// Returns an empty string if there are no observation parameters in the request.
// For weather_data_qc tables the IDs are single-quoted strings, otherwise plain integers.

std::string buildMeasurandIdList(const ParameterMapPtr &parameterMap,
                                 const std::vector<Spine::Parameter> &parameters,
                                 const std::string &stationtype,
                                 bool isWeatherDataQC)
{
  std::string result;
  std::set<std::string> seen;

  for (const auto &p : parameters)
  {
    if (!not_special(p))
      continue;

    std::string name = Fmi::ascii_tolower_copy(p.name());

    // Strip possible sensor number suffix (e.g., "KELI_1" → "KELI")
    auto pos = name.find_last_of('_');
    if (pos != std::string::npos)
    {
      auto suffix = name.substr(pos + 1);
      bool is_sensor = !suffix.empty() && std::all_of(suffix.begin(), suffix.end(), ::isdigit);
      if (is_sensor)
        name = name.substr(0, pos);
    }

    auto mid = parameterMap->getParameter(name, stationtype);
    if (mid.empty())
      continue;

    if (!seen.insert(mid).second)
      continue;  // duplicate

    if (!result.empty())
      result += ',';

    if (isWeatherDataQC)
      result += "'" + mid + "'";
    else
      result += mid;
  }

  return result;
}

// Resolve the cache table name for a given stationtype.
// Returns "observation_data" or "weather_data_qc", or empty if not applicable.

std::string resolveCacheTableName(const StationtypeConfig &stc, const std::string &stationtype)
{
  try
  {
    auto tablename = stc.getDatabaseTableNameByStationtype(stationtype);
    if (tablename == "observation_data_r1")
      return OBSERVATION_DATA_TABLE;
    if (tablename == "weather_data_qc")
      return WEATHER_DATA_QC_TABLE;
  }
  catch (...)
  {
  }
  return {};
}

}  // namespace

void DatabaseStations::getStationsByArea(Spine::Stations &stations,
                                         const Settings &settings,
                                         const std::string &wkt) const
{
  try
  {
    StationtypeConfig::GroupCodeSetType stationgroup_codes;
    getStationGroups(stationgroup_codes, settings.stationtype, settings.stationgroups);

    auto info = itsObservationEngineParameters->stationInfo.load();

    stations =
        info->findStationsInsideArea(stationgroup_codes, settings.starttime, settings.endtime, wkt);

    // Sort in ascending fmisid order
    std::sort(stations.begin(), stations.end(), sort_stations_function);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting stations by area failed!");
  }
}

void DatabaseStations::getStationsByBoundingBox(Spine::Stations &stations,
                                                const Settings &settings,
                                                const BoundingBoxSettings &bboxSettings) const
{
  try
  {
    StationtypeConfig::GroupCodeSetType stationgroup_codes;

    getStationGroups(stationgroup_codes, settings.stationtype, settings.stationgroups);

    auto info = itsObservationEngineParameters->stationInfo.load();

    try
    {
      auto stationList = info->findStationsInsideBox(bboxSettings.at("minx"),
                                                     bboxSettings.at("miny"),
                                                     bboxSettings.at("maxx"),
                                                     bboxSettings.at("maxy"),
                                                     stationgroup_codes,
                                                     settings.starttime,
                                                     settings.endtime);
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
    const Settings &settings, const GeoIdSettings &geoidSettings) const
{
  Spine::TaggedFMISIDList ret;

  StationtypeConfig::GroupCodeSetType stationgroup_codes;

  getStationGroups(stationgroup_codes, settings.stationtype, settings.stationgroups);

  Locus::QueryOptions opts;
  opts.SetLanguage(geoidSettings.language);
  opts.SetResultLimit(1);
  opts.SetCountries("");
  opts.SetFullCountrySearch(true);

  auto info = itsObservationEngineParameters->stationInfo.load();

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
                                            settings.starttime,
                                            settings.endtime);

        for (Spine::Station &s : stations)
          ret.emplace_back(Fmi::to_string(geoid), s.fmisid);
      }
    }
  }

  return ret;
}

Spine::TaggedFMISIDList DatabaseStations::translateToFMISID(
    const Settings &settings, const StationSettings &stationSettings) const
{
  Spine::TaggedFMISIDList result;

  if (settings.stationtype == NETATMO_PRODUCER || settings.stationtype == ROADCLOUD_PRODUCER ||
      settings.stationtype == FMI_IOT_PRODUCER || settings.stationtype == TAPSI_QC_PRODUCER)
    return result;

  auto info = itsObservationEngineParameters->stationInfo.load();

  // Convert input station identifiers to tagged stations

  auto wmos = info->translateWMOToFMISID(stationSettings.wmos, settings.endtime);
  auto rwsids = info->translateRWSIDToFMISID(stationSettings.rwsids, settings.endtime);
  auto lpnns = info->translateLPNNToFMISID(stationSettings.lpnns, settings.endtime);
  auto wsis = info->translateWSIToFMISID(stationSettings.wsis, settings.endtime);
  auto geoids = translateGeoIdsToFMISID(settings, stationSettings.geoid_settings);

  // FMISIDs need no translation
  for (auto id : stationSettings.fmisids)
    result.emplace_back(Fmi::to_string(id), id);

  result.insert(result.end(), wmos.begin(), wmos.end());
  result.insert(result.end(), lpnns.begin(), lpnns.end());
  result.insert(result.end(), geoids.begin(), geoids.end());
  result.insert(result.end(), wsis.begin(), wsis.end());

  // Bounding box
  if (!stationSettings.bounding_box_settings.empty())
  {
    Spine::Stations stations;
    getStationsByBoundingBox(stations, settings, stationSettings.bounding_box_settings);
    std::string bboxTag = DatabaseStations::getTag(stationSettings.bounding_box_settings);
    for (const auto &s : stations)
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
        getStationGroups(stationgroup_codes, settings.stationtype, settings.stationgroups);

        // Resolve cache table name and measurand IDs to decide whether to filter
        // stations by data availability. This avoids returning stations that are
        // nearby but have no data for the requested parameters (e.g. a buoy station
        // when temperature and precipitation are requested).

        auto cacheTableName = resolveCacheTableName(
            itsObservationEngineParameters->stationtypeConfig, settings.stationtype);

        bool isWeatherDataQC = (cacheTableName == WEATHER_DATA_QC_TABLE);

        auto measurandIds = buildMeasurandIdList(
            itsObservationEngineParameters->parameterMap,
            settings.parameters,
            settings.stationtype,
            isWeatherDataQC);

        // Get the cache for checking data availability
        std::shared_ptr<ObservationCache> cache;
        if (!measurandIds.empty() && !cacheTableName.empty())
          cache = itsObservationEngineParameters->observationCacheProxy->getCacheByTableName(
              cacheTableName);

        if (cache)
        {
          // Fetch a few extra candidates so that stations which lack the requested
          // parameters can be skipped without reducing the result count.  The number
          // of extra candidates is configurable (nearestStationExtraCandidates, default 3).

          int extra = itsObservationEngineParameters->nearestStationExtraCandidates;

          auto candidates = findNearestStations(*info,
                                                nss.longitude,
                                                nss.latitude,
                                                nss.maxdistance,
                                                nss.numberofstations + extra,
                                                stationgroup_codes,
                                                settings.starttime,
                                                settings.endtime);

          if (!candidates.empty())
          {
            // Lightweight SQL check: which candidate stations actually have data?
            std::vector<int> candidate_fmisids;
            candidate_fmisids.reserve(candidates.size());
            for (const auto &s : candidates)
              candidate_fmisids.push_back(s.fmisid);

            auto valid = cache->stationsWithObservations(
                candidate_fmisids, measurandIds, settings.starttime, settings.endtime,
                cacheTableName);

            // Pick the first numberofstations that have data (candidates are distance-sorted)
            int added = 0;
            for (const auto &s : candidates)
            {
              if (valid.find(s.fmisid) != valid.end())
              {
                result.emplace_back(nssTag, s.fmisid, s.stationDirection, s.distance);
                if (++added >= nss.numberofstations)
                  break;
              }
            }
          }
        }
        else
        {
          // No filtering possible: no observation parameters or no cache available.
          // Fall back to the original behaviour.

          auto stations = findNearestStations(*info,
                                              nss.longitude,
                                              nss.latitude,
                                              nss.maxdistance,
                                              nss.numberofstations,
                                              stationgroup_codes,
                                              settings.starttime,
                                              settings.endtime);

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
                                        const std::string &stationtype,
                                        const std::set<std::string> &stationgroups) const
{
  try
  {
    auto stationgroupCodeSet =
        itsObservationEngineParameters->stationtypeConfig.getGroupCodeSetByStationtype(stationtype);

    // Use all if there is no desired subgroup, otherwise use set intersection only
    if (stationgroups.empty())
      stationgroup_codes.insert(stationgroupCodeSet->begin(), stationgroupCodeSet->end());
    else
      for (const auto &desired_group : stationgroups)
        if (stationgroupCodeSet->find(desired_group) != stationgroupCodeSet->end())
          stationgroup_codes.insert(desired_group);
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
      getStationGroups(stationgroup_codes, settings.stationtype, settings.stationgroups);
    }
    catch (...)
    {
      return;
    }

    auto stationstarttime = day_start(settings.starttime);
    auto stationendtime = day_end(settings.endtime);

#ifdef MYDEBUG
    std::cout << "station search start\n";
#endif
    // Get all stations by different methods

    // get all places for given station type or get nearest stations by named locations (i.e. by its
    // coordinates) We are also getting all stations for a stationtype, don't bother to continue
    // with other means to find stations.

    auto info = itsObservationEngineParameters->stationInfo.load();
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
    std::cout << "total number of stations: " << stations.size() << '\n';
    std::cout << "station search end\n";
    std::cout << "observation query start\n";
#endif
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

std::string DatabaseStations::getTag(const BoundingBoxSettings &bboxSettings)
{
  return fmt::format("{},{},{},{}",
                     bboxSettings.at("minx"),
                     bboxSettings.at("miny"),
                     bboxSettings.at("maxx"),
                     bboxSettings.at("maxy"));
}

std::string DatabaseStations::getTag(const NearestStationSettings &nearestStationSettings)
{
  return fmt::format("{},{},{},{}",
                     nearestStationSettings.longitude,
                     nearestStationSettings.latitude,
                     nearestStationSettings.maxdistance,
                     nearestStationSettings.numberofstations);
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
