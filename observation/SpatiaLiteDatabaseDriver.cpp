#include "SpatiaLiteDatabaseDriver.h"
#include "ObservationCache.h"
#include "StationInfo.h"
#include "StationtypeConfig.h"
#include "QueryResult.h"
#include "QueryResultBase.h"
#include "DatabaseDriverParameters.h"

#include <chrono>
#include <spine/Convenience.h>

#include "boost/date_time/posix_time/posix_time.hpp" //include all types plus i/o

namespace ts = SmartMet::Spine::TimeSeries;

namespace SmartMet {
namespace Engine {
namespace Observation {
namespace {
/*!
 * \brief Find stations close to the given coordinate with filtering
 */

Spine::Stations
findNearestStations(const StationInfo &info, double longitude, double latitude,
                    double maxdistance, int numberofstations,
                    const std::set<std::string> &stationgroup_codes,
                    const boost::posix_time::ptime &starttime,
                    const boost::posix_time::ptime &endtime) {
  return info.findNearestStations(longitude, latitude, maxdistance,
                                  numberofstations, stationgroup_codes,
                                  starttime, endtime);
}
/*!
 * \brief Find stations close to the given location with filtering
 */

Spine::Stations
findNearestStations(const StationInfo &info, const Spine::LocationPtr &location,
                    double maxdistance, int numberofstations,
                    const std::set<std::string> &stationgroup_codes,
                    const boost::posix_time::ptime &starttime,
                    const boost::posix_time::ptime &endtime) {
  return findNearestStations(info, location->longitude, location->latitude,
                             maxdistance, numberofstations, stationgroup_codes,
                             starttime, endtime);
}

}; // anonymous namespace
SpatiaLiteDatabaseDriver::SpatiaLiteDatabaseDriver(
    boost::shared_ptr<DatabaseDriverParameters> p)
    : DatabaseDriver(p) {}

void SpatiaLiteDatabaseDriver::initializeConnectionPool() {
  *(itsDriverParameters->connectionsOK) = true;
}

void SpatiaLiteDatabaseDriver::shutdown() {
  try {
    // Shutting down cache connections
    itsDriverParameters->observationCache->shutdown();
  }
  catch (...) {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

boost::shared_ptr<Spine::Table> SpatiaLiteDatabaseDriver::makeQuery(
    Settings &settings,
    boost::shared_ptr<Spine::ValueFormatter> &valueFormatter) {

  boost::shared_ptr<Spine::Table> data;
  return data;
}

void SpatiaLiteDatabaseDriver::makeQuery(QueryBase *qb) {
  try {
    if (*(itsDriverParameters->shutdownRequested))
      return;

    if (qb == NULL) {
      std::ostringstream msg;
      msg << "SpatiaLiteDatabaseDriver::makeQuery : Implementation of '"
          << typeid(qb).name() << "' class is missing.\n";

      Spine::Exception exception(BCP, "Invalid parameter value!");

      exception.addDetail(msg.str());
      throw exception;
    }

    const std::string sqlStatement = qb->getSQLStatement();

    if (sqlStatement.empty()) {
      std::ostringstream msg;
      msg << "SpatiaLiteDatabaseDriver::makeQuery : SQL statement of '"
          << typeid(*qb).name() << "' class is empty.\n";

      Spine::Exception exception(BCP, "Invalid parameter value!");
      exception.addDetail(msg.str());
      throw exception;
    }

    std::shared_ptr<QueryResultBase> result = qb->getQueryResultContainer();

    // Try cache first
    boost::optional<std::shared_ptr<QueryResultBase> > cacheResult =
        itsDriverParameters->queryResultBaseCache->find(sqlStatement);
    if (cacheResult) {
      if (result->set(cacheResult.get()))
        return;
    }

    if (result == NULL) {
      std::ostringstream msg;
      msg << "SpatiaLiteDatabaseDriver::makeQuery : Result container of '"
          << typeid(*qb).name() << "' class not found.\n";

      Spine::Exception exception(BCP, "Invalid parameter value!");
      exception.addDetail(msg.str());
      throw exception;
    }
  }
  catch (...) {
    Spine::Exception exception(BCP, "Operation failed!");
    throw exception;
  }
}

ts::TimeSeriesVectorPtr SpatiaLiteDatabaseDriver::values(Settings &settings) {
  ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

  try {
    if (*(itsDriverParameters->shutdownRequested))
      return NULL;

    // Do sanity check for the parameters
    for (const Spine::Parameter &p : settings.parameters) {
      if (not_special(p)) {
        std::string name = parseParameterName(p.name());
        if (!isParameter(name, settings.stationtype) &&
            !isParameterVariant(name)) {
          throw Spine::Exception(BCP,
                                 "No parameter name " + name + " configured.");
        }
      }
    }

    if (itsDriverParameters->stationtypeConfig->getUseCommonQueryMethod(
                                                    settings.stationtype)
        and settings.producer_ids.empty())
      settings.producer_ids =
          *itsDriverParameters->stationtypeConfig
               ->getProducerIdSetByStationtype(settings.stationtype);

    auto stationgroupCodeSet =
        itsDriverParameters->stationtypeConfig->getGroupCodeSetByStationtype(
            settings.stationtype);
    settings.stationgroup_codes.insert(stationgroupCodeSet->begin(),
                                       stationgroupCodeSet->end());

    // Try first from cache and on failure (Spine::Exception::) get from
    // database.
    try {
      // Get all data from Cache database if all requirements below apply:
      // 1) stationtype is cached
      // 2) we have the requested time interval in cache
      // 3) stations are available in Cache
      if (settings.useDataCache &&
          itsDriverParameters->observationCache->dataAvailableInCache(
              settings) &&
          itsDriverParameters->observationCache->cacheHasStations()) {
        return itsDriverParameters->observationCache->valuesFromCache(settings);
      }
    }
    catch (...) {
      throw Spine::Exception(BCP, "Operation failed!", NULL);
    }
  }
  catch (...) {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }

  return ret;
}

/*
 * \brief Read values for given times only.
 */

Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLiteDatabaseDriver::values(
    Settings &settings,
    const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions) {

  ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

  try {
    // Do sanity check for the parameters
    for (const Spine::Parameter &p : settings.parameters) {
      if (not_special(p)) {
        std::string name = parseParameterName(p.name());
        if (!isParameter(name, settings.stationtype) &&
            !isParameterVariant(name)) {
          throw Spine::Exception(BCP,
                                 "No parameter name " + name + " configured.");
        }
      }
    }

    if (itsDriverParameters->stationtypeConfig->getUseCommonQueryMethod(
                                                    settings.stationtype)
        and settings.producer_ids.empty())
      settings.producer_ids =
          *itsDriverParameters->stationtypeConfig
               ->getProducerIdSetByStationtype(settings.stationtype);
    auto stationgroupCodeSet =
        itsDriverParameters->stationtypeConfig->getGroupCodeSetByStationtype(
            settings.stationtype);
    settings.stationgroup_codes.insert(stationgroupCodeSet->begin(),
                                       stationgroupCodeSet->end());

    try {
      // Get all data from Cache database if all requirements below apply:
      // 1) stationtype is cached
      // 2) we have the requested time interval in cache
      // 3) stations are available in cache
      if (settings.useDataCache &&
          itsDriverParameters->observationCache->dataAvailableInCache(
              settings) &&
          itsDriverParameters->observationCache->cacheHasStations())
        return itsDriverParameters->observationCache->valuesFromCache(
            settings, timeSeriesOptions);
    }
    catch (...) {
      throw Spine::Exception(BCP, "Operation failed!", NULL);
    }
  }
  catch (...) {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
  return ret;
}

bool
SpatiaLiteDatabaseDriver::isParameter(const std::string &alias,
                                      const std::string &stationType) const {
  try {
    std::string parameterAliasName = Fmi::ascii_tolower_copy(alias);
    Engine::Observation::removePrefix(parameterAliasName, "qc_");

    // Is the alias configured.
    std::map<std::string, std::map<std::string, std::string> >::const_iterator
    namePtr = itsDriverParameters->parameterMap->find(parameterAliasName);

    if (namePtr == itsDriverParameters->parameterMap->end())
      return false;

    // Is the stationType configured inside configuration block of the alias.
    std::string stationTypeLowerCase = Fmi::ascii_tolower_copy(stationType);
    std::map<std::string, std::string>::const_iterator stationTypeMapPtr =
        namePtr->second.find(stationTypeLowerCase);

    if (stationTypeMapPtr == namePtr->second.end())
      return false;

    return true;
  }
  catch (...) {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

bool
SpatiaLiteDatabaseDriver::isParameterVariant(const std::string &name) const {
  try {
    std::string parameterLowerCase = Fmi::ascii_tolower_copy(name);
    Engine::Observation::removePrefix(parameterLowerCase, "qc_");
    // Is the alias configured.
    std::map<std::string, std::map<std::string, std::string> >::const_iterator
    namePtr = itsDriverParameters->parameterMap->find(parameterLowerCase);

    if (namePtr == itsDriverParameters->parameterMap->end())
      return false;

    return true;
  }
  catch (...) {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

FlashCounts SpatiaLiteDatabaseDriver::getFlashCount(
    const boost::posix_time::ptime &starttime,
    const boost::posix_time::ptime &endtime,
    const Spine::TaggedLocationList &locations) {
  try {
    Settings settings;
    settings.stationtype = "flash";

    if (itsDriverParameters->observationCache->flashIntervalIsCached(starttime,
                                                                     endtime)) {
      return itsDriverParameters->observationCache->getFlashCount(
          starttime, endtime, locations);
    } else {

      return FlashCounts();
    }
  }
  catch (...) {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

boost::shared_ptr<std::vector<ObservableProperty> >
SpatiaLiteDatabaseDriver::observablePropertyQuery(
    std::vector<std::string> &parameters, const std::string language) {

  return itsDriverParameters->observationCache->observablePropertyQuery(
      parameters, language);
}

void SpatiaLiteDatabaseDriver::getStations(Spine::Stations &stations,
                                           Settings &settings) {
  try {
    try {
      // Convert the stationtype in the setting to station group codes. Cache
      // station search is
      // using the codes.
      auto stationgroupCodeSet =
          itsDriverParameters->stationtypeConfig->getGroupCodeSetByStationtype(
              settings.stationtype);
      settings.stationgroup_codes.insert(stationgroupCodeSet->begin(),
                                         stationgroupCodeSet->end());
    }
    catch (...) {
      return;
    }
    auto stationstarttime = day_start(settings.starttime);
    auto stationendtime = day_end(settings.endtime);

#ifdef MYDEBUG
    cout << "station search start" << endl;
#endif
    // Get all stations by different methods

    // 1) get all places for given station type or
    // get nearest stations by named locations (i.e. by its coordinates)
    // We are also getting all stations for a stationtype, don't bother to
    // continue with other means
    // to find stations.

    auto info = itsDriverParameters->stationInfo->load();

    if (settings.allplaces) {
      Spine::Stations allStationsFromGroups =
          itsDriverParameters->observationCache->findAllStationsFromGroups(
              settings.stationgroup_codes, *info, stationstarttime,
              stationendtime);

      stations = allStationsFromGroups;
      return;
    } else {
      auto taggedStations =
          itsDriverParameters->observationCache->getStationsByTaggedLocations(
              settings.taggedLocations, settings.numberofstations,
              settings.stationtype, settings.maxdistance,
              settings.stationgroup_codes, settings.starttime,
              settings.endtime);
      for (const auto &s : taggedStations)
        stations.push_back(s);

      // TODO: Remove this legacy support for Locations when obsplugin is
      // deprecated
      if (!settings.locations.empty()) {
        for (const auto &loc : settings.locations) {
          // BUG? Why is maxdistance int?
          std::string locationCacheKey = getLocationCacheKey(
              loc->geoid, settings.numberofstations, settings.stationtype,
              boost::numeric_cast<int>(settings.maxdistance), stationstarttime,
              stationendtime);
          auto cachedStations =
              itsDriverParameters->locationCache->find(locationCacheKey);

          if (cachedStations) {
            for (Spine::Station &cachedStation : *cachedStations) {
              cachedStation.tag = settings.missingtext;
              stations.push_back(cachedStation);
            }
          } else {
            auto newStations = findNearestStations(
                *info, loc, settings.maxdistance, settings.numberofstations,
                settings.stationgroup_codes, stationstarttime, stationendtime);

            if (!newStations.empty()) {
              for (Spine::Station &newStation : newStations) {
                newStation.tag = settings.missingtext;
                stations.push_back(newStation);
              }
              itsDriverParameters->locationCache->insert(locationCacheKey,
                                                         newStations);
            }
          }
        }
      }

      for (const auto &coordinate : settings.coordinates) {
        auto newStations = findNearestStations(
            *info, coordinate.at("lon"), coordinate.at("lat"),
            settings.maxdistance, settings.numberofstations,
            settings.stationgroup_codes, stationstarttime, stationendtime);

        for (const Spine::Station &newStation : newStations)
          stations.push_back(newStation);
      }
    }

    std::vector<int> fmisid_collection;

    // 2) Get stations by WMO or RWSID identifier
    /*
if (!settings.wmos.empty()) {
  std::vector<int> fmisids;
  if (settings.stationtype == "road") {
    fmisids = db.translateRWSIDToFMISID(settings.wmos);
  } else {
    fmisids = db.translateWMOToFMISID(settings.wmos);
  }
  for (int fmisid : fmisids)
    fmisid_collection.push_back(fmisid);
}
    */
    // 3) Get stations by LPNN number
    /*
if (!settings.lpnns.empty()) {
  std::vector<int> fmisids = db.translateLPNNToFMISID(settings.lpnns);
  for (int fmisid : fmisids)
    fmisid_collection.push_back(fmisid);
}
    */

    // 4) Get stations by FMISID number
    for (int fmisid : settings.fmisids) {
      Spine::Station s;
      if (itsDriverParameters->observationCache->getStationById(
              s, fmisid, settings.stationgroup_codes)) { // Chenking that some
                                                         // station group match.
        fmisid_collection.push_back(s.station_id);
      }
    }

    // Find station data by using fmisid
    std::vector<Spine::Station> station_collection;
    for (int fmisid : fmisid_collection) {
      Spine::Station s;

      if (not itsDriverParameters->observationCache->getStationById(
              s, fmisid, settings.stationgroup_codes))
        continue;

      stations.push_back(s);
      station_collection.push_back(s);
    }

    // Find the nearest stations of the requested station(s) if wanted.
    // Default value for settings.numberofstations is 1.
    if (settings.numberofstations > 1) {
      for (const Spine::Station &s : station_collection) {
        auto newStations = findNearestStations(
            *info, s.longitude_out, s.latitude_out, settings.maxdistance,
            settings.numberofstations, settings.stationgroup_codes,
            stationstarttime, stationendtime);

        for (const Spine::Station &nstation : newStations)
          stations.push_back(nstation);
      }
    }

    // 5) Get stations by geoid

    if (!settings.geoids.empty()) {
      // Cache
      for (int geoid : settings.geoids) {
        Locus::QueryOptions opts;
        opts.SetLanguage(settings.language);
        opts.SetResultLimit(1);
        opts.SetCountries("");
        opts.SetFullCountrySearch(true);

        auto places = itsDriverParameters->geonames->idSearch(opts, geoid);
        if (!places.empty()) {
          for (const auto &place : places) {
            auto newStations = findNearestStations(
                *info, place, settings.maxdistance, settings.numberofstations,
                settings.stationgroup_codes, stationstarttime, stationendtime);

            if (!newStations.empty()) {
              newStations.front().geoid = geoid;
              for (Spine::Station &s : newStations) {
                // Set tag to be the requested geoid
                s.tag = Fmi::to_string(geoid);
                stations.push_back(s);
              }
            }
          }
        }
      }
    }

    // 6) Get stations if bounding box is given
    if (!settings.boundingBox.empty()) {
      itsDriverParameters->observationCache->getStationsByBoundingBox(stations,
                                                                      settings);
    }

// 7) For output purposes, translate the station id to WMO only if needed.
// LPNN conversion is done anyway, because the data is sorted by LPNN number
// in data views. For road weather or mareograph stations this is not
// applicable, because
// there is no LPNN or WMO numbers for them
/*
  if (settings.stationtype != "road" &&
      settings.stationtype != "mareograph") {
    for (const Spine::Parameter &p : settings.parameters) {
      if (p.name() == "wmo") {
        db.translateToWMO(stations);
        break;
      }
    }
  }
*/
// 8) For road stations, translate FMISID to RWSID if requested
/*
  if (settings.stationtype == "road") {
    for (const Spine::Parameter &p : settings.parameters) {
      if (p.name() == "rwsid") {
        db.translateToRWSID(stations);
        break;
      }
    }
  }
*/

#ifdef MYDEBUG
    cout << "total number of stations: " << stations.size() << endl;
    cout << "station search end" << endl;
    cout << "observation query start" << endl;
#endif
  }
  catch (...) {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void SpatiaLiteDatabaseDriver::updateFlashCache() {}

void SpatiaLiteDatabaseDriver::updateObservationCache() {}

void SpatiaLiteDatabaseDriver::updateWeatherDataQCCache() {}

void SpatiaLiteDatabaseDriver::locationsFromDatabase() {}

void SpatiaLiteDatabaseDriver::preloadStations(
    const std::string &serializedStationsFile) {}

std::string SpatiaLiteDatabaseDriver::id() const { return "spatialite"; }

} // namespace Observation
} // namespace Engine
} // namespace SmartMet
