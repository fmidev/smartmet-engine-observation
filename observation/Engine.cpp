#include "Engine.h"
#include "ObservationCacheFactory.h"
#include "DatabaseDriverFactory.h"
#include "DBRegistry.h"
#include "DatabaseDriverParameters.h"

#include <spine/Convenience.h>
#include <macgyver/Geometry.h>

// #define MYDEBUG 1

namespace ts = SmartMet::Spine::TimeSeries;

namespace SmartMet {
namespace Engine {
namespace Observation {

Engine::Engine(const std::string &configfile)
    : itsConfigFile(configfile), itsDatabaseRegistry(new DBRegistry()) {}

void Engine::init() {
  try {
    itsEngineParameters.reset(new EngineParameters(itsConfigFile));
    itsDatabaseRegistry->loadConfigurations(
        itsEngineParameters->dbRegistryFolderPath);

    // Initialize the caches
    initializeCache();

    // Read itsPreloadedStations from disk if available
    unserializeStations();

    itsObservationCache.reset(ObservationCacheFactory::create(
        itsEngineParameters->observationCacheParameters));
    itsEngineParameters->databaseDriverParameters->observationCache =
        itsObservationCache;
    itsDatabaseDriver.reset(DatabaseDriverFactory::create(
        itsEngineParameters->databaseDriverParameters));
    if (itsDatabaseDriver) {
      logMessage(" [Observation Engine] database driver '" +
                     itsDatabaseDriver->id() + "' created",
                 itsEngineParameters->quiet);
    }

    itsReady = true;
  }
  catch (...) {
    throw Spine::Exception(BCP, "Observation-engine initialization failed",
                           NULL);
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Shutdown the engine
 */
// ----------------------------------------------------------------------

void Engine::shutdown() {
  try {
    std::cout << "  -- Shutdown requested (Observation)" << std::endl;

    itsEngineParameters->shutdownRequested = true;

    itsDatabaseDriver->shutdown();

    // Waiting active threads to terminate

    while (itsActiveThreadCount > 0) {
      boost::this_thread::sleep(boost::posix_time::milliseconds(100));
    }
  }
  catch (...) {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::unserializeStations() {
  try {
    boost::filesystem::path path =
        boost::filesystem::path(itsEngineParameters->serializedStationsFile);
    if (boost::filesystem::exists(path) && !boost::filesystem::is_empty(path)) {
      jss::shared_ptr<StationInfo> stationinfo =
          jss::make_shared<StationInfo>();
      stationinfo->unserialize(itsEngineParameters->serializedStationsFile);

      //  This is atomic
      itsEngineParameters->stationInfo = stationinfo;
      logMessage(
          " [Observation Engine] Unserialized stations successfully from " +
              path.string(),
          itsEngineParameters->quiet);
    } else {
      logMessage(
          " [Observation Engine] No serialized station file found from " +
              path.string(),
          itsEngineParameters->quiet);
    }
  }
  catch (...) {
    throw Spine::Exception(BCP, "Failed to unserialize station info!", NULL);
  }
}

void Engine::updateFlashCache() { itsDatabaseDriver->updateFlashCache(); }

void Engine::updateObservationCache() {
  itsDatabaseDriver->updateObservationCache();
}

void Engine::updateWeatherDataQCCache() {
  itsDatabaseDriver->updateWeatherDataQCCache();
}

void Engine::cacheFromDatabase() {
  try {
    if (!itsEngineParameters->connectionsOK) {
      errorLog(" [Observation Engine] cacheFromDatabase(): No connection to "
               "Database");
      return;
    }

    if (itsEngineParameters->shutdownRequested)
      return;

    // Updates are disabled for example in regression tests and sometimes when
    // profiling
    if (itsEngineParameters->disableUpdates)
      return;

    itsDatabaseDriver->locationsFromDatabase();

    itsUpdateCacheLoopThread.reset(new boost::thread(
        boost::bind(&Observation::Engine::updateCacheLoop, this)));
    itsUpdateWeatherDataQCCacheLoopThread.reset(new boost::thread(
        boost::bind(&Observation::Engine::updateWeatherDataQCCacheLoop, this)));
    itsUpdateFlashCacheLoopThread.reset(new boost::thread(
        boost::bind(&Observation::Engine::updateFlashCacheLoop, this)));
    itsPreloadStationThread.reset(new boost::thread(
        boost::bind(&Observation::Engine::preloadStations, this)));
  }
  catch (...) {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::updateCacheLoop() {
  try {
    itsActiveThreadCount++;
    while (!itsEngineParameters->shutdownRequested) {
      try {
        updateObservationCache();
      }
      catch (std::exception &err) {
        logMessage(
            std::string(" [Observation Engine] updateObservationCache(): ") +
                err.what(),
            itsEngineParameters->quiet);
      }
      catch (...) {
        logMessage(
            " [Observation Engine] updateObservationCache(): unknown error",
            itsEngineParameters->quiet);
      }

      // Total time to sleep in milliseconds
      int remaining = itsEngineParameters->finUpdateInterval * 1000;
      while (remaining > 0 && !itsEngineParameters->shutdownRequested) {
        int sleeptime = std::min(500, remaining);
        boost::this_thread::sleep(boost::posix_time::milliseconds(sleeptime));
        remaining -= sleeptime;
      }
    }
    itsActiveThreadCount--;
  }
  catch (...) {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::updateFlashCacheLoop() {
  try {
    itsActiveThreadCount++;
    while (!itsEngineParameters->shutdownRequested) {
      try {
        updateFlashCache();
      }
      catch (std::exception &err) {
        logMessage(
            std::string(
                " [Observation Engine] updateFlashCacheFromDatabase(): ") +
                err.what(),
            itsEngineParameters->quiet);
      }
      catch (...) {
        logMessage(" [Observation Engine] updateFlashCacheFromDatabase(): "
                   "unknown error",
                   itsEngineParameters->quiet);
      }

      // Total time to sleep in milliseconds
      int remaining = itsEngineParameters->flashUpdateInterval * 1000;
      while (remaining > 0 && !itsEngineParameters->shutdownRequested) {
        int sleeptime = std::min(500, remaining);
        boost::this_thread::sleep(boost::posix_time::milliseconds(sleeptime));
        remaining -= sleeptime;
      }
    }
    itsActiveThreadCount--;
  }
  catch (...) {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::updateWeatherDataQCCacheLoop() {
  try {
    itsActiveThreadCount++;
    while (!itsEngineParameters->shutdownRequested) {
      try {
        updateWeatherDataQCCache();
      }
      catch (std::exception &err) {
        logMessage(std::string(" [Observation Engine] "
                               "updateWeatherDataQCCacheFromDatabase(): ") +
                       err.what(),
                   itsEngineParameters->quiet);
      }
      catch (...) {
        logMessage(" [Observation Engine] "
                   "updateWeatherDataQCCacheFromDatabase(): unknown error",
                   itsEngineParameters->quiet);
      }

      // Total time to sleep in milliseconds
      int remaining = itsEngineParameters->extUpdateInterval * 1000;
      while (remaining > 0 && !itsEngineParameters->shutdownRequested) {
        int sleeptime = std::min(500, remaining);
        boost::this_thread::sleep(boost::posix_time::milliseconds(sleeptime));
        remaining -= sleeptime;
      }
    }
    itsActiveThreadCount--;
  }
  catch (...) {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::preloadStations() {
  if (!itsEngineParameters->preloaded || itsEngineParameters->forceReload) {
    itsActiveThreadCount++;
    itsDatabaseDriver->preloadStations(
        itsEngineParameters->serializedStationsFile);

    if (itsEngineParameters->connectionsOK &&
        !itsEngineParameters->shutdownRequested) {
      // Doesn't really matter that these aren't atomic
      itsEngineParameters->preloaded = true;
      itsReady = true;
      itsEngineParameters->forceReload = false;
      logMessage(" [Observation Engine] Preloading stations done.",
                 itsEngineParameters->quiet);
    }
    itsActiveThreadCount--;
  }
}

bool Engine::stationExistsInTimeRange(const Spine::Station &station,
                                      const boost::posix_time::ptime &starttime,
                                      const boost::posix_time::ptime &endtime) {
  try {
    // No up-to-date existence info for these station types
    if (station.isRoadStation || station.isSYKEStation) {
      return true;
    }
    if ((starttime < station.station_end && endtime > station.station_end) ||
        (starttime < station.station_end &&
         !station.station_end.is_not_a_date_time()) ||
        (endtime > station.station_start &&
         starttime < station.station_start) ||
        (starttime > station.station_start &&
         endtime < station.station_start)) {
      return true;
    }
    return false;
  }
  catch (...) {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

bool Engine::stationHasRightType(const Spine::Station &station,
                                 const Settings &settings) {
  try {
    if ((settings.stationtype == "fmi" || settings.stationtype == "opendata" ||
         settings.stationtype == "opendata_minute" ||
         settings.stationtype == "opendata_daily" ||
         settings.stationtype == "daily" || settings.stationtype == "hourly" ||
         settings.stationtype == "monthly" ||
         settings.stationtype == "lammitystarve" ||
         settings.stationtype == "solar" ||
         settings.stationtype == "minute_rad") &&
        station.isFMIStation) {
      return true;
    } else if (settings.stationtype == "foreign") {
      return true;
    } else if (settings.stationtype == "road" && station.isRoadStation) {
      return true;
    } else if ((settings.stationtype == "mareograph" ||
                settings.stationtype == "opendata_mareograph") &&
               station.isMareographStation) {
      return true;
    } else if ((settings.stationtype == "buoy" ||
                settings.stationtype == "opendata_buoy") &&
               station.isBuoyStation) {
      return true;
    } else if (settings.stationtype == "syke" && station.isSYKEStation) {
      return true;
    } else if (settings.stationtype == "MAST") {
      return true;
    }

    return false;
  }
  catch (...) {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::getStations(Spine::Stations &stations, Settings &settings) {
  return itsDatabaseDriver->getStations(stations, settings);
}

Spine::Stations Engine::getStationsByArea(const Settings &settings,
                                          const std::string &areaWkt) {
  try {
    Spine::Stations stations;
    Settings tempSettings = settings;

    try {
      auto stationgroupCodeSet =
          itsEngineParameters->stationtypeConfig.getGroupCodeSetByStationtype(
              tempSettings.stationtype);
      tempSettings.stationgroup_codes.insert(stationgroupCodeSet->begin(),
                                             stationgroupCodeSet->end());
    }
    catch (...) {
      return stations;
    }

    if (areaWkt.empty())
      return stations;

    try {
      auto info = itsEngineParameters->stationInfo.load();
      return itsObservationCache->findStationsInsideArea(tempSettings, areaWkt,
                                                         *info);
    }
    catch (...) {
      Spine::Exception exception(BCP, "Operation failed!", NULL);
      errorLog(exception.what());
      throw exception;
    }

    return stations;
  }
  catch (...) {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::getStationsByBoundingBox(Spine::Stations &stations,
                                      const Settings &settings) {
  itsObservationCache->getStationsByBoundingBox(stations, settings);
}

void Engine::getStationsByRadius(Spine::Stations &stations,
                                 const Settings &settings, double longitude,
                                 double latitude) {
  try {
    // Copy original data atomically so that a reload may simultaneously swap
    auto info = itsEngineParameters->stationInfo.load();

    // std::cout << "NOT USING FAST SEARCH!" << std::endl;

    // Now we can safely use "outdated" data even though the stations may have
    // simultaneously been
    // updated

    for (const Spine::Station &station : info->stations) {
      if (stationHasRightType(station, settings)) {
        double distance = Fmi::Geometry::GeoDistance(
            longitude, latitude, station.longitude_out, station.latitude_out);
        if (distance < settings.maxdistance) {
          stations.push_back(station);
        }
      }
    }
  }
  catch (...) {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

bool Engine::stationIsInBoundingBox(const Spine::Station &station,
                                    std::map<std::string, double> boundingBox) {
  try {
    if (station.latitude_out <= boundingBox["maxy"] &&
        station.latitude_out >= boundingBox["miny"] &&
        station.longitude_out <= boundingBox["maxx"] &&
        station.longitude_out >= boundingBox["minx"]) {
      return true;
    }

    return false;
  }
  catch (...) {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::initializePool(int poolSize) {
  try {
    logMessage(" [Observation Engine] Initializing connection pool...",
               itsEngineParameters->quiet);

    itsDatabaseDriver->initializeConnectionPool();
    itsObservationCache->initializeConnectionPool();

    logMessage(" [Observation Engine] Connection pool ready.",
               itsEngineParameters->quiet);
  }
  catch (...) {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void Engine::initializeCache() {
  try {
    if (itsEngineParameters->locationCacheSize) {
      itsEngineParameters->locationCache.resize(
          boost::numeric_cast<std::size_t>(
              itsEngineParameters->locationCacheSize));
    } else {
      itsEngineParameters->locationCache.resize(1000);
    }

    itsEngineParameters->queryResultBaseCache.resize(
        itsEngineParameters->queryResultBaseCacheSize);
  }
  catch (...) {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

bool Engine::ready() const { return itsReady; }

void Engine::setGeonames(Geonames::Engine *geonames_) {
  try {
    boost::mutex::scoped_lock lock(itsSetGeonamesMutex);

    //   if (itsEngineParameters->geonames == NULL)
    if (itsEngineParameters->databaseDriverParameters->geonames == NULL) {
      //      itsEngineParameters->geonames = geonames_;
      itsEngineParameters->databaseDriverParameters->geonames = geonames_;

      // Connection pool can be initialized only afer geonames is set
      initializePool(itsEngineParameters->poolSize);

      // boost::thread
      // initializeThread(boost::bind(&Engine::Observation::Engine::preloadStations,
      // this));
      if (not itsEngineParameters->disableUpdates) {
        itsActiveThreadCount++;
        cacheFromDatabase();
        itsActiveThreadCount--;
      }
    }
  }
  catch (...) {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

FlashCounts Engine::getFlashCount(const boost::posix_time::ptime &starttime,
                                  const boost::posix_time::ptime &endtime,
                                  const Spine::TaggedLocationList &locations) {
  return itsDatabaseDriver->getFlashCount(starttime, endtime, locations);
}

boost::shared_ptr<std::vector<ObservableProperty> >
Engine::observablePropertyQuery(std::vector<std::string> &parameters,
                                const std::string language) {
  return itsDatabaseDriver->observablePropertyQuery(parameters, language);
}

void Engine::reloadStations() {
  try {
    itsEngineParameters->forceReload = true;
    boost::thread initializeThread(
        boost::bind(&Observation::Engine::preloadStations, this));
  }
  catch (...) {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

boost::shared_ptr<Spine::Table>
Engine::makeQuery(Settings &settings,
                  boost::shared_ptr<Spine::ValueFormatter> &valueFormatter) {
  return itsDatabaseDriver->makeQuery(settings, valueFormatter);
}

ts::TimeSeriesVectorPtr Engine::values(Settings &settings) {
  return itsDatabaseDriver->values(settings);
}

void Engine::makeQuery(QueryBase *qb) { itsDatabaseDriver->makeQuery(qb); }

Spine::Parameter Engine::makeParameter(const std::string &name) const {
  return itsEngineParameters->makeParameter(name);
}

bool Engine::isParameter(const std::string &alias,
                         const std::string &stationType) const {
  return itsEngineParameters->isParameter(alias, stationType);
}

bool Engine::isParameterVariant(const std::string &name) const {
  return itsEngineParameters->isParameterVariant(name);
}

uint64_t Engine::getParameterId(const std::string &alias,
                                const std::string &stationType) const {
  return itsEngineParameters->getParameterId(alias, stationType);
}

std::set<std::string> Engine::getValidStationTypes() const {
  try {
    std::set<std::string> stationTypes;

    for (auto const &mapEntry :
         itsEngineParameters->stationtypeConfig.getGroupCodeSetMap()) {
      stationTypes.insert(mapEntry.first);
    }

    for (auto const &mapEntry :
         itsEngineParameters->stationtypeConfig.getDatabaseTableNameMap()) {
      stationTypes.insert(mapEntry.first);
    }

    for (auto const &mapEntry :
         itsEngineParameters->stationtypeConfig.getUseCommonQueryMethodMap()) {
      stationTypes.insert(mapEntry.first);
    }

    for (auto const &mapEntry :
         itsEngineParameters->stationtypeConfig.getProducerIdSetMap()) {
      stationTypes.insert(mapEntry.first);
    }

    return stationTypes;
  }
  catch (...) {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

/*
 * \brief Read values for given times only.
 */

Spine::TimeSeries::TimeSeriesVectorPtr
Engine::values(Settings &settings,
               const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions) {
  return itsDatabaseDriver->values(settings, timeSeriesOptions);
}

} // namespace Observation
} // namespace Engine
} // namespace SmartMet

// DYNAMIC MODULE CREATION TOOLS

extern "C" void *engine_class_creator(const char *configfile,
                                      void * /* user_data */) {
  return new SmartMet::Engine::Observation::Engine(configfile);
}

extern "C" const char *engine_name() { return "Observation"; }
