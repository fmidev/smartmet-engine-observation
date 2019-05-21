#include "Engine.h"
#include "DBRegistry.h"
#include "DatabaseDriverFactory.h"
#include "ObservationCacheFactory.h"
#include <boost/make_shared.hpp>
#include <macgyver/Geometry.h>
#include <spine/Convenience.h>
#include <spine/Reactor.h>
#include <spine/TimeSeriesOutput.h>

namespace ts = SmartMet::Spine::TimeSeries;

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
Engine::Engine(const std::string &configfile)
    : itsConfigFile(configfile), itsDatabaseRegistry(new DBRegistry())
{
}

void Engine::init()
{
  try
  {
    Spine::ConfigBase cfg(itsConfigFile);

    itsEngineParameters.reset(new EngineParameters(cfg));

    itsDatabaseRegistry->loadConfigurations(itsEngineParameters->dbRegistryFolderPath);

    // Initialize the caches
    initializeCache();

    // Read preloaded stations from disk if available
    unserializeStations();

    itsEngineParameters->observationCache.reset(
        ObservationCacheFactory::create(itsEngineParameters, cfg));
#ifdef TODO_CAUSES_SEGFAULT_AT_EXIT
    itsDatabaseDriver.reset(DatabaseDriverFactory::create(itsEngineParameters, cfg));
#else
    itsDatabaseDriver = DatabaseDriverFactory::create(itsEngineParameters, cfg);
#endif
    if (itsDatabaseDriver != nullptr)
    {
      logMessage("[Observation Engine] Database driver '" + itsDatabaseDriver->id() + "' created",
                 itsEngineParameters->quiet);
      itsDatabaseDriver->init(this);
    }

    itsSpecialParameters.insert("name");
    itsSpecialParameters.insert("geoid");
    itsSpecialParameters.insert("stationname");
    itsSpecialParameters.insert("distance");
    itsSpecialParameters.insert("stationary");
    itsSpecialParameters.insert("stationlongitude");
    itsSpecialParameters.insert("stationlon");
    itsSpecialParameters.insert("stationlatitude");
    itsSpecialParameters.insert("stationlat");
    itsSpecialParameters.insert("longitude");
    itsSpecialParameters.insert("lon");
    itsSpecialParameters.insert("latitude");
    itsSpecialParameters.insert("lat");
    itsSpecialParameters.insert("elevation");
    itsSpecialParameters.insert("wmo");
    itsSpecialParameters.insert("lpnn");
    itsSpecialParameters.insert("fmisid");
    itsSpecialParameters.insert("rwsid");
    itsSpecialParameters.insert("modtime");
    itsSpecialParameters.insert("model");
    itsSpecialParameters.insert("origintime");
    itsSpecialParameters.insert("timestring");
    itsSpecialParameters.insert("tz");
    itsSpecialParameters.insert("place");
    itsSpecialParameters.insert("region");
    itsSpecialParameters.insert("iso2");
    itsSpecialParameters.insert("direction");
    itsSpecialParameters.insert("country");
    itsSpecialParameters.insert("time");
    itsSpecialParameters.insert("sensor_no");
    itsSpecialParameters.insert("level");
    itsSpecialParameters.insert("xmltime");
    itsSpecialParameters.insert("localtime");
    itsSpecialParameters.insert("utctime");
    itsSpecialParameters.insert("epochtime");
    itsSpecialParameters.insert("windcompass8");
    itsSpecialParameters.insert("windcompass16");
    itsSpecialParameters.insert("windcompass32");
    itsSpecialParameters.insert("feelslike");
    itsSpecialParameters.insert("smartsymbol");
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Observation-engine initialization failed");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Shutdown the engine
 */
// ----------------------------------------------------------------------

void Engine::shutdown()
{
  std::cout << "  -- Shutdown requested (Observation)" << std::endl;
  itsDatabaseDriver->shutdown();
}

void Engine::unserializeStations()
{
  boost::filesystem::path path = itsEngineParameters->serializedStationsFile;

  try
  {
    if (boost::filesystem::exists(path) && !boost::filesystem::is_empty(path))
    {
      boost::shared_ptr<StationInfo> stationinfo = boost::make_shared<StationInfo>();
      stationinfo->unserialize(itsEngineParameters->serializedStationsFile);

      //  This is atomic
      itsEngineParameters->stationInfo = stationinfo;
      logMessage("[Observation Engine] Unserialized stations successfully from " + path.string(),
                 itsEngineParameters->quiet);
    }
    else
    {
      logMessage("[Observation Engine] No serialized station file found from " + path.string(),
                 itsEngineParameters->quiet);
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Failed to unserialize station info!")
        .addParameter("station file", path.string());
  }
}

bool Engine::stationHasRightType(const Spine::Station &station, const Settings &settings)
{
  try
  {
    if ((settings.stationtype == "fmi" || settings.stationtype == "opendata" ||
         settings.stationtype == "opendata_minute" || settings.stationtype == "opendata_daily" ||
         settings.stationtype == "daily" || settings.stationtype == "hourly" ||
         settings.stationtype == "monthly" || settings.stationtype == "lammitystarve" ||
         settings.stationtype == "solar" || settings.stationtype == "minute_rad") &&
        station.isFMIStation)
    {
      return true;
    }
    else if (settings.stationtype == "foreign")
    {
      return true;
    }
    else if (settings.stationtype == "road" && station.isRoadStation)
    {
      return true;
    }
    else if ((settings.stationtype == "mareograph" ||
              settings.stationtype == "opendata_mareograph") &&
             station.isMareographStation)
    {
      return true;
    }
    else if ((settings.stationtype == "buoy" || settings.stationtype == "opendata_buoy") &&
             station.isBuoyStation)
    {
      return true;
    }
    else if (settings.stationtype == "syke" && station.isSYKEStation)
    {
      return true;
    }
    else if (settings.stationtype == "MAST")
    {
      return true;
    }

    return false;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

void Engine::getStations(Spine::Stations &stations, Settings &settings)
{
  return itsDatabaseDriver->getStations(stations, settings);
}

Spine::Stations Engine::getStationsByArea(const Settings &settings, const std::string &areaWkt)
{
  try
  {
    Spine::Stations stations;
    Settings tempSettings = settings;

    try
    {
      auto stationgroupCodeSet =
          itsEngineParameters->stationtypeConfig.getGroupCodeSetByStationtype(
              tempSettings.stationtype);
      tempSettings.stationgroup_codes.insert(stationgroupCodeSet->begin(),
                                             stationgroupCodeSet->end());
    }
    catch (...)
    {
      return stations;
    }

    if (areaWkt.empty())
      return stations;

    try
    {
      auto info = boost::atomic_load(&itsEngineParameters->stationInfo);
      return itsEngineParameters->observationCache->findStationsInsideArea(
          tempSettings, areaWkt, *info);
    }
    catch (...)
    {
      throw Spine::Exception::Trace(BCP, "Operation failed!");
    }

    return stations;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

void Engine::getStationsByBoundingBox(Spine::Stations &stations, const Settings &settings)
{
  itsEngineParameters->observationCache->getStationsByBoundingBox(stations, settings);
}

void Engine::getStationsByRadius(Spine::Stations &stations,
                                 const Settings &settings,
                                 double longitude,
                                 double latitude)
{
  try
  {
    // Copy original data atomically so that a reload may simultaneously swap
    auto info = boost::atomic_load(&itsEngineParameters->stationInfo);

    // std::cout << "NOT USING FAST SEARCH!" << std::endl;

    // Now we can safely use "outdated" data even though the stations may have
    // simultaneously been
    // updated

    for (const Spine::Station &station : info->stations)
    {
      if (stationHasRightType(station, settings))
      {
        double distance = Fmi::Geometry::GeoDistance(
            longitude, latitude, station.longitude_out, station.latitude_out);
        if (distance < settings.maxdistance)
        {
          stations.push_back(station);
        }
      }
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

void Engine::initializeCache()
{
  try
  {
    if (itsEngineParameters->locationCacheSize)
    {
      itsEngineParameters->locationCache.resize(
          boost::numeric_cast<std::size_t>(itsEngineParameters->locationCacheSize));
    }
    else
    {
      itsEngineParameters->locationCache.resize(1000);
    }

    itsEngineParameters->queryResultBaseCache.resize(itsEngineParameters->queryResultBaseCacheSize);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

bool Engine::ready() const
{
  std::cout << "Warning: obsengine::ready called" << std::endl;
  return true;
}

Geonames::Engine *Engine::getGeonames() const
{
  // this will wait until the engine is ready
  auto *engine = itsReactor->getSingleton("Geonames", nullptr);
  return reinterpret_cast<Geonames::Engine *>(engine);
}

FlashCounts Engine::getFlashCount(const boost::posix_time::ptime &starttime,
                                  const boost::posix_time::ptime &endtime,
                                  const Spine::TaggedLocationList &locations)
{
  return itsDatabaseDriver->getFlashCount(starttime, endtime, locations);
}

boost::shared_ptr<std::vector<ObservableProperty> > Engine::observablePropertyQuery(
    std::vector<std::string> &parameters, const std::string language)
{
  return itsDatabaseDriver->observablePropertyQuery(parameters, language);
}

boost::shared_ptr<Spine::Table> Engine::makeQuery(
    Settings &settings, boost::shared_ptr<Spine::ValueFormatter> &valueFormatter)
{
  return itsDatabaseDriver->makeQuery(settings, valueFormatter);
}

ts::TimeSeriesVectorPtr Engine::values(Settings &settings)
{
  // Drop unknown parameters from parameter list and
  // store their indexes
  std::vector<unsigned int> unknownParameterIndexes;
  Settings querySettings = beforeQuery(settings, unknownParameterIndexes);

  ts::TimeSeriesVectorPtr ret = itsDatabaseDriver->values(querySettings);

  // Insert missing values for unknown parameters
  afterQuery(ret, unknownParameterIndexes);

  return ret;
}

void Engine::makeQuery(QueryBase *qb)
{
  itsDatabaseDriver->makeQuery(qb);
}

Spine::Parameter Engine::makeParameter(const std::string &name) const
{
  return itsEngineParameters->makeParameter(name);
}

bool Engine::isParameter(const std::string &alias, const std::string &stationType) const
{
  return itsEngineParameters->isParameter(alias, stationType);
}

bool Engine::isParameterVariant(const std::string &name) const
{
  return itsEngineParameters->isParameterVariant(name);
}

uint64_t Engine::getParameterId(const std::string &alias, const std::string &stationType) const
{
  return itsEngineParameters->getParameterId(alias, stationType);
}

std::set<std::string> Engine::getValidStationTypes() const
{
  try
  {
    std::set<std::string> stationTypes;

    for (auto const &mapEntry : itsEngineParameters->stationtypeConfig.getGroupCodeSetMap())
    {
      stationTypes.insert(mapEntry.first);
    }

    for (auto const &mapEntry : itsEngineParameters->stationtypeConfig.getDatabaseTableNameMap())
    {
      stationTypes.insert(mapEntry.first);
    }

    for (auto const &mapEntry : itsEngineParameters->stationtypeConfig.getUseCommonQueryMethodMap())
    {
      stationTypes.insert(mapEntry.first);
    }

    for (auto const &mapEntry : itsEngineParameters->stationtypeConfig.getProducerIdSetMap())
    {
      stationTypes.insert(mapEntry.first);
    }

    for (auto const &mapEntry : itsEngineParameters->externalAndMobileProducerConfig)
    {
      stationTypes.insert(mapEntry.first);
    }

    return stationTypes;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

/*
 * \brief Read values for given times only.
 */

Spine::TimeSeries::TimeSeriesVectorPtr Engine::values(
    Settings &settings, const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions)
{
  // Drop unknown parameters from parameter list and
  // store their indexes
  std::vector<unsigned int> unknownParameterIndexes;
  Settings querySettings = beforeQuery(settings, unknownParameterIndexes);

  Spine::TimeSeries::TimeSeriesVectorPtr ret =
      itsDatabaseDriver->values(querySettings, timeSeriesOptions);

  // Insert missing values for unknown parameters
  afterQuery(ret, unknownParameterIndexes);

  return ret;
}

MetaData Engine::metaData(const std::string &producer) const
{
  return itsDatabaseDriver->metaData(producer);
}

Settings Engine::beforeQuery(const Settings &settings,
                             std::vector<unsigned int> &unknownParameterIndexes) const
{
  // Copy original settings
  Settings ret = settings;
  // Clear parameter list
  ret.parameters.clear();
  // Add known parameters back to list and store indexes of unknown parameters
  for (unsigned int i = 0; i < settings.parameters.size(); i++)
  {
    const auto &p = settings.parameters.at(i);
    std::string pname = Fmi::ascii_tolower_copy(p.name());
    if (!isParameter(pname, settings.stationtype) &&
        itsSpecialParameters.find(pname) == itsSpecialParameters.end())
    {
      unknownParameterIndexes.push_back(i);
      continue;
    }
    ret.parameters.push_back(p);
  }

  return ret;
}

void Engine::afterQuery(Spine::TimeSeries::TimeSeriesVectorPtr tsvPtr,
                        const std::vector<unsigned int> &unknownParameterIndexes) const
{
  if (tsvPtr->size() > 0 && unknownParameterIndexes.size() > 0)
  {
    // Take copy of the first time series
    Spine::TimeSeries::TimeSeries ts = tsvPtr->at(0);
    // Set values in all timestesps to Spine::TimeSeries::None()
    for (auto &timedvalue : ts)
      timedvalue.value = Spine::TimeSeries::None();
    // Insert the nullified times series to time series vector
    for (auto index : unknownParameterIndexes)
      tsvPtr->insert(tsvPtr->begin() + index, ts);
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

// DYNAMIC MODULE CREATION TOOLS

extern "C" void *engine_class_creator(const char *configfile, void * /* user_data */)
{
  return new SmartMet::Engine::Observation::Engine(configfile);
}

extern "C" const char *engine_name()
{
  return "Observation";
}
