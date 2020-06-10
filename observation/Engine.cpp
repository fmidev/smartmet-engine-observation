#include "Engine.h"
#include "DBRegistry.h"
#include "DatabaseDriverFactory.h"
#include "ObservationCacheFactory.h"
#include <boost/algorithm/string.hpp>
#include <boost/make_shared.hpp>
#include <macgyver/Geometry.h>
#include <spine/Convenience.h>
#include <spine/ParameterTools.h>
#include <spine/Reactor.h>
#include <spine/TimeSeriesOutput.h>

namespace ts = SmartMet::Spine::TimeSeries;
namespace ba = boost::algorithm;

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
    boost::shared_ptr<StationInfo> stationinfo = boost::make_shared<StationInfo>();
    if (boost::filesystem::exists(path) && !boost::filesystem::is_empty(path))
    {
      stationinfo->unserialize(itsEngineParameters->serializedStationsFile);

      boost::atomic_store(&itsEngineParameters->stationInfo, stationinfo);
      logMessage("[Observation Engine] Unserialized stations successfully from " + path.string(),
                 itsEngineParameters->quiet);
    }
    else
    {
      boost::atomic_store(&itsEngineParameters->stationInfo, stationinfo);
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

void Engine::getStationsByArea(Spine::Stations &stations,
                               const std::string &stationtype,
                               const boost::posix_time::ptime &starttime,
                               const boost::posix_time::ptime &endtime,
                               const std::string &areaWkt)
{
  return itsDatabaseDriver->getStationsByArea(stations, stationtype, starttime, endtime, areaWkt);
}

void Engine::getStationsByBoundingBox(Spine::Stations &stations, const Settings &settings)
{
  return itsDatabaseDriver->getStationsByBoundingBox(stations, settings);
}

void Engine::initializeCache()
{
  try
  {
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

boost::shared_ptr<std::vector<ObservableProperty>> Engine::observablePropertyQuery(
    std::vector<std::string> &parameters, const std::string language)
{
  return itsDatabaseDriver->observablePropertyQuery(parameters, language);
}

ts::TimeSeriesVectorPtr Engine::values(Settings &settings)
{
  // Drop unknown parameters from parameter list and
  // store their indexes
  std::vector<unsigned int> unknownParameterIndexes;
  if (settings.debug_options & Settings::DUMP_SETTINGS) {
    std::cout << "SmartMet::Engine::Observation::Settings:\n" << settings << std::endl;
  }
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

std::string Engine::getParameterIdAsString(const std::string &alias,
                                           const std::string &stationType) const
{
  return itsEngineParameters->getParameterIdAsString(alias, stationType);
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
  if (settings.debug_options & Settings::DUMP_SETTINGS) {
    std::cout << "SmartMet::Engine::Observation::Settings:\n" << settings << std::endl;
    std::cout << "SmartMet::Spine::TimeSeriesGeneratorOptions:\n" << timeSeriesOptions << std::endl;
  }
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

// Translates WMO, RWID,LPNN to FMISID
Spine::TaggedFMISIDList Engine::translateToFMISID(const boost::posix_time::ptime &starttime,
                                                  const boost::posix_time::ptime &endtime,
                                                  const std::string &stationtype,
                                                  const StationSettings &stationSettings) const
{
  return itsDatabaseDriver->translateToFMISID(starttime, endtime, stationtype, stationSettings);
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
    if (!isParameter(pname, settings.stationtype) && !Spine::is_special_parameter(pname))
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
