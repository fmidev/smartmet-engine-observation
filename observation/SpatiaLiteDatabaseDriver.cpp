#include "SpatiaLiteDatabaseDriver.h"
#include "ObservationCache.h"
#include "QueryResult.h"
#include "StationInfo.h"
#include "StationtypeConfig.h"
#include <macgyver/DateTime.h>
#include <boost/make_shared.hpp>
#include <spine/Convenience.h>
#include <spine/Reactor.h>
#include <timeseries/TimeSeriesInclude.h>
#include <atomic>
#include <chrono>
#include <clocale>
#include <numeric>

// #define MYDEBUG 1

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
SpatiaLiteDatabaseDriver::SpatiaLiteDatabaseDriver(const std::string &name,
                                                   const EngineParametersPtr &p,
                                                   Spine::ConfigBase &cfg)
    : DatabaseDriverBase(name), itsParameters(name, p)
{
  if (setlocale(LC_NUMERIC, "en_US.utf8") == nullptr)
    throw Fmi::Exception(BCP, "Spatialite database driver failed to set locale to en_US.utf8");

  readConfig(cfg);
}

void SpatiaLiteDatabaseDriver::init(Engine *obsengine)
{
  try
  {
    itsDatabaseStations.reset(new DatabaseStations(itsParameters.params, obsengine->getGeonames()));

    auto cacheAdmin = std::make_shared<ObservationCacheAdminSpatiaLite>(
        itsParameters, obsengine->getGeonames(), itsConnectionsOK, false);
    if (!Spine::Reactor::isShuttingDown())
    {
      itsObservationCacheAdminSpatiaLite.store(cacheAdmin);
      cacheAdmin->init();
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void SpatiaLiteDatabaseDriver::makeQuery(QueryBase * /* qb */)
{
  try
  {
    // Not implemeted
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

TS::TimeSeriesVectorPtr SpatiaLiteDatabaseDriver::values(Settings &settings)
{
  if (Spine::Reactor::isShuttingDown())
    return nullptr;

  parameterSanityCheck(
      settings.stationtype, settings.parameters, *itsParameters.params->parameterMap);
  updateProducers(itsParameters.params, settings);

  settings.useCommonQueryMethod =
      itsParameters.params->stationtypeConfig.getUseCommonQueryMethod(settings.stationtype);

  if (!settings.dataFilter.exist("data_quality"))
    settings.dataFilter.setDataFilter(
        "data_quality", itsParameters.params->dataQualityFilters.at(settings.stationtype));

  // This driver fetched data only from cache
  try
  {
    if (settings.useDataCache)
    {
      auto cache = resolveCache(settings.stationtype, itsParameters.params);

      if (cache && cache->dataAvailableInCache(settings))
      {
        return cache->valuesFromCache(settings);
      }
    }

    return std::make_shared<TS::TimeSeriesVector>();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reading data from cache failed!");
  }
}

/*
 * \brief Read values for given times only.
 */

TS::TimeSeriesVectorPtr SpatiaLiteDatabaseDriver::values(
    Settings &settings, const TS::TimeSeriesGeneratorOptions &timeSeriesOptions)
{
  if (Spine::Reactor::isShuttingDown())
    return nullptr;

  parameterSanityCheck(
      settings.stationtype, settings.parameters, *itsParameters.params->parameterMap);
  updateProducers(itsParameters.params, settings);

  settings.useCommonQueryMethod =
      itsParameters.params->stationtypeConfig.getUseCommonQueryMethod(settings.stationtype);

  if (!settings.dataFilter.exist("data_quality"))
    settings.dataFilter.setDataFilter(
        "data_quality", itsParameters.params->dataQualityFilters.at(settings.stationtype));

  // This driver fetched data only from cache
  try
  {
    if (settings.useDataCache)
    {
      auto cache = resolveCache(settings.stationtype, itsParameters.params);

      if (cache && cache->dataAvailableInCache(settings))
      {
        return cache->valuesFromCache(settings, timeSeriesOptions);
      }
    }

    return std::make_shared<TS::TimeSeriesVector>();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reading data from cache failed!");
  }
}

FlashCounts SpatiaLiteDatabaseDriver::getFlashCount(
    const Fmi::DateTime &starttime,
    const Fmi::DateTime &endtime,
    const Spine::TaggedLocationList &locations) const
{
  try
  {
    Settings settings;
    settings.stationtype = FLASH_PRODUCER;

    auto cache = resolveCache(settings.stationtype, itsParameters.params);
    if (cache && cache->flashIntervalIsCached(starttime, endtime))
    {
      return cache->getFlashCount(starttime, endtime, locations);
    }

    FlashCounts ret;
    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting flash count failed!");
  }
}

void SpatiaLiteDatabaseDriver::getMovingStationsByArea(Spine::Stations &stations,
                                                       const Settings &settings,
                                                       const std::string &wkt) const
{
  try
  {
    auto cache = resolveCache(settings.stationtype, itsParameters.params);
    if (cache)
      cache->getMovingStations(stations, settings, wkt);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reading data from cache failed!");
  }
}

std::shared_ptr<std::vector<ObservableProperty>> SpatiaLiteDatabaseDriver::observablePropertyQuery(
    std::vector<std::string> & /* parameters */, const std::string & /* language */)
{
  try
  {
    // Not implemented
    return {};
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void SpatiaLiteDatabaseDriver::readConfig(Spine::ConfigBase &cfg)
{
  try
  {
    DatabaseDriverBase::readConfig(cfg, itsParameters);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reading SpatiaLite configuration failed!");
  }
}

std::string SpatiaLiteDatabaseDriver::id() const
{
  return "spatialite";
}

void SpatiaLiteDatabaseDriver::shutdown() {}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
