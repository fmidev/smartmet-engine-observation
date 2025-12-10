#include "Engine.h"
#include "DBRegistry.h"
#include "DatabaseDriverFactory.h"
#include "EngineImpl.h"
#include "ObservationCacheFactory.h"
#include "SpecialParameters.h"
#include <boost/algorithm/string.hpp>
#include <boost/make_shared.hpp>
#include <macgyver/AnsiEscapeCodes.h>
#include <macgyver/Geometry.h>
#include <macgyver/TypeName.h>
#include <spine/Convenience.h>
#include <spine/Reactor.h>
#include <timeseries/ParameterTools.h>
#include <timeseries/TimeSeriesInclude.h>

#ifdef DEBUG_ENGINE_DISABLING
#define DISABLE_STACKTRACE
#else
#define DISABLE_STACKTRACE .disableStackTrace()
#endif

#define DISABLED_MSG ": engine is disabled"
#define REPORT_DISABLED throw Fmi::Exception(BCP, METHOD_NAME + DISABLED_MSG) DISABLE_STACKTRACE

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
Engine::Engine() = default;

Engine *Engine::create(const std::string &configfile)
{
  try
  {
    const bool disabled = [&configfile]()
    {
      const char *name = "SmartMet::Engine::Observation::Engine::create";
      if (configfile.empty())
      {
        std::cout << Spine::log_time_str() << ' ' << ANSI_FG_RED << name
                  << ": configuration file not specified or its name is empty string: "
                  << "engine disabled." << ANSI_FG_DEFAULT << '\n';
        return true;
      }

      SmartMet::Spine::ConfigBase cfg(configfile);
      const bool result = cfg.get_optional_config_param<bool>("disabled", false);
      if (result)
        std::cout << Spine::log_time_str() << ' ' << ANSI_FG_RED << name << ": engine disabled"
                  << ANSI_FG_DEFAULT << '\n';
      return result;
    }();

    if (disabled)
      return new SmartMet::Engine::Observation::Engine();

    return new SmartMet::Engine::Observation::EngineImpl(configfile);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

TS::TimeSeriesVectorPtr Engine::values(Settings &settings)
{
  (void)settings;
  REPORT_DISABLED;
}

TS::TimeSeriesVectorPtr Engine::values(Settings &settings,
                                       const TS::TimeSeriesGeneratorOptions &timeSeriesOptions)
{
  (void)settings;
  (void)timeSeriesOptions;
  REPORT_DISABLED;
}

void Engine::makeQuery(QueryBase *qb)
{
  (void)qb;
  REPORT_DISABLED;
}

FlashCounts Engine::getFlashCount(const Fmi::DateTime & /*starttime */,
                                  const Fmi::DateTime & /* endtime */,
                                  const Spine::TaggedLocationList & /*locations */)
{
  REPORT_DISABLED;
}

std::shared_ptr<std::vector<ObservableProperty>> Engine::observablePropertyQuery(
    std::vector<std::string> &parameters, const std::string &language)
{
  (void)parameters;
  (void)language;
  REPORT_DISABLED;
}

bool Engine::ready() const
{
  return true;
}

Geonames::Engine *Engine::getGeonames() const
{
  REPORT_DISABLED;
}

std::shared_ptr<DBRegistry> Engine::dbRegistry() const
{
  REPORT_DISABLED;
}

void Engine::reloadStations() {}

void Engine::getStations(Spine::Stations & /*stations*/, const Settings & /*settings*/)
{
  REPORT_DISABLED;
}

void Engine::getStationsByArea(Spine::Stations & /*stations*/,
                               const Settings & /*settings*/,
                               const std::string & /*areaWkt*/)
{
  REPORT_DISABLED;
}

void Engine::getStationsByBoundingBox(Spine::Stations & /*stations*/, const Settings & /*settings*/)
{
  REPORT_DISABLED;
}

bool Engine::isParameter(const std::string & /*alias*/, const std::string & /*stationType*/) const
{
  REPORT_DISABLED;
}

bool Engine::isParameterVariant(const std::string & /*name*/) const
{
  REPORT_DISABLED;
}

uint64_t Engine::getParameterId(const std::string & /*alias*/,
                                const std::string & /*stationType*/) const
{
  REPORT_DISABLED;
}

std::string Engine::getParameterIdAsString(const std::string & /*alias*/,
                                           const std::string & /*stationType*/) const
{
  REPORT_DISABLED;
}

std::set<std::string> Engine::getValidStationTypes() const
{
  REPORT_DISABLED;
}

ContentTable Engine::getProducerInfo(const std::optional<std::string> & /*producer*/) const
{
  REPORT_DISABLED;
}

ContentTable Engine::getParameterInfo(const std::optional<std::string> & /*producer*/) const
{
  REPORT_DISABLED;
}

ContentTable Engine::getStationInfo(const StationOptions & /*options*/) const
{
  REPORT_DISABLED;
}

MetaData Engine::metaData(const std::string & /*producer*/, const Settings & /*settings*/) const
{
  REPORT_DISABLED;
}

Spine::TaggedFMISIDList Engine::translateToFMISID(const Settings & /*settings*/,
                                                  const StationSettings & /*stationSettings*/) const
{
  REPORT_DISABLED;
}

const ProducerMeasurandInfo &Engine::getMeasurandInfo() const
{
  REPORT_DISABLED;
}

Fmi::DateTime Engine::getLatestDataUpdateTime(const std::string &producer,
                                              const Fmi::DateTime &from) const
{
  REPORT_DISABLED;
}

void Engine::init() {}

void Engine::shutdown() {}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

// DYNAMIC MODULE CREATION TOOLS

extern "C" void *engine_class_creator(const char *configfile, void * /*user_data*/)
{
  return SmartMet::Engine::Observation::Engine::create(configfile);
}

extern "C" const char *engine_name()
{
  return "Observation";
}
