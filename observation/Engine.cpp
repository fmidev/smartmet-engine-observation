#include "Engine.h"
#include "EngineImpl.h"
#include "DBRegistry.h"
#include "DatabaseDriverFactory.h"
#include "ObservationCacheFactory.h"
#include "SpecialParameters.h"
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/make_shared.hpp>
#include <macgyver/Geometry.h>
#include <macgyver/TypeName.h>
#include <spine/Convenience.h>
#include <spine/Reactor.h>
#include <timeseries/TimeSeriesInclude.h>
#include <timeseries/ParameterTools.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{

#define DISABLED_MSG " is not available because of  observation engine is not enabled"

Engine::Engine()
{
}

Engine* Engine::create(const std::string& configfile)
{
  try {
      SmartMet::Spine::ConfigBase cfg(configfile);
      bool disabled = cfg.get_optional_config_param<bool>("disabled", false);
      if (disabled) {
          return new SmartMet::Engine::Observation::Engine();
      } else {
          return new SmartMet::Engine::Observation::EngineImpl(configfile);
      }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

TS::TimeSeriesVectorPtr Engine::values(Settings &settings)
{
    (void)settings;
    throw Fmi::Exception(BCP, METHOD_NAME + DISABLED_MSG);
}

TS::TimeSeriesVectorPtr Engine::values(
      Settings &settings, const TS::TimeSeriesGeneratorOptions &timeSeriesOptions)
{
    (void)settings;
    (void)timeSeriesOptions;
    throw Fmi::Exception(BCP, METHOD_NAME + DISABLED_MSG);
}

void Engine::makeQuery(QueryBase *qb)
{
    (void)qb;
    throw Fmi::Exception(BCP, METHOD_NAME + DISABLED_MSG);
}

FlashCounts
Engine::getFlashCount(const boost::posix_time::ptime &starttime,
                            const boost::posix_time::ptime &endtime,
                            const Spine::TaggedLocationList &locations)
{
    (void) starttime;
    (void) endtime;
    throw Fmi::Exception(BCP, METHOD_NAME + DISABLED_MSG);
}

std::shared_ptr<std::vector<ObservableProperty>>
Engine::observablePropertyQuery(std::vector<std::string> &parameters, const std::string language)
{
    (void)parameters;
    (void)language;
    throw Fmi::Exception(BCP, METHOD_NAME + DISABLED_MSG);
}

bool
Engine::ready() const
{
    return true;
}

Geonames::Engine *
Engine::getGeonames() const
{
    throw Fmi::Exception(BCP, METHOD_NAME + DISABLED_MSG);
}

const std::shared_ptr<DBRegistry>
Engine::dbRegistry() const
{
    throw Fmi::Exception(BCP, METHOD_NAME + DISABLED_MSG);
}

void
Engine::reloadStations()
{
}

void
Engine::getStations(Spine::Stations &stations, Settings &settings)
{
    (void)settings;
    throw Fmi::Exception(BCP, METHOD_NAME + DISABLED_MSG);
}

void
Engine::getStationsByArea(Spine::Stations &stations,
                         const std::string &stationtype,
                         const boost::posix_time::ptime &starttime,
                         const boost::posix_time::ptime &endtime,
                         const std::string &areaWkt)
{
    (void)stations;
    (void)stationtype;
    (void)starttime;
    (void)endtime;
    (void)areaWkt;
    throw Fmi::Exception(BCP, METHOD_NAME + DISABLED_MSG);
}

void
Engine::getStationsByBoundingBox(Spine::Stations &stations, const Settings &settings)
{
    (void)stations;
    (void)settings;
    throw Fmi::Exception(BCP, METHOD_NAME + DISABLED_MSG);
}

bool
Engine::isParameter(const std::string &alias, const std::string &stationType) const
{
    (void)alias;
    (void)stationType;
    throw Fmi::Exception(BCP, METHOD_NAME + DISABLED_MSG);
}

bool
Engine::isParameterVariant(const std::string &name) const
{
    (void)name;
    throw Fmi::Exception(BCP, METHOD_NAME + DISABLED_MSG);
}

uint64_t
Engine::getParameterId(const std::string &alias,
                       const std::string &stationType) const
{
    (void)alias;
    (void)stationType;
    throw Fmi::Exception(BCP, METHOD_NAME + DISABLED_MSG);
}

std::string
Engine::getParameterIdAsString(const std::string &alias,
                               const std::string &stationType) const
{
    (void)alias;
    (void)stationType;
    throw Fmi::Exception(BCP, METHOD_NAME + DISABLED_MSG);
}

std::set<std::string>
Engine::getValidStationTypes() const
{
    throw Fmi::Exception(BCP, METHOD_NAME + DISABLED_MSG);
}

ContentTable
Engine::getProducerInfo(boost::optional<std::string> producer) const
{
    (void)producer;
    throw Fmi::Exception(BCP, METHOD_NAME + DISABLED_MSG);
}

ContentTable
Engine::getParameterInfo(boost::optional<std::string> producer) const
{
    (void)producer;
    throw Fmi::Exception(BCP, METHOD_NAME + DISABLED_MSG);
}

ContentTable
Engine::getStationInfo(const StationOptions &options) const
{
    (void)options;
    throw Fmi::Exception(BCP, METHOD_NAME + DISABLED_MSG);
}

MetaData
Engine::metaData(const std::string &producer) const
{
    (void)producer;
    throw Fmi::Exception(BCP, METHOD_NAME + DISABLED_MSG);
}

Spine::TaggedFMISIDList
Engine::translateToFMISID(const boost::posix_time::ptime &starttime,
                          const boost::posix_time::ptime &endtime,
                          const std::string &stationtype,
                          const StationSettings &stationSettings) const
{
    (void)starttime;
    (void)endtime;
    (void)stationtype;
    (void)stationSettings;
    throw Fmi::Exception(BCP, METHOD_NAME + DISABLED_MSG);
}

void
Engine::init()
{
}

void
Engine::shutdown()
{
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet


// DYNAMIC MODULE CREATION TOOLS

extern "C" void *engine_class_creator(const char *configfile, void * /* user_data */)
{
  return SmartMet::Engine::Observation::Engine::create(configfile);
}

extern "C" const char *engine_name()
{
  return "Observation";
}

