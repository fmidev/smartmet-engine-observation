#include "Engine.h"
#include "DisabledEngine.h"
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
      return new SmartMet::Engine::Observation::DisabledEngine();

    return new SmartMet::Engine::Observation::EngineImpl(configfile);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

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
