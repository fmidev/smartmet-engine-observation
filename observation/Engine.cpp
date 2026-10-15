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
#include <algorithm>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
Engine::Engine() = default;

std::vector<std::string> Engine::getDerivedParameters(const std::string &stationtype) const
{
  try
  {
    struct DerivedParameter
    {
      std::vector<std::string> names;
      std::vector<std::string> sources;
      bool qc_supported;  // calculated also for the road and foreign station types
    };

    static const std::vector<DerivedParameter> derived_parameters = {
        {{"windcompass8", "windcompass16", "windcompass32"}, {"winddirection"}, true},
        {{"feelslike"}, {"windspeedms", "relativehumidity", "temperature"}, false},
        {{"smartsymbol", "smartsymboltext"}, {"wawa", "totalcloudcover", "temperature"}, false},
        {{"cloudceiling", "cloudceilingft", "cloudceilinghft"},
         {"cla1_pt1m_acc",
          "cla2_pt1m_acc",
          "cla3_pt1m_acc",
          "cla4_pt1m_acc",
          "cla5_pt1m_acc",
          "clhb1_pt1m_instant",
          "clhb2_pt1m_instant",
          "clhb3_pt1m_instant",
          "clhb4_pt1m_instant",
          "clh5_pt1m_instant"},
         false}};

    // Producer 'fmi' is deprecated, the database queries use 'observations_fmi' instead
    const std::string dbstationtype = (stationtype == "fmi" ? "observations_fmi" : stationtype);
    const bool is_qc_table = (dbstationtype == "foreign" || dbstationtype == "road");

    std::vector<std::string> ret;
    for (const auto &derived : derived_parameters)
    {
      if (is_qc_table && !derived.qc_supported)
        continue;

      const bool available = std::all_of(
          derived.sources.begin(),
          derived.sources.end(),
          [&](const std::string &source)
          { return !getParameterIdAsString(source, dbstationtype).empty(); });
      if (available)
        ret.insert(ret.end(), derived.names.begin(), derived.names.end());
    }

    // roadww is calculated only for road weather stations, from the precipitation
    // intensity (prclst) and form (prst2)
    if (dbstationtype == "road" && !getParameterIdAsString("prclst", dbstationtype).empty() &&
        !getParameterIdAsString("prst2", dbstationtype).empty())
      ret.emplace_back("roadww");

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!").addParameter("Station type", stationtype);
  }
}

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
