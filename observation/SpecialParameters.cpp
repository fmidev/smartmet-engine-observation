#include "SpecialParameters.h"
#include "SpecialParameterHandlerMap.h"
#include <macgyver/Exception.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{

// FIXME: put SpecialParameterHandlerMap into engine object and add required extra initialization (locale jne)

SpecialParameterHandlerMap special_parameter_handler_map;

Spine::TimeSeries::TimedValue getSpecialParameterValue(
    const Spine::Station &station,
    const std::string &stationType,
    const std::string &parameter,
    const boost::local_time::local_date_time &obstime,
    const boost::local_time::local_date_time &origintime,
    const std::string &timeZone)
{
  try
  {
      const SpecialParameterHandlerMap::Args args(station, stationType, obstime, origintime, timeZone);
      Spine::TimeSeries::Value value = special_parameter_handler_map(parameter, args);
      return Spine::TimeSeries::TimedValue(obstime, value);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
