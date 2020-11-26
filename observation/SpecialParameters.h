#pragma once

#include <spine/Station.h>
#include <spine/TimeSeries.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{

Spine::TimeSeries::TimedValue getSpecialParameterValue(
    const Spine::Station &station,
    const std::string &stationType,
    const std::string &parameter,
    const boost::local_time::local_date_time &obstime,
    const boost::local_time::local_date_time &origintime,
	const std::string& timeZone);

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
