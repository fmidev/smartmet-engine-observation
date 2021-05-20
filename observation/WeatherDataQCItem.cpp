#include "WeatherDataQCItem.h"
#include <macgyver/Exception.h>
#include <macgyver/Hash.h>
#include <macgyver/StringConversion.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
std::size_t WeatherDataQCItem::hash_value() const
{
  try
  {
    std::size_t hash = Fmi::hash_value(fmisid);
    Fmi::hash_combine(hash, Fmi::hash_value(obstime));
    Fmi::hash_combine(hash, Fmi::hash_value(parameter));
    Fmi::hash_combine(hash, Fmi::hash_value(sensor_no));
    Fmi::hash_combine(hash, Fmi::hash_value(value));
    Fmi::hash_combine(hash, Fmi::hash_value(flag));
    Fmi::hash_combine(hash, Fmi::hash_value(modified_last));
    return hash;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Failed to get hash_value for WeatherDataQCItem!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
