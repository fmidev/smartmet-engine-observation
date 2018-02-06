#include "WeatherDataQCItem.h"
#include <boost/functional/hash.hpp>
#include <macgyver/StringConversion.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
std::size_t WeatherDataQCItem::hash_value() const
{
  std::size_t hash = boost::hash_value(fmisid);
  boost::hash_combine(hash, boost::hash_value(Fmi::to_iso_string(obstime)));
  boost::hash_combine(hash, boost::hash_value(parameter));
  boost::hash_combine(hash, boost::hash_value(sensor_no));
  boost::hash_combine(hash, boost::hash_value(value));
  boost::hash_combine(hash, boost::hash_value(flag));
  return hash;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
