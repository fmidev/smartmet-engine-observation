#include "MovingLocationItem.h"
#include <macgyver/Exception.h>
#include <macgyver/Hash.h>
#include <macgyver/StringConversion.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
std::size_t MovingLocationItem::hash_value() const
{
  try
  {
    std::size_t hash = Fmi::hash_value(station_id);
    Fmi::hash_combine(hash, Fmi::hash_value(sdate));
    Fmi::hash_combine(hash, Fmi::hash_value(edate));
    Fmi::hash_combine(hash, Fmi::hash_value(lon));
    Fmi::hash_combine(hash, Fmi::hash_value(lat));
    Fmi::hash_combine(hash, Fmi::hash_value(elev));
    return hash;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Failed to get hash_value for DataItem!");
  }
}


}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

std::ostream& operator<<(std::ostream& out, const SmartMet::Engine::Observation::MovingLocationItem& item)
{
  out << Fmi::to_iso_string(item.station_id) << ' ' << Fmi::to_iso_string(item.sdate) << ' '
      << Fmi::to_iso_string(item.edate) << ' ' << Fmi::to_string(item.lon) << ' '
      << Fmi::to_string(item.lat) << ' ' << Fmi::to_string(item.elev)  << ' ' << Fmi::to_string(item.hash_value());
  return out;
}
