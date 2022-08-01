#include "MagnetometerDataItem.h"
#include <macgyver/Exception.h>
#include <macgyver/Hash.h>
#include <macgyver/StringConversion.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
std::size_t MagnetometerDataItem::hash_value() const
{
  try
  {
    std::size_t hash = Fmi::hash_value(fmisid);
    Fmi::hash_combine(hash, Fmi::hash_value(magnetometer));
    Fmi::hash_combine(hash, Fmi::hash_value(level));
    Fmi::hash_combine(hash, Fmi::hash_value(data_time));
    Fmi::hash_combine(hash, Fmi::hash_value(x));
    Fmi::hash_combine(hash, Fmi::hash_value(y));
    Fmi::hash_combine(hash, Fmi::hash_value(z));
    Fmi::hash_combine(hash, Fmi::hash_value(t));
    Fmi::hash_combine(hash, Fmi::hash_value(f));
    Fmi::hash_combine(hash, Fmi::hash_value(data_quality));
    Fmi::hash_combine(hash, Fmi::hash_value(modified_last));
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

std::ostream& operator<<(std::ostream& out,
                         const SmartMet::Engine::Observation::MagnetometerDataItem& item)
{
  out << Fmi::to_string(item.fmisid) << ' ' << item.magnetometer << ' '
      << Fmi::to_string(item.level) << ' ' << Fmi::to_iso_string(item.data_time) << ' '
      << (item.x ? Fmi::to_string(*item.x) : "") << ' ' << (item.y ? Fmi::to_string(*item.y) : "")
      << ' ' << (item.z ? Fmi::to_string(*item.z) : "") << ' '
      << (item.t ? Fmi::to_string(*item.t) : "") << ' ' << (item.f ? Fmi::to_string(*item.f) : "")
      << ' ' << Fmi::to_string(item.data_quality) << ' ' << Fmi::to_iso_string(item.modified_last)
      << ' ' << Fmi::to_string(item.hash_value());

  return out;
}
