#include "DataItem.h"
#include <macgyver/Exception.h>
#include <macgyver/Hash.h>
#include <macgyver/StringConversion.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
std::size_t DataItem::hash_value() const
{
  try
  {
    std::size_t hash = Fmi::hash_value(fmisid);
    Fmi::hash_combine(hash, Fmi::hash_value(measurand_id));
    Fmi::hash_combine(hash, Fmi::hash_value(sensor_no));
    Fmi::hash_combine(hash, Fmi::hash_value(producer_id));
    Fmi::hash_combine(hash, Fmi::hash_value(measurand_no));
    Fmi::hash_combine(hash, Fmi::hash_value(data_time));
    Fmi::hash_combine(hash, Fmi::hash_value(get_value()));
    Fmi::hash_combine(hash, Fmi::hash_value(data_quality));
    Fmi::hash_combine(hash, Fmi::hash_value(data_source));
    Fmi::hash_combine(hash, Fmi::hash_value(modified_last));
    return hash;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Failed to get hash_value for DataItem!");
  }
}

std::string DataItem::get_value() const
{
  return data_value ? Fmi::to_string(*data_value) : std::string("NULL");
}

std::string DataItem::get_data_source() const
{
  if (data_source < 0)
    return "NULL";
  return Fmi::to_string(data_source);
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

std::ostream& operator<<(std::ostream& out, const SmartMet::Engine::Observation::DataItem& item)
{
  out << Fmi::to_iso_string(item.data_time) << ' ' << Fmi::to_iso_string(item.modified_last) << ' '
      << Fmi::to_string(item.fmisid) << ' ' << Fmi::to_string(item.sensor_no) << ' '
      << Fmi::to_string(item.measurand_id) << ' ' << Fmi::to_string(item.measurand_no) << ' '
      << item.get_value() << ' ' << Fmi::to_string(item.hash_value());
  return out;
}
