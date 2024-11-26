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
    Fmi::hash_combine(hash, Fmi::hash_value(data_value));
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

bool DataItem::operator==(const DataItem& other) const
{
  // modified_last, data_value and data_quality may change with obs updates

  // clang-format off
  return (data_time == other.data_time &&
          fmisid == other.fmisid &&
          sensor_no == other.sensor_no &&
          measurand_id == other.measurand_id &&
          producer_id == other.producer_id &&
          measurand_no == other.measurand_no &&
          data_source == other.data_source);
  // clang-format on
}

bool DataItem::operator<(const DataItem& other) const
{
  if (fmisid != other.fmisid)
    return fmisid < other.fmisid;
  if (data_time != other.data_time)
    return data_time < other.data_time;
  if (measurand_id < other.measurand_id)
    return measurand_id < other.measurand_id;
  if (measurand_no < other.measurand_no)
    return measurand_no < other.measurand_no;
  if (producer_id != other.producer_id)
    return producer_id < other.producer_id;
  if (data_source != other.data_source)
    return data_source < other.data_source;
  if (sensor_no != other.sensor_no)
    return sensor_no < other.sensor_no;

  return modified_last > other.modified_last;  // YES, WE WANT LATER OBS TO BE FIRST!
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
