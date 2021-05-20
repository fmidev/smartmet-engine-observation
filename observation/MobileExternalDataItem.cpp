#include "MobileExternalDataItem.h"
#include <macgyver/Exception.h>
#include <macgyver/Hash.h>
#include <macgyver/StringConversion.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
std::size_t MobileExternalDataItem::hash_value() const
{
  try
  {
    std::size_t hash = Fmi::hash_value(prod_id);
    if (station_id)
      Fmi::hash_combine(hash, Fmi::hash_value(*station_id));
    if (dataset_id)
      Fmi::hash_combine(hash, Fmi::hash_value(*dataset_id));
    if (data_level)
      Fmi::hash_combine(hash, Fmi::hash_value(*data_level));
    Fmi::hash_combine(hash, Fmi::hash_value(mid));
    if (sensor_no)
      Fmi::hash_combine(hash, Fmi::hash_value(*sensor_no));
    Fmi::hash_combine(hash, Fmi::hash_value(data_time));
    Fmi::hash_combine(hash, Fmi::hash_value(data_value));
    if (data_value_txt)
      Fmi::hash_combine(hash, Fmi::hash_value(*data_value_txt));
    if (data_quality)
      Fmi::hash_combine(hash, Fmi::hash_value(*data_quality));
    if (ctrl_status)
      Fmi::hash_combine(hash, Fmi::hash_value(*ctrl_status));
    Fmi::hash_combine(hash, Fmi::hash_value(created));
    if (longitude)
      Fmi::hash_combine(hash, Fmi::hash_value(longitude));
    if (latitude)
      Fmi::hash_combine(hash, Fmi::hash_value(latitude));
    if (altitude)
      Fmi::hash_combine(hash, Fmi::hash_value(*altitude));

    return hash;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Failed to get hash_value for MobileExternalDataItem!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
