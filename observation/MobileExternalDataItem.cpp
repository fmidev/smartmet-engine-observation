#include "MobileExternalDataItem.h"
#include <boost/functional/hash.hpp>
#include <macgyver/StringConversion.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
std::size_t MobileExternalDataItem::hash_value() const
{
  std::size_t hash = boost::hash_value(prod_id);
  if (station_id)
    boost::hash_combine(hash, boost::hash_value(*station_id));
  if (dataset_id)
    boost::hash_combine(hash, boost::hash_value(*dataset_id));
  if (data_level)
    boost::hash_combine(hash, boost::hash_value(*data_level));
  boost::hash_combine(hash, boost::hash_value(mid));
  if (sensor_no)
    boost::hash_combine(hash, boost::hash_value(*sensor_no));
  boost::hash_combine(hash, boost::hash_value(Fmi::to_iso_string(data_time)));
  boost::hash_combine(hash, boost::hash_value(data_value));
  if (data_value_txt)
    boost::hash_combine(hash, boost::hash_value(*data_value_txt));
  if (data_quality)
    boost::hash_combine(hash, boost::hash_value(*data_quality));
  if (ctrl_status)
    boost::hash_combine(hash, boost::hash_value(*ctrl_status));
  boost::hash_combine(hash, boost::hash_value(Fmi::to_iso_string(created)));
  if (longitude)
    boost::hash_combine(hash, boost::hash_value(longitude));
  if (latitude)
    boost::hash_combine(hash, boost::hash_value(latitude));
  if (altitude)
    boost::hash_combine(hash, boost::hash_value(*altitude));

  return hash;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
