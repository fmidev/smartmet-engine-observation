#include "DataItem.h"
#include <boost/functional/hash.hpp>
#include <macgyver/StringConversion.h>
#include <spine/Exception.h>

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
    std::size_t hash = boost::hash_value(fmisid);
    boost::hash_combine(hash, boost::hash_value(measurand_id));
    boost::hash_combine(hash, boost::hash_value(sensor_no));
    boost::hash_combine(hash, boost::hash_value(producer_id));
    boost::hash_combine(hash, boost::hash_value(measurand_no));
    boost::hash_combine(hash, boost::hash_value(Fmi::to_iso_string(data_time)));
    boost::hash_combine(hash, boost::hash_value(data_value));
    boost::hash_combine(hash, boost::hash_value(data_quality));
    boost::hash_combine(hash, boost::hash_value(data_source));
    boost::hash_combine(hash, boost::hash_value(Fmi::to_iso_string(modified_last)));
    return hash;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Failed to get hash_value for DataItem!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

std::ostream& operator<<(std::ostream& out, const SmartMet::Engine::Observation::DataItem& item)
{
  out << Fmi::to_iso_string(item.data_time) << ' ' << Fmi::to_iso_string(item.modified_last) << ' '
      << Fmi::to_string(item.fmisid) << ' ' << Fmi::to_string(item.sensor_no) << ' '
      << Fmi::to_string(item.measurand_id) << ' ' << Fmi::to_string(item.measurand_no) << ' '
      << Fmi::to_string(item.data_value) << ' ' << Fmi::to_string(item.hash_value());
  return out;
};
