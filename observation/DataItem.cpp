#include "DataItem.h"
#include <boost/functional/hash.hpp>
#include <macgyver/StringConversion.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
std::size_t DataItem::hash_value() const
{
  std::size_t hash = boost::hash_value(fmisid);
  boost::hash_combine(hash, boost::hash_value(measurand_id));
  boost::hash_combine(hash, boost::hash_value(producer_id));
  boost::hash_combine(hash, boost::hash_value(measurand_no));
  boost::hash_combine(hash, boost::hash_value(Fmi::to_iso_string(data_time)));
  boost::hash_combine(hash, boost::hash_value(data_value));
  boost::hash_combine(hash, boost::hash_value(data_quality));
  return hash;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

std::ostream& operator<<(std::ostream& out, const SmartMet::Engine::Observation::DataItem& item)
{
  out << Fmi::to_iso_string(item.data_time) << ' ' << Fmi::to_string(item.fmisid) << ' '
      << Fmi::to_string(item.measurand_id) << ' ' << Fmi::to_string(item.measurand_no) << ' '
      << Fmi::to_string(item.data_value) << ' ' << Fmi::to_string(item.hash_value());
  return out;
};
