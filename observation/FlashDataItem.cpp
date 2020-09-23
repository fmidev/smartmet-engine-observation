#include "FlashDataItem.h"
#include <boost/functional/hash.hpp>
#include <macgyver/StringConversion.h>
#include <macgyver/Exception.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
std::size_t FlashDataItem::hash_value() const
{
  try
  {
    std::size_t hash = boost::hash_value(Fmi::to_iso_string(stroke_time));
    boost::hash_combine(hash, boost::hash_value(stroke_time_fraction));
    boost::hash_combine(hash, boost::hash_value(longitude));
    boost::hash_combine(hash, boost::hash_value(latitude));
    boost::hash_combine(hash, boost::hash_value(flash_id));
    boost::hash_combine(hash, boost::hash_value(multiplicity));
    boost::hash_combine(hash, boost::hash_value(peak_current));
    boost::hash_combine(hash, boost::hash_value(sensors));
    boost::hash_combine(hash, boost::hash_value(freedom_degree));
    boost::hash_combine(hash, boost::hash_value(ellipse_angle));
    boost::hash_combine(hash, boost::hash_value(ellipse_major));
    boost::hash_combine(hash, boost::hash_value(ellipse_minor));
    boost::hash_combine(hash, boost::hash_value(chi_square));
    boost::hash_combine(hash, boost::hash_value(rise_time));
    boost::hash_combine(hash, boost::hash_value(ptz_time));
    boost::hash_combine(hash, boost::hash_value(cloud_indicator));
    boost::hash_combine(hash, boost::hash_value(angle_indicator));
    boost::hash_combine(hash, boost::hash_value(signal_indicator));
    boost::hash_combine(hash, boost::hash_value(timing_indicator));
    boost::hash_combine(hash, boost::hash_value(stroke_status));
    boost::hash_combine(hash, boost::hash_value(data_source));
    if (!created.is_not_a_date_time())
      boost::hash_combine(hash, boost::hash_value(Fmi::to_iso_string(created)));
    if (!modified_last.is_not_a_date_time())
      boost::hash_combine(hash, boost::hash_value(Fmi::to_iso_string(modified_last)));
    boost::hash_combine(hash, boost::hash_value(modified_by));
    return hash;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Failed to get hash_value for FlashDataItem!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
