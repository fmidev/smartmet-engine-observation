#include "FlashDataItem.h"
#include <macgyver/Exception.h>
#include <macgyver/Hash.h>
#include <macgyver/StringConversion.h>

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
    std::size_t hash = Fmi::hash_value(stroke_time);
    Fmi::hash_combine(hash, Fmi::hash_value(stroke_time_fraction));
    Fmi::hash_combine(hash, Fmi::hash_value(longitude));
    Fmi::hash_combine(hash, Fmi::hash_value(latitude));
    Fmi::hash_combine(hash, Fmi::hash_value(flash_id));
    Fmi::hash_combine(hash, Fmi::hash_value(multiplicity));
    Fmi::hash_combine(hash, Fmi::hash_value(peak_current));
    Fmi::hash_combine(hash, Fmi::hash_value(sensors));
    Fmi::hash_combine(hash, Fmi::hash_value(freedom_degree));
    Fmi::hash_combine(hash, Fmi::hash_value(ellipse_angle));
    Fmi::hash_combine(hash, Fmi::hash_value(ellipse_major));
    Fmi::hash_combine(hash, Fmi::hash_value(ellipse_minor));
    Fmi::hash_combine(hash, Fmi::hash_value(chi_square));
    Fmi::hash_combine(hash, Fmi::hash_value(rise_time));
    Fmi::hash_combine(hash, Fmi::hash_value(ptz_time));
    Fmi::hash_combine(hash, Fmi::hash_value(cloud_indicator));
    Fmi::hash_combine(hash, Fmi::hash_value(angle_indicator));
    Fmi::hash_combine(hash, Fmi::hash_value(signal_indicator));
    Fmi::hash_combine(hash, Fmi::hash_value(timing_indicator));
    Fmi::hash_combine(hash, Fmi::hash_value(stroke_status));
    Fmi::hash_combine(hash, Fmi::hash_value(data_source));
    Fmi::hash_combine(hash, Fmi::hash_value(created));
    Fmi::hash_combine(hash, Fmi::hash_value(modified_last));
    Fmi::hash_combine(hash, Fmi::hash_value(modified_by));
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
