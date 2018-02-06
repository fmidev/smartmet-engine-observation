#pragma once

#include <boost/date_time/gregorian/formatters.hpp>
#include <boost/date_time/posix_time/ptime.hpp>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class FlashDataItem
{
 public:
  boost::posix_time::ptime stroke_time;
  int stroke_time_fraction;
  double longitude;
  double latitude;
  unsigned int flash_id;
  int multiplicity;
  int peak_current;
  int sensors;
  int freedom_degree;
  double ellipse_angle;
  double ellipse_major;
  double ellipse_minor;
  double chi_square;
  double rise_time;
  double ptz_time;
  int cloud_indicator;
  int angle_indicator;
  int signal_indicator;
  int timing_indicator;
  int stroke_status;
  int data_source;
  boost::posix_time::ptime created;
  boost::posix_time::ptime modified_last;
  int modified_by;

  std::size_t hash_value() const;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
