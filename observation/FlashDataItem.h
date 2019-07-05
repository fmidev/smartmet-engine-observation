#pragma once

#include <boost/date_time/gregorian/formatters.hpp>
#include <boost/date_time/posix_time/ptime.hpp>
#include <vector>

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
  boost::posix_time::ptime created;
  boost::posix_time::ptime modified_last;
  double longitude;
  double latitude;
  double ellipse_angle;
  double ellipse_major;
  double ellipse_minor;
  double chi_square;
  double rise_time;
  double ptz_time;
  int stroke_time_fraction;
  int multiplicity;
  int peak_current;
  int sensors;
  int freedom_degree;
  int cloud_indicator;
  int angle_indicator;
  int signal_indicator;
  int timing_indicator;
  int stroke_status;
  int data_source;
  int modified_by;
  unsigned int flash_id;

  std::size_t hash_value() const;
};

using FlashDataItems = std::vector<FlashDataItem>;

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
