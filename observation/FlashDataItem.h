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
  double longitude = 0;
  double latitude = 0;
  double ellipse_angle = 0;
  double ellipse_major = 0;
  double ellipse_minor = 0;
  double chi_square = 0;
  double rise_time = 0;
  double ptz_time = 0;
  int stroke_time_fraction = 0;
  int multiplicity = 0;
  int peak_current = 0;
  int sensors = 0;
  int freedom_degree = 0;
  int cloud_indicator = 0;
  int angle_indicator = 0;
  int signal_indicator = 0;
  int timing_indicator = 0;
  int stroke_status = 0;
  int data_source = -1;  // -1 indicates NULL value
  int modified_by = 0;
  unsigned int flash_id = 0;

  std::size_t hash_value() const;
};

using FlashDataItems = std::vector<FlashDataItem>;

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
