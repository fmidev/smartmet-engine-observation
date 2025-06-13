#pragma once

#include <macgyver/DateTime.h>
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
  Fmi::DateTime stroke_time;
  Fmi::DateTime created;
  Fmi::DateTime modified_last;
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

  bool operator==(const FlashDataItem& other) const;
  bool operator<(const FlashDataItem& other) const;
};

using FlashDataItems = std::vector<FlashDataItem>;

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
