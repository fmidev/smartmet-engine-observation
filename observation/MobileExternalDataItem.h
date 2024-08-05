#pragma once

#include <macgyver/DateTime.h>
#include <optional>
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class MobileExternalDataItem
{
 public:
  Fmi::DateTime created;
  Fmi::DateTime data_time;
  std::optional<std::string> station_code;
  std::optional<std::string> dataset_id;
  std::optional<std::string> data_value_txt;
  std::optional<double> altitude;
  std::optional<int> station_id;
  std::optional<int> data_level;
  std::optional<int> sensor_no;
  std::optional<int> data_quality;
  std::optional<int> ctrl_status;
  double data_value = 0;
  double longitude = 0;
  double latitude = 0;
  int mid = 0;
  int prod_id = 0;

  std::size_t hash_value() const;
};

using MobileExternalDataItems = std::vector<MobileExternalDataItem>;

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
