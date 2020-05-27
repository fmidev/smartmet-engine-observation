#pragma once

#include "DataItem.h"
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class LocationDataItem
{
 public:
  DataItem data;
  double longitude;
  double latitude;
  double elevation;
};

struct LocationDataItems : public std::vector<LocationDataItem>
{
  std::map<int, std::map<int, int>> default_sensors;  // fmisid -> measurand_id -> sensor_number
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
