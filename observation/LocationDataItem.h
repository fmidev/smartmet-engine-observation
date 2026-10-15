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
  double longitude = 0.0;
  double latitude = 0.0;
  double elevation = 0.0;
  std::string stationtype;
};

using LocationDataItems = std::vector<LocationDataItem>;

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
