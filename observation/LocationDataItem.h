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
  std::string stationtype;
};

using LocationDataItems = std::vector<LocationDataItem>;

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
