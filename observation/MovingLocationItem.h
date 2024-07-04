#pragma once

#include <macgyver/DateTime.h>
#include <optional>
#include <macgyver/StringConversion.h>
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class MovingLocationItem
{
 public:
  int station_id = 0;
  Fmi::DateTime sdate;
  Fmi::DateTime edate;
  double lon;
  double lat;
  double elev;

  std::size_t hash_value() const;
};

using MovingLocationItems = std::vector<MovingLocationItem>;

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

std::ostream& operator<<(std::ostream& out,
                         const SmartMet::Engine::Observation::MovingLocationItem& item);
