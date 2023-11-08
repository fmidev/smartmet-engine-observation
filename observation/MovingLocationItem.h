#pragma once

#include <boost/date_time/gregorian/formatters.hpp>
#include <macgyver/DateTime.h>
#include <boost/optional.hpp>
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
