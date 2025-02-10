#pragma once

#include <macgyver/DateTime.h>
#include <spine/Value.h>
#include <set>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
struct MetaData
{
  MetaData(Spine::BoundingBox b, const Fmi::TimePeriod &p, int step)
      : bbox(std::move(b)), period(p), timestep(step)
  {
  }
  MetaData() : bbox(0.0, 0.0, 0.0, 0.0), period(Fmi::DateTime(), Fmi::DateTime()) {}
  MetaData(const MetaData &md) = default;
  MetaData &operator=(const MetaData &md) = default;
  MetaData(MetaData &&md) = default;
  MetaData &operator=(MetaData &&md) = default;

  Spine::BoundingBox bbox;
  Fmi::TimePeriod period;
  bool fixedPeriodEndTime{false};
  int timestep = 1;  // timestep in minutes
  std::set<std::string> parameters;
  Fmi::DateTime latestDataUpdateTime;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
