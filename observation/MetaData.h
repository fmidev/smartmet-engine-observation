#pragma once

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
  MetaData(Spine::BoundingBox b, const boost::posix_time::time_period &p, int step)
      : bbox(std::move(b)), period(p), timestep(step)
  {
  }
  MetaData()
      : bbox(0.0, 0.0, 0.0, 0.0), period(boost::posix_time::ptime(), boost::posix_time::ptime())
  {
  }
  MetaData(const MetaData &md) = default;
  MetaData &operator=(const MetaData &md) = default;
  MetaData(MetaData &&md) = default;
  MetaData &operator=(MetaData &&md) = default;

  Spine::BoundingBox bbox;
  boost::posix_time::time_period period;
  bool fixedPeriodEndTime{false};
  int timestep = 1;  // timestep in minutes
  std::set<std::string> parameters;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
