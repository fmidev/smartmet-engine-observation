#pragma once

#include <spine/Value.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
struct MetaData
{
  MetaData(const Spine::BoundingBox &b, const boost::posix_time::time_period &p, int step)
      : bbox(b), period(p), timestep(step)
  {
  }
  MetaData()
      : bbox(0.0, 0.0, 0.0, 0.0),
        period(boost::posix_time::ptime(), boost::posix_time::ptime()),
        timestep(1)
  {
  }
  MetaData(const MetaData &md) = default;

  Spine::BoundingBox bbox;
  boost::posix_time::time_period period;
  bool fixedPeriodEndTime{false};
  int timestep;  // timestep in minutes
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
