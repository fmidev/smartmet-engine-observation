#pragma once

#include <optional>
#include <macgyver/DateTime.h>
#include <spine/Value.h>
#include <set>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
struct StationOptions
{
  std::set<int> fmisid;
  std::set<int> lpnn;
  std::set<int> wmo;
  std::set<int> rwsid;
  std::set<std::string> wsi;

  std::string type;
  std::string name;
  std::string iso2;
  std::string region;
  std::string timeformat;
  Fmi::DateTime start_time;
  Fmi::DateTime end_time;
  std::optional<Spine::BoundingBox> bbox;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
