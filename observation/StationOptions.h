#pragma once

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
  std::string type;
  std::string name;
  std::string iso2;
  std::string region;
  std::string timeformat;
  boost::posix_time::ptime start_time;
  boost::posix_time::ptime end_time;
  boost::optional<Spine::BoundingBox> bbox;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
