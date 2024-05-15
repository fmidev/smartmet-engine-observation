#pragma once

#include <macgyver/DateTime.h>
#include <boost/optional.hpp>
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class MagnetometerDataItem
{
 public:
  int fmisid;
  std::string magnetometer;
  int level;
  Fmi::DateTime data_time;
  boost::optional<double> x;
  boost::optional<double> y;
  boost::optional<double> z;
  boost::optional<double> t;
  boost::optional<double> f;
  int data_quality{2};  // TODO
  Fmi::DateTime modified_last;

  std::size_t hash_value() const;
};

using MagnetometerDataItems = std::vector<MagnetometerDataItem>;

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

std::ostream& operator<<(std::ostream& out,
                         const SmartMet::Engine::Observation::MagnetometerDataItem& item);
