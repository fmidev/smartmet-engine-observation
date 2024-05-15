#pragma once

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
class DataItem
{
 public:
  // If you add new data members don't forget to change hash_value()
  Fmi::DateTime data_time;
  Fmi::DateTime modified_last;
  boost::optional<double> data_value;
  int fmisid = 0;
  int sensor_no = 0;
  int measurand_id = 0;
  int producer_id = 0;
  int measurand_no = 0;
  int data_quality = 0;
  int data_source = -1;  // -1 indicates NULL value

  std::size_t hash_value() const;
  std::string get_value() const;
  std::string get_data_source() const;
};

using DataItems = std::vector<DataItem>;

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

std::ostream& operator<<(std::ostream& out, const SmartMet::Engine::Observation::DataItem& item);
