#pragma once

#include <boost/date_time/gregorian/formatters.hpp>
#include <boost/date_time/posix_time/ptime.hpp>
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
  boost::posix_time::ptime data_time;
  boost::posix_time::ptime modified_last;
  double data_value = 0;
  int fmisid = 0;
  int measurand_id = 0;
  int producer_id = 0;
  int measurand_no = 0;
  int data_quality = 0;
  int data_source = 0;

  std::size_t hash_value() const;
};

using DataItems = std::vector<DataItem>;

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

std::ostream& operator<<(std::ostream& out, const SmartMet::Engine::Observation::DataItem& item);
