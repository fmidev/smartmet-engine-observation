#pragma once

#include <boost/date_time/gregorian/formatters.hpp>
#include <boost/date_time/posix_time/ptime.hpp>

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
  int fmisid;
  int measurand_id;
  int producer_id;
  int measurand_no;
  double data_level;
  boost::posix_time::ptime data_time;
  double data_value;
  int data_quality;

  std::size_t hash_value() const;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
