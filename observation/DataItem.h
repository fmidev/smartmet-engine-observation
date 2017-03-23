#pragma once

#include <boost/date_time/gregorian/formatters.hpp>
#include <boost/date_time/posix_time/ptime.hpp>

namespace pt = boost::posix_time;

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class DataItem
{
 public:
  int fmisid;
  int measurand_id;
  int producer_id;
  int measurand_no;
  double data_level;
  pt::ptime data_time;
  double data_value;
  int data_quality;

 private:
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
