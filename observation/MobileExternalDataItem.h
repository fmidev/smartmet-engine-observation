#pragma once

#include <boost/date_time/gregorian/formatters.hpp>
#include <boost/date_time/posix_time/ptime.hpp>
#include <boost/optional.hpp>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class MobileExternalDataItem
{
 public:
  int prod_id;
  boost::optional<int> station_id;
  boost::optional<std::string> dataset_id;
  boost::optional<int> data_level;
  int mid;
  boost::optional<int> sensor_no;
  boost::posix_time::ptime data_time;
  double data_value;
  boost::optional<std::string> data_value_txt;
  boost::optional<int> data_quality;
  boost::optional<int> ctrl_status;
  boost::posix_time::ptime created;
  double longitude;
  double latitude;
  boost::optional<double> altitude;

  std::size_t hash_value() const;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
