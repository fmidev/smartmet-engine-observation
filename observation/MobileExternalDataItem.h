#pragma once

#include <boost/date_time/gregorian/formatters.hpp>
#include <boost/date_time/posix_time/ptime.hpp>
#include <boost/optional.hpp>
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class MobileExternalDataItem
{
 public:
  boost::posix_time::ptime created;
  boost::posix_time::ptime data_time;
  boost::optional<std::string> station_code;
  boost::optional<std::string> dataset_id;
  boost::optional<std::string> data_value_txt;
  boost::optional<double> altitude;
  boost::optional<int> station_id;
  boost::optional<int> data_level;
  boost::optional<int> sensor_no;
  boost::optional<int> data_quality;
  boost::optional<int> ctrl_status;
  double data_value = 0;
  double longitude = 0;
  double latitude = 0;
  int mid = 0;
  int prod_id = 0;

  std::size_t hash_value() const;
};

using MobileExternalDataItems = std::vector<MobileExternalDataItem>;

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
