#pragma once

#include "ExternalAndMobileProducerConfig.h"
#include <boost/date_time/posix_time/posix_time.hpp>
#include <timeseries/TimeSeriesInclude.h>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class ExternalAndMobileDBInfo
{
 public:
  explicit ExternalAndMobileDBInfo(
      const ExternalAndMobileProducerConfigItem *producerConfig = nullptr);

  std::string sqlSelect(const std::vector<int> &measurandIds,
                        const boost::posix_time::ptime &starttime,
                        const boost::posix_time::ptime &endtime,
                        const std::vector<std::string> &station_ids,
                        const TS::DataFilter &dataFilter) const;
  std::string sqlSelect(const std::vector<int> &measurandIds,
                        const boost::posix_time::ptime &starttime,
                        const boost::posix_time::ptime &endtime,
                        const std::string &wktAreaFilter,
                        const TS::DataFilter &dataFilter) const;
  std::string sqlSelectFromCache(const std::vector<int> &measurandIds,
                                 const boost::posix_time::ptime &starttime,
                                 const boost::posix_time::ptime &endtime,
                                 const std::string &wktAreaFilter,
                                 const TS::DataFilter &dataFilter,
                                 bool spatialite = false) const;

  std::string measurandFieldname(int measurandId) const;

  static std::string sqlSelectForCache(const std::string &producer,
                                       const boost::posix_time::ptime &from_data_time,
                                       const boost::posix_time::ptime &from_created_time);

 private:
  const ExternalAndMobileProducerConfigItem *itsProducerConfig{nullptr};
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
