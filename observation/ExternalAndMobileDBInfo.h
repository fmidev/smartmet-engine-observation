#pragma once

#include <boost/date_time/posix_time/posix_time.hpp>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "ExternalAndMobileProducerConfig.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class ExternalAndMobileDBInfo
{
 public:
  ExternalAndMobileDBInfo(const ExternalAndMobileProducerMeasurand *producerMeasurand = nullptr);

  std::string sqlSelect(const std::vector<int> &measurandIds,
                        const boost::posix_time::ptime &starttime,
                        const boost::posix_time::ptime &endtime,
                        const std::string &wktAreaFilter,
                        const std::map<std::string, std::vector<std::string>> &data_filter) const;
  std::string sqlSelectForCache(const std::string &producer,
                                const boost::posix_time::ptime &from_data_time,
                                const boost::posix_time::ptime &from_created_time) const;
  std::string sqlSelectFromCache(const std::vector<int> &measurandIds,
                                 const boost::posix_time::ptime &starttime,
                                 const boost::posix_time::ptime &endtime,
                                 const std::string &wktAreaFilter,
                                 const std::map<std::string, std::vector<std::string>> &data_filter,
                                 bool spatialite = false) const;

  std::string measurandFieldname(int measurandId) const;
  static boost::posix_time::ptime epoch2ptime(double epoch);

 private:
  const ExternalAndMobileProducerMeasurand *itsProducerMeasurand{nullptr};
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
