#pragma once

#include <boost/date_time/posix_time/posix_time.hpp>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "ExternalAndMobileProducerConfig.h"
#include "SQLDataFilter.h"

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
                        const std::vector<std::string> &station_ids,
                        const SQLDataFilter &sqlDataFilter) const;
  std::string sqlSelect(const std::vector<int> &measurandIds,
                        const boost::posix_time::ptime &starttime,
                        const boost::posix_time::ptime &endtime,
                        const std::string &wktAreaFilter,
                        const SQLDataFilter &sqlDataFilter) const;
  std::string sqlSelectForCache(const std::string &producer,
                                const boost::posix_time::ptime &from_data_time,
                                const boost::posix_time::ptime &from_created_time) const;
  std::string sqlSelectFromCache(const std::vector<int> &measurandIds,
                                 const boost::posix_time::ptime &starttime,
                                 const boost::posix_time::ptime &endtime,
                                 const std::string &wktAreaFilter,
                                 const SQLDataFilter &sqlDataFilter,
                                 bool spatialite = false) const;

  std::string measurandFieldname(int measurandId) const;
  static boost::posix_time::ptime epoch2ptime(double epoch);

 private:
  const ExternalAndMobileProducerMeasurand *itsProducerMeasurand{nullptr};
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
