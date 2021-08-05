#pragma once

#include "ExternalAndMobileProducerConfig.h"
#include <boost/date_time/posix_time/posix_time.hpp>
#include <spine/DataFilter.h>
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
  ExternalAndMobileDBInfo(const ExternalAndMobileProducerMeasurand *producerMeasurand = nullptr);

  std::string sqlSelect(const std::vector<int> &measurandIds,
                        const boost::posix_time::ptime &starttime,
                        const boost::posix_time::ptime &endtime,
                        const std::vector<std::string> &station_ids,
                        const Spine::DataFilter &dataFilter) const;
  std::string sqlSelect(const std::vector<int> &measurandIds,
                        const boost::posix_time::ptime &starttime,
                        const boost::posix_time::ptime &endtime,
                        const std::string &wktAreaFilter,
                        const Spine::DataFilter &dataFilter) const;
  std::string sqlSelectForCache(const std::string &producer,
                                const boost::posix_time::ptime &from_data_time,
                                const boost::posix_time::ptime &from_created_time) const;
  std::string sqlSelectFromCache(const std::vector<int> &measurandIds,
                                 const boost::posix_time::ptime &starttime,
                                 const boost::posix_time::ptime &endtime,
                                 const std::string &wktAreaFilter,
                                 const Spine::DataFilter &dataFilter,
                                 bool spatialite = false) const;

  std::string measurandFieldname(const std::string& producerName, int measurandId) const;
  void setDatabaseTableName(const std::string &tablename);

 private:
  const ExternalAndMobileProducerMeasurand *itsProducerMeasurand{nullptr};
  std::string itsDatabaseTableName{"ext_obsdata"};
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
