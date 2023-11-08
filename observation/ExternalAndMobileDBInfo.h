#pragma once

#include "ExternalAndMobileProducerConfig.h"
#include <macgyver/DateTime.h>
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
                        const Fmi::DateTime &starttime,
                        const Fmi::DateTime &endtime,
                        const std::vector<std::string> &station_ids,
                        const TS::DataFilter &dataFilter) const;
  std::string sqlSelect(const std::vector<int> &measurandIds,
                        const Fmi::DateTime &starttime,
                        const Fmi::DateTime &endtime,
                        const std::string &wktAreaFilter,
                        const TS::DataFilter &dataFilter) const;
  std::string sqlSelectFromCache(const std::vector<int> &measurandIds,
                                 const Fmi::DateTime &starttime,
                                 const Fmi::DateTime &endtime,
                                 const std::string &wktAreaFilter,
                                 const TS::DataFilter &dataFilter,
                                 bool spatialite = false) const;

  std::string measurandFieldname(int measurandId) const;

  static std::string sqlSelectForCache(const std::string &producer,
                                       const Fmi::DateTime &from_data_time,
                                       const Fmi::DateTime &from_created_time);

 private:
  const ExternalAndMobileProducerConfigItem *itsProducerConfig{nullptr};
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
