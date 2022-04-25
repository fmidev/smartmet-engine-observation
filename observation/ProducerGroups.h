#pragma once

#include <boost/date_time/posix_time/posix_time.hpp>
#include <set>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{

using ProducerGroupPeriod = boost::posix_time::time_period; // group period
using ProducerGroupPeriodVector = std::vector<ProducerGroupPeriod>; // group periods
using ProducerGroupPeriodMap = std::map<unsigned int, ProducerGroupPeriodVector>; // producer id -> group periods
using ProducerGroupPeriods = std::map<std::string, ProducerGroupPeriodMap>; // group_name -> group periods
  
class ProducerGroups
{
public:  
  void addGroupPeriod(const std::string& group_name,
					  unsigned int producer_id,
					  const boost::posix_time::ptime& starttime,
					  const boost::posix_time::ptime& endtime);

  std::set<unsigned int> getProducerIds(const std::string& group_name,
										const boost::posix_time::ptime& starttime,
										const boost::posix_time::ptime& endtime) const;
  std::set<std::string> getProducerIdsString(const std::string& group_name,
											 const boost::posix_time::ptime& starttime,
											 const boost::posix_time::ptime& endtime) const;
  std::set<std::string> getProducerGroups() const;
  void replaceProducerIds(const std::string& group_name_from, const std::string& group_name_to);

private:
  ProducerGroupPeriods itsGroupPeriods;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
