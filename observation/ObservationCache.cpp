#include "ObservationCache.h"
#include <macgyver/StringConversion.h>
#include <macgyver/TimeParser.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
ObservationCache::ObservationCache(const CacheInfoItem& ci) : itsCacheInfo(ci) {}

const std::string &ObservationCache::name() const { return itsCacheInfo.name; }

  
std::vector<std::map<std::string,std::string>> ObservationCache::getFakeCacheSettings(const std::string& tablename) const
{
	std::vector<std::map<std::string,std::string>> ret;

	if(itsCacheInfo.params.find(tablename) != itsCacheInfo.params.end())
	  {
		std::string table_config = itsCacheInfo.params.at(tablename);
		std::vector<std::string> table_config_items;
		boost::algorithm::split(table_config_items, table_config, boost::algorithm::is_any_of("#"));
		
		for(const auto& config_item : table_config_items)
		  {
			if(config_item.empty())
			  continue;
			std::vector<std::string> config_vector;
			boost::algorithm::split(config_vector, config_item, boost::algorithm::is_any_of(";"));
			std::map<std::string,std::string> config_map;
			config_map["starttime"] = config_vector[0];
			config_map["endtime"] = config_vector[1];
			config_map["measurand_id"] = config_vector[2];
			config_map["fmisid"] = config_vector[3];
			ret.push_back(config_map);
		  }
	  }

	return ret;
}

// Only fake cache contains table name as parameter name 
bool ObservationCache::isFakeCache(const std::string& tablename) const
{
  return (itsCacheInfo.params.find(tablename) != itsCacheInfo.params.end());

}

ObservationCache::~ObservationCache() {}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
