#pragma once

#include "ObservationCache.h"
#include <map>
#include <set>
#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
using ObservationCaches =
    std::map<std::string, boost::shared_ptr<ObservationCache>>;  // tablename/cache name -> cache

class ObservationCacheProxy
{
 public:
  boost::shared_ptr<ObservationCache> getCacheByTableName(const std::string& tablename) const;
  boost::shared_ptr<ObservationCache> getCacheByName(const std::string& cachename) const;
  void addCache(const std::string& tablename, const boost::shared_ptr<ObservationCache>& cache);

  void shutdown();

 private:
  ObservationCaches itsCachesByTableName;
  ObservationCaches itsCachesByName;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
