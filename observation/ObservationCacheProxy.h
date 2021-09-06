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
    std::map<std::string, std::shared_ptr<ObservationCache>>;  // tablename/cache name -> cache

class ObservationCacheProxy
{
 public:
  std::shared_ptr<ObservationCache> getCacheByTableName(const std::string& tablename) const;
  std::shared_ptr<ObservationCache> getCacheByName(const std::string& cachename) const;
  void addCache(const std::string& tablename, const std::shared_ptr<ObservationCache>& cache);
  const ObservationCaches& getCachesByName() const { return itsCachesByName; }

  void shutdown();

 private:
  ObservationCaches itsCachesByTableName;
  ObservationCaches itsCachesByName;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
