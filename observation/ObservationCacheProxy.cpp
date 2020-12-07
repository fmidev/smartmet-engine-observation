#include "ObservationCacheProxy.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
namespace
{
const std::shared_ptr<ObservationCache> emptyObservationCache;
}

std::shared_ptr<ObservationCache> ObservationCacheProxy::getCacheByTableName(
    const std::string& tablename) const
{
  if (itsCachesByTableName.find(tablename) != itsCachesByTableName.end())
    return itsCachesByTableName.at(tablename);

  return emptyObservationCache;
}

std::shared_ptr<ObservationCache> ObservationCacheProxy::getCacheByName(
    const std::string& cachename) const
{
  if (itsCachesByName.find(cachename) != itsCachesByName.end())
    return itsCachesByName.at(cachename);

  return emptyObservationCache;
}

void ObservationCacheProxy::addCache(const std::string& tablename,
                                     const std::shared_ptr<ObservationCache>& cache)
{
  itsCachesByTableName.insert(std::make_pair(tablename, cache));
  itsCachesByName.insert(std::make_pair(cache->name(), cache));
}

void ObservationCacheProxy::shutdown()
{
  for (const auto& item : itsCachesByName)
    item.second->shutdown();
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
