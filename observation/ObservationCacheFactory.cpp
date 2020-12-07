#include "ObservationCacheFactory.h"
#include "DummyCache.h"
#include "PostgreSQLCache.h"
#include "SpatiaLiteCache.h"
#include <boost/algorithm/string/join.hpp>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
std::shared_ptr<ObservationCacheProxy> ObservationCacheFactory::create(const EngineParametersPtr& p,
                                                                       const Spine::ConfigBase& cfg)
{
  std::shared_ptr<ObservationCacheProxy> cacheProxy(new ObservationCacheProxy());
  ObservationCaches caches;

  const std::map<std::string, CacheInfoItem>& cacheInfoItems =
      p->databaseDriverInfo.getAggregateCacheInfo();

  for (const auto& item : cacheInfoItems)
  {
    if (!item.second.active)
      continue;

    std::shared_ptr<ObservationCache> cache;
    const std::string& cacheName = item.first;

    if (boost::algorithm::starts_with(cacheName, "postgresql_"))
    {
      cache.reset(new PostgreSQLCache(cacheName, p, cfg));
    }
    else if (boost::algorithm::starts_with(cacheName, "spatialite_"))
    {
      cache.reset(new SpatiaLiteCache(cacheName, p, cfg));
    }
    else if (boost::algorithm::starts_with(cacheName, "dummy_"))
    {
      cache.reset(new DummyCache(cacheName, p));
    }

    if (cache)
    {
      for (const auto& tablename : item.second.tables)
      {
        // Map table name to cache
        cacheProxy->addCache(tablename, cache);
      }
    }
  }

  return cacheProxy;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
