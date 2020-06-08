#include "ObservationCacheFactory.h"
#include "DummyCache.h"
#include "PostgreSQLCache.h"
#include "SpatiaLiteCache.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
ObservationCache* ObservationCacheFactory::create(const EngineParametersPtr& p,
                                                  Spine::ConfigBase& cfg)
{
  const std::vector<Engine::Observation::DatabaseDriverInfoItem>& cacheInfoItems =
      p->databaseDriverInfo.getCacheInfo();
  for (const auto& item : cacheInfoItems)
  {
    if (!item.active)
      continue;

    const std::string& driver_id = item.name;
    if (boost::algorithm::starts_with(driver_id, "postgresql_") &&
        boost::algorithm::ends_with(driver_id, "_cache"))
    {
      return (new PostgreSQLCache(driver_id, p, cfg));
    }
    else if (boost::algorithm::starts_with(driver_id, "spatialite_") &&
             boost::algorithm::ends_with(driver_id, "_cache"))
    {
      return (new SpatiaLiteCache(driver_id, p, cfg));
    }
    else if (boost::algorithm::starts_with(driver_id, "dummy_") &&
             boost::algorithm::ends_with(driver_id, "_cache"))
    {
      return (new DummyCache(driver_id, p));
    }

    // Only one cache possible for now, maybe later a cache per producer/group of producers
    break;
  }

  return nullptr;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
