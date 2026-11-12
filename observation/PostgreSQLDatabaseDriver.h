#pragma once

#include "DatabaseDriverBase.h"
#include "EngineParameters.h"
#include "PostgreSQLDriverParameters.h"
#include "PostgreSQLObsDBConnectionPool.h"
#include <macgyver/AtomicSharedPtr.h>
#include <memory>
#include <string>

namespace SmartMet::Engine::Geonames
{
class Engine;
}  // namespace SmartMet::Engine::Geonames
namespace SmartMet::Engine::Observation
{
class Engine;
class ObservationCacheAdminPostgreSQL;
}  // namespace SmartMet::Engine::Observation
namespace SmartMet::Spine
{
class ConfigBase;
}  // namespace SmartMet::Spine

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class PostgreSQLDatabaseDriver : public DatabaseDriverBase
{
 public:
  void shutdown() override;
  void init(Engine *obsengine) override;
  Geonames::Engine *getGeonames() const;
  void reloadStations() override;

 protected:
  PostgreSQLDatabaseDriver(const std::string &name,
                           const EngineParametersPtr &p,
                           const Spine::ConfigBase& cfg);

  void initializeConnectionPool();
  void readConfig(Spine::ConfigBase &cfg);

  std::unique_ptr<PostgreSQLObsDBConnectionPool> itsPostgreSQLConnectionPool;
  Fmi::AtomicSharedPtr<ObservationCacheAdminPostgreSQL> itsObservationCacheAdmin;
  PostgreSQLDriverParameters itsParameters;
  Engine *itsObsEngine{nullptr};
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
