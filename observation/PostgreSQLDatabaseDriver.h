#pragma once

#include "DatabaseDriverBase.h"
#include "Engine.h"
#include "ObservationCacheAdminPostgreSQL.h"
#include "PostgreSQLDriverParameters.h"
#include "PostgreSQLObsDB.h"
#include "PostgreSQLObsDBConnectionPool.h"
#include <boost/smart_ptr/atomic_shared_ptr.hpp>
#include <memory>
#include <string>

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
                           Spine::ConfigBase &cfg);

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
