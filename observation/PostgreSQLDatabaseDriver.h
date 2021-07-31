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
  virtual void init(Engine *obsengine) override;
  Geonames::Engine *getGeonames() const;
  std::shared_ptr<FmiIoTStations> &getFmiIoTStations() { return itsParameters.fmiIoTStations; }
  //  void setOracleDriver(DatabaseDriverBase *dbDriver) { itsOracleDriver = dbDriver; }
  void reloadStations() override;

 protected:
  PostgreSQLDatabaseDriver(const std::string &name,
                           const EngineParametersPtr &p,
                           Spine::ConfigBase &cfg);

  void initializeConnectionPool();
  void readConfig(Spine::ConfigBase &cfg);

  std::unique_ptr<PostgreSQLObsDBConnectionPool> itsPostgreSQLConnectionPool;
  boost::atomic_shared_ptr<ObservationCacheAdminPostgreSQL> itsObservationCacheAdmin;
  PostgreSQLDriverParameters itsParameters;
  Engine *itsObsEngine{nullptr};
  //  DatabaseDriverBase *itsOracleDriver;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
