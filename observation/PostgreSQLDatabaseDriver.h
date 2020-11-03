#pragma once

#include "DatabaseDriverBase.h"
#include "ObservationCacheAdminPostgreSQL.h"
#include "PostgreSQLObsDB.h"
#include "PostgreSQLObsDBConnectionPool.h"
#include "PostgreSQLDriverParameters.h"

#include "Engine.h"
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
  void shutdown();
  virtual void init(Engine *obsengine);
  Geonames::Engine *getGeonames() const;
  boost::shared_ptr<FmiIoTStations> &getFmiIoTStations() { return itsParameters.fmiIoTStations; }
  //  void setOracleDriver(DatabaseDriverBase *dbDriver) { itsOracleDriver = dbDriver; }
  void reloadStations();

 protected:
  PostgreSQLDatabaseDriver(const std::string &name,
                           const EngineParametersPtr &p,
                           Spine::ConfigBase &cfg);

  void initializeConnectionPool();
  void readConfig(Spine::ConfigBase &cfg);

  std::unique_ptr<PostgreSQLObsDBConnectionPool> itsPostgreSQLConnectionPool;
  boost::shared_ptr<ObservationCacheAdminPostgreSQL> itsObservationCacheAdmin;
  PostgreSQLDriverParameters itsParameters;
  Engine *itsObsEngine{nullptr};
  //  DatabaseDriverBase *itsOracleDriver;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
