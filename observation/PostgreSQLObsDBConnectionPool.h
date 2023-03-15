#pragma once

#include "PostgreSQLObsDB.h"
#include <macgyver/PostgreSQLConnection.h>
#include <spine/Thread.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class PostgreSQLDatabaseDriver;

class PostgreSQLObsDBConnectionPool
{
 public:
  ~PostgreSQLObsDBConnectionPool() = default;
  PostgreSQLObsDBConnectionPool() = delete;
  PostgreSQLObsDBConnectionPool(const PostgreSQLObsDBConnectionPool& other) = delete;
  PostgreSQLObsDBConnectionPool(PostgreSQLObsDBConnectionPool&& other) = delete;
  PostgreSQLObsDBConnectionPool& operator=(const PostgreSQLObsDBConnectionPool& other) = delete;
  PostgreSQLObsDBConnectionPool& operator=(PostgreSQLObsDBConnectionPool&& other) = delete;

  bool initializePool(const StationtypeConfig& stc, const ParameterMapPtr& pm);

  std::shared_ptr<PostgreSQLObsDB> getConnection(bool debug);
  void releaseConnection(int connectionId);
  PostgreSQLObsDBConnectionPool(PostgreSQLDatabaseDriver* driver);
  bool addService(const Fmi::Database::PostgreSQLConnectionOptions& connectionOptions,
                  int poolSize);

  /**
   * @brief How long we wait an inactive connection if all the connections are active.
   * @param seconds Timeout seconds (default is 30 seconds)
   */
  void setGetConnectionTimeOutSeconds(const size_t seconds);

  void shutdown();

 private:
  std::vector<int> itsWorkingList;
  std::vector<std::shared_ptr<PostgreSQLObsDB> > itsWorkerList;
  Spine::MutexType itsGetMutex;
  std::vector<Fmi::Database::PostgreSQLConnectionOptions> itsConnectionOptions;
  std::vector<size_t> itsServicePool;
  std::size_t itsPoolSize = 0;
  std::size_t itsLastConnectionID = 0;  // for rotating through the pool frequently
  std::size_t itsGetConnectionTimeOutSeconds = 30;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
