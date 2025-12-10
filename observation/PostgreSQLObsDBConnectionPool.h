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
struct PostgreSQLDriverParameters;

class PostgreSQLObsDBConnectionPool
{
 public:
  PostgreSQLObsDBConnectionPool() = default;

  ~PostgreSQLObsDBConnectionPool() = default;

  bool initializePool(const PostgreSQLDriverParameters& itsParameters);

  std::shared_ptr<PostgreSQLObsDB> getConnection(bool debug);

  void shutdown();

 private:
  bool initializePool(const StationtypeConfig& stc, const ParameterMapPtr& pm);

  bool addService(const Fmi::Database::PostgreSQLConnectionOptions& connectionOptions,
                  int poolSize);

  void releaseConnection(int connectionId);

  /**
   * @brief How long we wait an inactive connection if all the connections are active.
   * @param seconds Timeout seconds (default is 30 seconds)
   */
  void setGetConnectionTimeOutSeconds(std::size_t seconds);

  PostgreSQLObsDBConnectionPool(const PostgreSQLObsDBConnectionPool& other) = delete;
  PostgreSQLObsDBConnectionPool(PostgreSQLObsDBConnectionPool&& other) = delete;
  PostgreSQLObsDBConnectionPool& operator=(const PostgreSQLObsDBConnectionPool& other) = delete;
  PostgreSQLObsDBConnectionPool& operator=(PostgreSQLObsDBConnectionPool&& other) = delete;

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
