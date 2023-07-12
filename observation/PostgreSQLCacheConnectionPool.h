#pragma once

#include "PostgreSQLCacheDB.h"
#include "PostgreSQLCacheParameters.h"
#include <spine/Thread.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class PostgreSQLCacheConnectionPool
{
 public:
  std::shared_ptr<PostgreSQLCacheDB> getConnection();

  void releaseConnection(int connectionId);

  PostgreSQLCacheConnectionPool() = delete;
  explicit PostgreSQLCacheConnectionPool(const PostgreSQLCacheParameters& options);

  void shutdown();

 private:
  PostgreSQLCacheParameters itsOptions;

  std::vector<int> itsWorkingList;
  std::vector<std::shared_ptr<PostgreSQLCacheDB>> itsWorkerList;

  Spine::MutexType itsGetMutex;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
