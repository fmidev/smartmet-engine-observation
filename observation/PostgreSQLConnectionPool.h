#pragma once

#include "PostgreSQL.h"
#include "PostgreSQLCacheParameters.h"
#include <spine/Thread.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class PostgreSQLConnectionPool
{
 public:
  ~PostgreSQLConnectionPool() {}
  boost::shared_ptr<PostgreSQL> getConnection();

  void releaseConnection(int connectionId);

  PostgreSQLConnectionPool(const PostgreSQLCacheParameters& options);

  void shutdown();

 private:
  PostgreSQLCacheParameters itsOptions;

  std::vector<int> itsWorkingList;
  std::vector<boost::shared_ptr<PostgreSQL> > itsWorkerList;

  SmartMet::Spine::MutexType itsGetMutex;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
