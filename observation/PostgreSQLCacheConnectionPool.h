#pragma once

#include "PostgreSQLCacheDB.h"
#include "PostgreSQLCacheParameters.h"
#include <macgyver/Exception.h>
#include <macgyver/Pool.h>
#include <spine/Thread.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{

class PostgreSQLCacheConnectionPool final
{
  using PoolType = Fmi::Pool<Fmi::PoolInitType::Parallel,
                             PostgreSQLCacheDB,
                             PostgreSQLCacheParameters>;

 public:

  using Ptr = PoolType::Ptr;

  Ptr getConnection();

  explicit PostgreSQLCacheConnectionPool(const PostgreSQLCacheParameters& options);

  void shutdown();

 private:
  PostgreSQLCacheConnectionPool() = delete;
  PostgreSQLCacheConnectionPool(const PostgreSQLCacheConnectionPool&) = delete;
  PostgreSQLCacheConnectionPool& operator=(const PostgreSQLCacheConnectionPool&) = delete;
  PostgreSQLCacheConnectionPool(PostgreSQLCacheConnectionPool&&) = delete;
  PostgreSQLCacheConnectionPool& operator=(PostgreSQLCacheConnectionPool&&) = delete;

  PostgreSQLCacheParameters itsOptions;

  PoolType itsPool;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
