#pragma once

#include "SpatiaLite.h"
#include "SpatiaLiteCacheParameters.h"
#include <macgyver/Pool.h>
#include <spine/Thread.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{

class SpatiaLiteConnectionPool final
{
  using PoolType =
      Fmi::Pool<Fmi::PoolInitType::Sequential, SpatiaLite, std::string, SpatiaLiteCacheParameters>;

 public:

  using Ptr = PoolType::Ptr;

  explicit SpatiaLiteConnectionPool(const SpatiaLiteCacheParameters& options);

  Ptr getConnection();

 private:
  SpatiaLiteConnectionPool() = delete;
  SpatiaLiteConnectionPool(const SpatiaLiteConnectionPool&) = delete;
  SpatiaLiteConnectionPool& operator=(const SpatiaLiteConnectionPool&) = delete;
  SpatiaLiteConnectionPool(SpatiaLiteConnectionPool&&) = delete;
  SpatiaLiteConnectionPool& operator=(SpatiaLiteConnectionPool&&) = delete;

  std::string itsSpatialiteFile;
  SpatiaLiteCacheParameters itsOptions;

  PoolType itsPool;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
