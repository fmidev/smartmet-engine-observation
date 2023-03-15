#pragma once

#include "SpatiaLite.h"
#include "SpatiaLiteCacheParameters.h"
#include <spine/Thread.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class SpatiaLiteConnectionPool
{
 public:
  SpatiaLiteConnectionPool(const SpatiaLiteCacheParameters& options);
  SpatiaLiteConnectionPool() = delete;

  std::shared_ptr<SpatiaLite> getConnection();

  void releaseConnection(int connectionId);

  void shutdown();

 private:
  std::string itsSpatialiteFile;
  SpatiaLiteCacheParameters itsOptions;

  std::vector<int> itsWorkingList;
  std::vector<std::shared_ptr<SpatiaLite> > itsWorkerList;

  Spine::MutexType itsGetMutex;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
