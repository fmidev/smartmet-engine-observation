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
  ~SpatiaLiteConnectionPool() = default;
  std::shared_ptr<SpatiaLite> getConnection();

  void releaseConnection(int connectionId);

  SpatiaLiteConnectionPool(const SpatiaLiteCacheParameters& options);

  void shutdown();

 private:
  std::string itsSpatialiteFile;
  SpatiaLiteCacheParameters itsOptions;

  std::vector<int> itsWorkingList;
  std::vector<std::shared_ptr<SpatiaLite> > itsWorkerList;

  SmartMet::Spine::MutexType itsGetMutex;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
