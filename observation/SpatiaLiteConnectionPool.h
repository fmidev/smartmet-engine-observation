#pragma once

#include "SpatiaLite.h"
#include <spine/Thread.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class SpatiaLiteOptions;

class SpatiaLiteConnectionPool
{
 public:
  ~SpatiaLiteConnectionPool() {}  //{ delete itsInstance; }
  boost::shared_ptr<SpatiaLite> getConnection();

  void releaseConnection(int connectionId);

  SpatiaLiteConnectionPool(int poolSize,
                           const std::string &spatialiteFile,
                           std::size_t maxInsertSize,
                           const SpatiaLiteOptions &options);

  void shutdown();

 private:
  std::string itsSpatialiteFile;
  std::size_t itsMaxInsertSize;
  SpatiaLiteOptions itsOptions;

  std::vector<int> itsWorkingList;
  std::vector<boost::shared_ptr<SpatiaLite> > itsWorkerList;

  SmartMet::Spine::MutexType itsGetMutex;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
