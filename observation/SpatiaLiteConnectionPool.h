#pragma once

#include "SpatiaLite.h"
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
  ~SpatiaLiteConnectionPool() {}  //{ delete itsInstance; }
  boost::shared_ptr<SpatiaLite> getConnection();

  void releaseConnection(int connectionId);

  SpatiaLiteConnectionPool(int poolSize,
                           const std::string &spatialiteFile,
                           std::size_t max_insert_size,
                           const std::string &synchronous,
                           const std::string &journal_mode,
                           std::size_t mmap_size,
                           bool shared_cache,
                           int timeout);

  void shutdown();

 private:
  std::string itsSpatialiteFile;
  std::size_t itsMaxInsertSize;
  std::string itsSynchronous;
  std::string itsJournalMode;
  std::size_t itsMMapSize;
  bool itsSharedCache;
  int itsTimeout;

  std::vector<int> itsWorkingList;
  std::vector<boost::shared_ptr<SpatiaLite> > itsWorkerList;

  SmartMet::Spine::MutexType itsGetMutex;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
