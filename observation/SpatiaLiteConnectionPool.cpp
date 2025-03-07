#include "SpatiaLiteConnectionPool.h"
#include "SpatiaLiteCacheParameters.h"
#include <boost/make_shared.hpp>
#include <fmt/format.h>
#include <macgyver/Exception.h>

using namespace std;

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
namespace
{
template <class T>
struct Releaser
{
  explicit Releaser(Engine::Observation::SpatiaLiteConnectionPool* pool_handle)
      : poolHandle(pool_handle)
  {
  }
  void operator()(T* t)
  {
    try
    {
      poolHandle->releaseConnection(t->connectionId());
    }
    catch (...)
    {
      throw Fmi::Exception::Trace(BCP, "Operation failed!");
    }
  }

  Engine::Observation::SpatiaLiteConnectionPool* poolHandle;
};
}  // namespace

SpatiaLiteConnectionPool::SpatiaLiteConnectionPool(const SpatiaLiteCacheParameters& options)
    : itsSpatialiteFile(options.cacheFile), itsOptions(options)
{
  try
  {
    itsWorkingList.resize(options.connectionPoolSize, 0);
    itsWorkerList.resize(options.connectionPoolSize);

    // Create all connections in advance, not when needed
    for (auto& worker : itsWorkerList)
      worker = std::make_shared<SpatiaLite>(itsSpatialiteFile, itsOptions);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

std::shared_ptr<SpatiaLite> SpatiaLiteConnectionPool::getConnection()
{
  try
  {
    /*
     *  1 --> active
     *  0 --> inactive
     * -1 --> uninitialized
     *
     * Logic of returning connections:
     *
     * 1. Check if worker is idle, if so return that worker.
     * 2. Check if worker is uninitialized, if so create worker and return that.
     * 3. Sleep and start over
     */

    auto failures = 0;

    while (true)
    {
      {
        Spine::WriteLock lock(itsGetMutex);
        for (unsigned int i = 0; i < itsWorkingList.size(); i++)
        {
          if (itsWorkingList[i] == 0)
          {
            itsWorkingList[i] = 1;
            itsWorkerList[i]->setConnectionId(i);
            if (failures > 0)
              std::cerr << fmt::format(
                  "Success: after {} failure(s) got a free connection from the Spatialite "
                  "connection pool\n",
                  failures);
            return {itsWorkerList[i].get(), Releaser<SpatiaLite>(this)};
          }
        }
      }
      // If we cannot get the mutex, let other threads to try to get it.
      // This potentially helps to recover from situations where many threads are trying to get the
      // same lock.
      if (failures++ == 0)
        std::cerr << "Warning: failed to get a connection from the Spatialite connection pool\n";

      boost::this_thread::yield();
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void SpatiaLiteConnectionPool::releaseConnection(int connectionId)
{
  try
  {
    // This mutex is not needed since writing the int is atomic. In fact, if there is a queue to
    // get connections, releasing a SpatiaLite back to the pool would have to compete against the
    // threads which are trying to get a connection. The more requests are coming, the less
    // chances we have of releasing the connection back to the pool, which may escalate the
    // problem - Mika

    // boost::mutex::scoped_lock lock(itsGetMutex);

    // Do "destructor" stuff here, because SpatiaLite instances are never destructed

    // Release the worker to the pool
    itsWorkingList[connectionId] = 0;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Shutdown connections
 */
// ----------------------------------------------------------------------

void SpatiaLiteConnectionPool::shutdown()
{
  try
  {
    std::cout << fmt::format(
        "  -- Shutdown requested for SpatiaLiteConnectionPool with {} workers\n",
        itsWorkerList.size());

    for (const auto& worker : itsWorkerList)
    {
      auto* sl = worker.get();
      if (sl != nullptr)
        sl->shutdown();
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
