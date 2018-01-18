#include "SpatiaLiteConnectionPool.h"
#include <spine/Exception.h>

#include <boost/foreach.hpp>
#include <boost/make_shared.hpp>

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
  Releaser(SmartMet::Engine::Observation::SpatiaLiteConnectionPool* pool_handle)
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
      throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
    }
  }

  SmartMet::Engine::Observation::SpatiaLiteConnectionPool* poolHandle;
};
}  // namespace

SpatiaLiteConnectionPool::SpatiaLiteConnectionPool(int poolSize,
                                                   const string& spatialiteFile,
                                                   std::size_t maxInsertSize,
                                                   const SpatiaLiteOptions& options)
    : itsSpatialiteFile(spatialiteFile), itsMaxInsertSize(maxInsertSize), itsOptions(options)
{
  try
  {
    itsWorkingList.resize(poolSize, -1);
    itsWorkerList.resize(poolSize);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

boost::shared_ptr<SpatiaLite> SpatiaLiteConnectionPool::getConnection()
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

    while (true)
    {
      {
        SmartMet::Spine::WriteLock lock(itsGetMutex);
        for (unsigned int i = 0; i < itsWorkingList.size(); i++)
        {
          if (itsWorkingList[i] == 0)
          {
            itsWorkingList[i] = 1;
            itsWorkerList[i]->setConnectionId(i);
            return boost::shared_ptr<SpatiaLite>(itsWorkerList[i].get(),
                                                 Releaser<SpatiaLite>(this));
          }
          else if (itsWorkingList[i] == -1)
          {
            try
            {
              // Logon here
              itsWorkerList[i] =
                  boost::make_shared<SpatiaLite>(itsSpatialiteFile, itsMaxInsertSize, itsOptions);

              itsWorkingList[i] = 1;
              itsWorkerList[i]->setConnectionId(i);
              return boost::shared_ptr<SpatiaLite>(itsWorkerList[i].get(),
                                                   Releaser<SpatiaLite>(this));
            }
            catch (...)
            {
              cerr << "[Observation] SpatiaLiteConnectionPool error: could not get a connection: "
                   << endl;
            }
          }
        }
      }
      // If we cannot get the mutex, let other threads to try to get it.
      // This potentially helps to recover from situations where many threads are trying to get the
      // same lock.
      boost::this_thread::yield();
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
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
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
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
    std::cout << "  -- Shutdown requested (SpatiaLiteConnectionPool)\n";
    for (unsigned int i = 0; i < itsWorkerList.size(); i++)
    {
      auto sl = itsWorkerList[i].get();
      if (sl != NULL)
        sl->shutdown();
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
