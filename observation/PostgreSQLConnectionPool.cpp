#include "PostgreSQLConnectionPool.h"
#include "PostgreSQLCacheParameters.h"
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
  Releaser(Engine::Observation::PostgreSQLConnectionPool* pool_handle) : poolHandle(pool_handle) {}
  void operator()(T* t)
  {
    try
    {
      poolHandle->releaseConnection(t->connectionId());
    }
    catch (...)
    {
      throw Spine::Exception::Trace(BCP, "Operation failed!");
    }
  }

  Engine::Observation::PostgreSQLConnectionPool* poolHandle;
};
}  // namespace

PostgreSQLConnectionPool::PostgreSQLConnectionPool(const PostgreSQLCacheParameters& options)
    : itsOptions(options)
{
  try
  {
    itsWorkingList.resize(options.connectionPoolSize, 0);
    itsWorkerList.resize(options.connectionPoolSize);

    // Create all connections in advance, not when needed
    for (std::size_t i = 0; i < itsWorkerList.size(); i++)
      itsWorkerList[i] = boost::make_shared<PostgreSQL>(itsOptions);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

boost::shared_ptr<PostgreSQL> PostgreSQLConnectionPool::getConnection()
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
        Spine::WriteLock lock(itsGetMutex);
        for (unsigned int i = 0; i < itsWorkingList.size(); i++)
        {
          if (itsWorkingList[i] == 0)
          {
            itsWorkingList[i] = 1;
            itsWorkerList[i]->setConnectionId(i);
            return boost::shared_ptr<PostgreSQL>(itsWorkerList[i].get(),
                                                 Releaser<PostgreSQL>(this));
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
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

void PostgreSQLConnectionPool::releaseConnection(int connectionId)
{
  try
  {
    // This mutex is not needed since writing the int is atomic. In fact, if there is a queue to
    // get connections, releasing a PostgreSQL back to the pool would have to compete against the
    // threads which are trying to get a connection. The more requests are coming, the less
    // chances we have of releasing the connection back to the pool, which may escalate the
    // problem - Mika

    // boost::mutex::scoped_lock lock(itsGetMutex);

    // Do "destructor" stuff here, because PostgreSQL instances are never destructed

    // Release the worker to the pool
    itsWorkingList[connectionId] = 0;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Shutdown connections
 */
// ----------------------------------------------------------------------

void PostgreSQLConnectionPool::shutdown()
{
  try
  {
    std::cout << "  -- Shutdown requested (PostgreSQLConnectionPool)\n";
    for (unsigned int i = 0; i < itsWorkerList.size(); i++)
    {
      auto sl = itsWorkerList[i].get();
      if (sl != nullptr)
        sl->shutdown();
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
