#include "PostgreSQLObsDBConnectionPool.h"
#include <macgyver/Exception.h>

#include <boost/foreach.hpp>

using namespace std;

namespace
{
template <class T>
struct Releaser
{
  Releaser(SmartMet::Engine::Observation::PostgreSQLObsDBConnectionPool* pool_handle) : poolHandle(pool_handle) {}
  void operator()(T* t) { poolHandle->releaseConnection(t->connectionId()); }
  SmartMet::Engine::Observation::PostgreSQLObsDBConnectionPool* poolHandle;
};
}  // namespace

namespace SmartMet
{
namespace Engine
{
namespace Observation
{

PostgreSQLObsDBConnectionPool::PostgreSQLObsDBConnectionPool(PostgreSQLDatabaseDriver* driver) {}

bool PostgreSQLObsDBConnectionPool::addService(
    const Fmi::Database::PostgreSQLConnectionOptions& connectionOptions, int poolSize)
{
  try
  {
    itsConnectionOptions.push_back(connectionOptions);

    itsServicePool.push_back(static_cast<unsigned>(poolSize));
    itsPoolSize += static_cast<unsigned>(poolSize);
    itsWorkingList.resize(itsPoolSize, -1);
    itsWorkerList.resize(itsPoolSize);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
  return true;
}

bool PostgreSQLObsDBConnectionPool::initializePool(
    const StationtypeConfig& stc,
    const ParameterMapPtr& pm)
{
  try
  {
    unsigned int filled = 0;
    unsigned long tofill = itsServicePool[filled];
    for (unsigned int i = 0; i < itsWorkingList.size(); i++)
    {
      try
      {
        // Logon here

        itsWorkerList[i] =
            boost::shared_ptr<PostgreSQLObsDB>(new PostgreSQLObsDB(itsConnectionOptions[filled], stc, pm));
        itsWorkingList[i] = 0;
        itsWorkerList[i]->setConnectionId(static_cast<signed>(i));
        if (i == tofill)
        {
          filled++;
          if (filled < itsServicePool.size())
          {
            tofill += itsServicePool[filled];
          }
        }
      }
      catch (std::exception& err)
      {
        cerr << "PostgreSQLObsDBConnectionPool::initializePool: " << err.what() << endl;
        return false;
      }
      catch (...)
      {
        cerr << "PostgreSQLObsDBConnectionPool::initializePool: Unknown error." << endl;
        return false;
      }
    }

    // Everything is OK
    return true;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

boost::shared_ptr<PostgreSQLObsDB> PostgreSQLObsDBConnectionPool::getConnection(bool debug /*= false*/)
{
  try
  {
    /*
     *  1 --> active
     *  0 --> inactive
     *
     * Logic of returning connections:
     *
     * 1. Check if worker is idle, if so return that worker.
     * 2. Sleep and start over
     */
    size_t countTimeOut = 0;
    while (true)
    {
      // Local scope to minimize lock life time
      {
        SmartMet::Spine::WriteLock lock(itsGetMutex);
        for (unsigned int i = 0; i < itsWorkingList.size(); i++)
        {
          if (itsWorkingList[i] == 0)
          {
            itsWorkingList[i] = 1;
            // itsWorkerList[i]->attach();
            // itsWorkerList[i]->beginSession(tryNumber); // this is very slow
            itsWorkerList[i]->setConnectionId(static_cast<signed>(i));
            itsWorkerList[i]->setDebug(debug);
            return boost::shared_ptr<PostgreSQLObsDB>(itsWorkerList[i].get(),
                                                 Releaser<PostgreSQLObsDB>(this));
          }
        }
      }

      // Fail after timeout seconds is reached.
      if (++countTimeOut > itsGetConnectionTimeOutSeconds)
      {
        throw Fmi::Exception(
            BCP, "Could not get a database connection. All the database connections are in use!");
      }
      else
      {
        // The timeout counter above assumes the sleep time here is one second. Should be rewritten.
        boost::this_thread::sleep(boost::posix_time::milliseconds(1000));
      }
    }

    // NEVER EXECUTED:
    // throw Fmi::Exception(BCP,"[Observation] Could not get a connection in
    // PostgreSQLObsDBConnectionPool::getConnection()");
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting PostgreSQL connection failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Shutdown connections
 */
// ----------------------------------------------------------------------

void PostgreSQLObsDBConnectionPool::shutdown()
{
  try
  {
    std::cout << "  -- Shutdown requested (PostgreSQLObsDBConnectionPool)\n";
    for (unsigned int i = 0; i < itsWorkerList.size(); i++)
    {
      auto sl = itsWorkerList[i].get();
      if (sl != nullptr) sl->shutdown();
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void PostgreSQLObsDBConnectionPool::releaseConnection(int connectionId)
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
    itsWorkingList[static_cast<unsigned>(connectionId)] = 0;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void PostgreSQLObsDBConnectionPool::setGetConnectionTimeOutSeconds(const size_t seconds)
{
  try
  {
    itsGetConnectionTimeOutSeconds = seconds;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet