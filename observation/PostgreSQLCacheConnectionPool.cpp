#include "PostgreSQLCacheConnectionPool.h"
#include "PostgreSQLCacheParameters.h"
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

PostgreSQLCacheConnectionPool::PostgreSQLCacheConnectionPool(
    const PostgreSQLCacheParameters& options)
try
    : itsOptions(options)
    , itsPool(options.connectionPoolSize, options.connectionPoolSize, options)
{
}
catch (...)
{
  throw Fmi::Exception::Trace(BCP, "Operation failed!");
}

PostgreSQLCacheConnectionPool::Ptr PostgreSQLCacheConnectionPool::getConnection()
{
  try
  {
    return itsPool.get();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void PostgreSQLCacheConnectionPool::shutdown()
{
  // Remove me
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
