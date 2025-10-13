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

SpatiaLiteConnectionPool::SpatiaLiteConnectionPool(const SpatiaLiteCacheParameters& options)
try
    : itsSpatialiteFile(options.cacheFile)
    , itsOptions(options)
    , itsPool(options.connectionPoolSize, options.connectionPoolSize, itsSpatialiteFile, options)
{
  // In this case we could avoid creating copies of file and options for each connection
  // as pool will not be expanded later, but it is safer to make local copies for case if an
  // expansion will be added later
}
catch (...)
{
  throw Fmi::Exception::Trace(BCP, "Operation failed!");
}

SpatiaLiteConnectionPool::Ptr SpatiaLiteConnectionPool::getConnection()
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

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
