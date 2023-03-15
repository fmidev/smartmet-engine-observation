#pragma once

#include <boost/shared_ptr.hpp>
#include <memory>
#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class QueryResult;

class QueryBase
{
 public:
  QueryBase() = default;

  virtual ~QueryBase();

  /**
   *  @brief Get empty SQL statement.
   *  @return Empty SQL statement.
   */
  virtual std::string getSQLStatement(const std::string& database = "oracle") const { return ""; }
  /**
   *  @brief Get a reference with null value to a container to store data.
   *  @return Null pointer.
   */
  virtual std::shared_ptr<QueryResult> getQueryResultContainer() { return {}; }

 private:
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
