#ifndef MAST_QUERY_H
#define MAST_QUERY_H

#include "MastQueryParams.h"
#include "QueryBase.h"
#include "QueryParamsBase.h"
#include "QueryResult.h"
#include <map>
#include <string>
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
/**
 * @brief The class implements interface to fetch Mast data.
 *
 */
class MastQuery : public QueryBase
{
 public:
  explicit MastQuery();

  ~MastQuery();

  /**
   * @brief Get SQL statement constructed in the class.
   * @return SQL statement string of empty string when failure occur.
   */
  std::string getSQLStatement() const;

  /**
   * @brief Get reference to the result container of
   *        the class object to store or read data.
   * @return Reference to the result container or NULL if
   *         SQL statement produce an empty result.
   */
  std::shared_ptr<QueryResult> getQueryResultContainer();

  /**
   * \brief Set query params used in SQL statement formation.
   *        The result lines will be ordered by message_time (ascending order)
   *        and / or station_id respectively if the parameter requested.
   * @exception OPERATION_PROCESSING_FAILED
   *            If time format conversion to Oracle time format fail.
   * @exception INVALID_SQL_STATEMENT
   *            If SELECT parameter list is empty.
   * @exception MISSING_PARAMETER_VALUE
   *            If there is no any location to look for.
   */
  void setQueryParams(const MastQueryParams *qParams);

 private:
  MastQuery &operator=(const MastQuery &other);
  MastQuery(const MastQuery &other);

  // SQL statement parts contructed in setQueryParams method
  // used lated in getSQLStatement method.
  int m_selectSize;
  std::string m_select;
  std::string m_from;
  std::string m_where;
  std::string m_orderBy;
  std::string m_distinct;

  std::shared_ptr<QueryResult> m_queryResult;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

#endif  // ENVIRONMENTAL_MONITORING_FACILITY_QUERY_H
