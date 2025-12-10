#include "MastQuery.h"
#include <macgyver/Exception.h>
#include <memory>
#include <string>
#include <unordered_map>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
MastQuery::~MastQuery() = default;

std::string MastQuery::getSQLStatement(const std::string &database /*= "oracle"*/) const
{
  try
  {
    std::string statement;
    if (m_select.empty() or m_from.empty())
      return statement;

    statement.append("SELECT ");
    if (not m_distinct.empty())
      statement.append(m_distinct).append(" ");
    statement.append(m_select);
    statement.append(" FROM").append(m_from);

    if (not m_where.empty())
      statement.append(" WHERE ").append(database == "oracle" ? m_where : m_wherePostgreSQL);

    if (not m_orderBy.empty())
      statement.append(" ORDER BY ").append(m_orderBy);

    return statement;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

std::shared_ptr<QueryResult> MastQuery::getQueryResultContainer()
{
  try
  {
    if (!m_queryResult)
    {
      if (m_selectSize > 0)
        m_queryResult = std::make_shared<QueryResult>(m_selectSize);
    }

    return m_queryResult;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void MastQuery::setQueryParams(const MastQueryParams *qParams)
{
  try
  {
    // In case of multiple calls.
    m_select.clear();
    m_from.clear();
    m_where.clear();
    m_wherePostgreSQL.clear();
    m_orderBy.clear();
    m_distinct.clear();

    if (qParams->isDistinct())
      m_distinct = "DISTINCT";

    // SELECT part of a SQL statement
    using FieldMapType = MastQueryParams::FieldMapType;
    const std::shared_ptr<FieldMapType> fv = qParams->getFieldMap();
    m_selectSize = fv->size();
    for (auto it = fv->cbegin(); it != fv->cend(); ++it)
    {
      if (it != fv->begin())
        m_select.append(",");
      // e.g. " table.column_name" where it->second is "table" and it->first is
      // "column_name"
      m_select.append(" ").append(it->second).append(".").append(it->first);
      using FieldAliasMapType = MastQueryParams::FieldAliasMapType;
      const std::shared_ptr<FieldAliasMapType> fam = qParams->getFieldAliasMap();
      const auto famIt = fam->find(it->first);
      if (famIt != fam->end() && !famIt->second.empty())
        m_select.append(" as ").append(famIt->second);
    }

    // WHERE part of the SQL statement
    using OperationMapType = MastQueryParams::OperationMapType;

    const std::shared_ptr<OperationMapType> om = qParams->getOperationMap();

    for (const auto &op : *om)
    {
      if (op.second.empty())
        continue;

      if (!m_where.empty())
      {
        m_where.append(" and ");
        m_wherePostgreSQL.append(" and ");
      }

      m_where.append("(");
      m_wherePostgreSQL.append("(");

      for (auto it = op.second.begin(); it != op.second.end(); ++it)
      {
        if (it != op.second.begin())
        {
          m_where.append(" or ");
          m_wherePostgreSQL.append(" or ");
        }

        // e.g. " table.column_name = '60'" where it->second is "table" and
        // it->first is
        // "column_name
        // = '60'".
        // m_where.append((*it).second).append(".").append((*it).first->getExpression());
        m_where.append((*it).first->getExpression((*it).second));
        m_wherePostgreSQL.append((*it).first->getExpression((*it).second, "postgresql"));
      }

      m_where.append(")");
      m_wherePostgreSQL.append(")");
    }

    // FROM part of the SQL statement
    using JoinOnListTupleVectorType = MastQueryParams::JoinOnListTupleVectorType;
    const std::shared_ptr<JoinOnListTupleVectorType> joinOnListTupleVector =
        qParams->getJoinOnListTupleVector();
    m_from.append(" ").append(qParams->getTableName()).append(" ").append(qParams->getTableName());

    if (!joinOnListTupleVector->empty())
    {
      auto joinOnIt = joinOnListTupleVector->cbegin();
      for (; joinOnIt != joinOnListTupleVector->cend(); ++joinOnIt)
      {
        m_from.append(" ")
            .append(std::get<3>(*joinOnIt))
            .append(" ")
            .append(std::get<1>(*joinOnIt))
            .append(" ")
            .append(std::get<1>(*joinOnIt));

        for (auto joinField = std::get<2>(*joinOnIt).cbegin();
             joinField != std::get<2>(*joinOnIt).cend();
             joinField++)
        {
          m_from.append(joinField == std::get<2>(*joinOnIt).begin() ? " ON " : " AND ")
              .append(std::get<0>(*joinOnIt))
              .append(".")
              .append(*joinField)
              .append(" = ")
              .append(std::get<1>(*joinOnIt))
              .append(".")
              .append(*joinField);
        }
      }
    }

    // ORDER BY part of the SQL statement
    using OrderByVectorType = MastQueryParams::OrderByVectorType;
    const std::shared_ptr<OrderByVectorType> orderVector = qParams->getOrderByVector();
    for (auto it = orderVector->cbegin(); it != orderVector->cend(); ++it)
    {
      if (it != orderVector->cbegin())
        m_orderBy.append(", ");
      m_orderBy.append(it->first).append(" ").append(it->second);
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
