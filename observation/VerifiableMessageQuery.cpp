#include "VerifiableMessageQuery.h"
#include <spine/Exception.h>
#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
VerifiableMessageQuery::VerifiableMessageQuery()
{
  m_returnOnlyLatest = false;
}

VerifiableMessageQuery::~VerifiableMessageQuery() {}

std::string VerifiableMessageQuery::getSQLStatement() const
{
  try
  {
    std::string select;
    for (unsigned int i = 0; i < m_select.size(); ++i)
    {
      select = select + m_select.at(i);
      if (i < (m_select.size() - 1))
        select = select + ", ";
    }

    std::string queryStatement;

    if (m_returnOnlyLatest)
    {
      // Checking that there is at least one station selected.
      if (m_stationIDs.size() == 0)
      {
        std::cerr << "warning: "
                     "SmartMet::Engine::Observation::VerifiableMessageQuery::"
                     "getSQLStatement - "
                     "Trying to form SQL statement without station_id.\n";
        return "";
      }

      queryStatement.append("SELECT * FROM (");

      // Make union from all the select statements. Each of the statements are
      // identical but the
      // station_id differs.
      for (std::vector<std::string>::const_iterator it = m_stationIDs.begin();
           it != m_stationIDs.end();
           ++it)
      {
        queryStatement.append(" (SELECT * FROM (")
            .append(" SELECT ")
            .append(select)
            .append(" FROM ")
            .append(m_from)
            .append(" WHERE ")
            .append(m_where)
            .append(" and data.station_id = '")
            .append(*it)
            .append("'")
            .append(" ORDER BY ")
            .append(m_orderBy)
            .append(") WHERE ROWNUM = 1)");

        if (it + 1 != m_stationIDs.end())
          queryStatement.append(" UNION ALL");
      }

      queryStatement.append(") ORDER BY STATION_ID ASC");
    }
    else
    {
      queryStatement =
          "SELECT " + select + " FROM " + m_from + " WHERE " + m_where + " ORDER BY " + m_orderBy;
    }

    return queryStatement;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

std::shared_ptr<QueryResult> VerifiableMessageQuery::getQueryResultContainer()
{
  try
  {
    if (not m_queryResult)
    {
      if (m_select.size() > 0)
        m_queryResult.reset(new QueryResult(m_select.size()));
    }

    return m_queryResult;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

void VerifiableMessageQuery::setQueryParams(const VerifiableMessageQueryParams *qParams)
{
  try
  {
    // In case of multiple calls.
    m_select.clear();
    m_from.clear();
    m_where.clear();
    m_orderBy.clear();

    bool orderByMessageTime = false;
    bool orderByStationId = false;
    m_returnOnlyLatest =
        qParams->isRestriction(VerifiableMessageQueryParams::Restriction::RETURN_ONLY_LATEST);

    const VerifiableMessageQueryParams::SelectNameListType *selectNames =
        qParams->getSelectNameList();
    if (selectNames->empty())
      throw Spine::Exception(BCP, "Invalid SQL statement: Empty select name list");

    for (VerifiableMessageQueryParams::SelectNameListType::const_iterator it = selectNames->begin();
         it != selectNames->end();
         ++it)
    {
      std::string sname = "data." + (*it);
      std::string fieldMethod = qParams->getSelectNameMethod(*it);
      if (not fieldMethod.empty())
        sname.append(".").append(fieldMethod);
      sname.append(" as ").append(*it);
      m_select.push_back(sname);

      if ((*it) == "MESSAGE_TIME" or m_returnOnlyLatest)
        orderByMessageTime = true;
      if ((*it) == "STATION_ID")
        orderByStationId = true;
    }

    m_from = qParams->getTableName();
    if (m_from.empty())
      throw Spine::Exception(BCP, "Invalid SQL statement: Empty table name");

    m_from.append(" data");

    Engine::Observation::VerifiableMessageQueryParams::StationIdVectorType *icaoCodes =
        qParams->getStationIdVector();

    if (icaoCodes->empty())
      throw Spine::Exception(BCP, "Empty location list");

    if (m_returnOnlyLatest)
    {  // Station_id list is used in getSQLStatement
       // method.
      m_stationIDs.resize(icaoCodes->size());
      std::copy(icaoCodes->begin(), icaoCodes->end(), m_stationIDs.begin());
    }
    else
    {
      m_where.append("(");
      Engine::Observation::VerifiableMessageQueryParams::StationIdVectorType::iterator icaoIT;
      for (icaoIT = icaoCodes->begin(); icaoIT != icaoCodes->end(); icaoIT++)
      {
        m_where.append("data.station_id = '").append(*icaoIT).append("'");
        if (std::next(icaoIT) != icaoCodes->end())
          m_where.append(" or ");
      }
      m_where.append(")");
    }

    const std::string beginTime = qParams->getBeginTime();
    const std::string endTime = qParams->getEndTime();

    if (not m_returnOnlyLatest)
      m_where.append(" and ");
    // type 1 = METAR and type 2 = METAR COR
    m_where.append("(data.message_type = 1 or data.message_type = 2) and ")
        .append("data.iwxxm_status is NULL and ")
        .append("(data.iwxxm_errcode is NULL or data.iwxxm_errcode = 0) and ")
        .append("data.iwxxm_content is not NULL and ")
        .append("data.message_time >= TO_DATE('")
        .append(beginTime)
        .append("','YYYY-MM-DD HH24:MI:SS') and ")
        .append("data.message_time <= TO_DATE('")
        .append(endTime)
        .append("','YYYY-MM-DD HH24:MI:SS')");

    size_t orderByLength = m_orderBy.size();

    // At first order by message_time and message_type. There might be
    // corrections
    // CCA and CCB with the same timestamp.
    if (m_returnOnlyLatest)
      m_orderBy.append(" data.message_time DESC, data.message_type DESC");
    else if (orderByMessageTime)
      m_orderBy.append(" data.message_time ASC");

    // then order by station_id if those are requested.
    if (orderByStationId)
    {
      if (m_orderBy.size() > orderByLength)
        m_orderBy.append(",");
      m_orderBy.append(" data.station_id ASC");
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
