// ======================================================================
/*!
 * \brief Implementation of class ResultSet
 *
 * A class to store database resultset temporarily. One row in timeseries output
 * corresponds to several rows in database resultset. At first resultset is stored
 * as such row by row. When all rows in Oracle resultset has been read and stored
 * getResultSet()-function can called to get the final resultset by Id and timestep
 *
 */
// ======================================================================

#include "ResultSet.h"
#include "spine/TimeSeriesOutput.h"
#include <boost/date_time/posix_time/posix_time.hpp>

namespace ts = SmartMet::Spine::TimeSeries;

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
// ----------------------------------------------------------------------
/*!
 * \brief Store a database resultset row in itsResultSet data structure
 */
// ----------------------------------------------------------------------
void ResultSet::addRow(const std::string &id,
                       const boost::local_time::local_date_time &timestep,
                       const ResultSetRow &row)
{
  std::string dataParameterNames;
  for (auto item : row)
  {
    if (item.second != ts::Value(ts::None()) &&
        itsParameterNames.find(item.first) != itsParameterNames.end())
    {
      if (!dataParameterNames.empty())
        dataParameterNames += ",";
      dataParameterNames += item.first;
    }
  }

  if (itsResultSet.find(id) == itsResultSet.end())
  {
    ResultSetByTimeStep rsTS;
    rsTS.insert(std::make_pair(timestep, ResultSetRows()));
    rsTS[timestep].push_back(row);
    rsTS[timestep].back().insert(make_pair("DATA_PARAMETER_NAMES", ts::Value(dataParameterNames)));
    itsResultSet.insert(std::make_pair(id, rsTS));
    itsTimeSteps.insert(timestep);
    itsIds.insert(id);
    return;
  }
  ResultSetByTimeStep &rsTS = itsResultSet.at(id);
  if (rsTS.find(timestep) == rsTS.end())
  {
    rsTS.insert(std::make_pair(timestep, ResultSetRows()));
    rsTS[timestep].push_back(row);
    rsTS[timestep].back().insert(make_pair("DATA_PARAMETER_NAMES", ts::Value(dataParameterNames)));
    itsTimeSteps.insert(timestep);
    return;
  }
  ResultSetRows &rsRows = rsTS.at(timestep);
  rsRows.push_back(row);
  rsRows.back().insert(make_pair("DATA_PARAMETER_NAMES", ts::Value(dataParameterNames)));
}

// ----------------------------------------------------------------------
/*!
 * \brief Returns the resultset by Id and timestep
 */
// ----------------------------------------------------------------------

ResultSet::ResultSetById ResultSet::getResultSet(
    const SmartMet::Spine::TimeSeriesGenerator::LocalTimeList &tlist) const
{
  ResultSet::ResultSetById resultSet;

  // Create empty resultset for each Id
  for (auto id : itsIds)
    for (auto timestep : itsTimeSteps)
    {
      ResultSetByTimeStep rsTS;
      rsTS.insert(std::make_pair(timestep, ResultSetRows()));
      rsTS[timestep].push_back(ResultSetRow());
      resultSet.insert(std::make_pair(id, rsTS));
    }

  Spine::ValueFormatter formatter((Spine::ValueFormatterParam()));

  // Iterate Ids
  for (auto id : itsIds)
  {
    const ResultSetByTimeStep &rsTS = itsResultSet.at(id);
    // Iterate timesteps inside each Id
    for (auto rsrs : rsTS)
    {
      const boost::local_time::local_date_time &timestep = rsrs.first;
      // Iterate resultset rows
      for (auto rsr : rsrs.second)
      {
        std::string dataParameterNames = boost::get<std::string>(rsr.at("DATA_PARAMETER_NAMES"));
        std::vector<std::string> dataParams;
        boost::split(dataParams, dataParameterNames, boost::is_any_of(","));

        // Go thru the input resultset row field by field and add them
        // to output resultset so that all fields with the same Id and timestep
        // go to the same output row
        for (auto field : rsr)
        {
          if (resultSet[id][timestep].empty())
            resultSet[id][timestep].push_back(ResultSetRow());
          ResultSetRow &row = resultSet[id][timestep].back();

          std::string fieldname = field.first;
          ts::Value value = field.second;

          if (fieldname == "DATA_SOURCE")
          {
            for (auto p : dataParams)
            {
              fieldname = p + "_" + field.first;
              if (row.find(fieldname) == row.end())
                row.insert(std::make_pair(fieldname, value));
              else if (value != ts::Value(ts::None()))
                row[fieldname] = value;
            }
          }
          else
          {
            if (row.find(fieldname) == row.end())
              row.insert(std::make_pair(fieldname, value));
            else if (value != ts::Value(ts::None()))
              row[fieldname] = value;
          }
        }
      }
    }
  }

  // Go thru the requested timesteps and compose the final resultset
  ResultSet::ResultSetById ret;
  for (auto rsTS : resultSet)
  {
    std::string id = rsTS.first;
    auto timeIterator = tlist.begin();
    for (auto rsrs : rsTS.second)
    {
      if (rsrs.second[0].size() == 0)
        continue;

      // All timesteps
      if (tlist.size() == 0)
      {
        ret[id][rsrs.first].push_back(rsrs.second[0]);
        continue;
      }

      const boost::local_time::local_date_time &dataTimestep = rsrs.first;

      while (*timeIterator < dataTimestep && timeIterator != tlist.end())
      {
        // Add rows with missing data
        ResultSetRow rsr = rsrs.second[0];
        rsr["OBSTIME"] = *timeIterator;
        for (auto p : itsParameterNames)
        {
          if (rsr.find(p) != rsr.end())
            rsr[p] = ts::Value(ts::None());
        }
        ret[id][*timeIterator].push_back(rsr);
        timeIterator++;
      }

      if (dataTimestep == *timeIterator)
      {
        ret[id][*timeIterator].push_back(rsrs.second[0]);
        timeIterator++;
      }
    }
  }

  return ret;
}

std::ostream &operator<<(std::ostream &os, const ResultSet &rs)
{
  const std::set<std::string> &ids = rs.ids();

  if (ids.empty())
    return os;

  const std::set<boost::local_time::local_date_time> &timesteps = rs.timesteps();
  const ResultSet::ResultSetById &resultset = rs.resultset();

  unsigned int rowno = 0;
  for (auto id : ids)
  {
    if (resultset.find(id) == resultset.end())
    {
      os << "***** " << id << " NOT FOUND *****" << std::endl;
      continue;
    }
    os << "***** " << id << " *****" << std::endl;
    const ResultSet::ResultSetByTimeStep &rsTS = resultset.at(id);
    for (auto timestep : timesteps)
    {
      if (rsTS.find(timestep) == rsTS.end())
      {
        os << "** Timestep: " << timestep << " NOT FOUND **" << std::endl;
        continue;
      }
      os << "** Timestep: " << timestep << std::endl;
      const ResultSet::ResultSetRows &rsrs = rsTS.at(timestep);
      for (auto row : rsrs)
      {
        os << "** Rowno: " << rowno++ << std::endl;
        for (auto item : row)
        {
          os << item.first << " -> " << item.second << std::endl;
        }
      }
    }
  }
  return os;
}

std::ostream &operator<<(std::ostream &os, const ResultSet::ResultSetById &resultset)
{
  if (resultset.empty())
    return os;

  unsigned int rowno = 0;
  for (const auto &rsTS : resultset)
  {
    os << "***** " << rsTS.first << " *****" << std::endl;
    for (const auto &rsrs : rsTS.second)
    {
      os << "** Timestep: " << rsrs.first << std::endl;
      for (const auto &rsr : rsrs.second)
      {
        os << "** Rowno: " << rowno++ << std::endl;
        for (const auto &item : rsr)
        {
          os << item.first << " -> " << item.second << std::endl;
        }
      }
    }
  }
  return os;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
