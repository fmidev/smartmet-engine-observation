// ======================================================================
/*!
 * \brief Interface of class ResultSet
 */
// ======================================================================

#pragma once

#include <spine/TimeSeries.h>
#include <spine/TimeSeriesGenerator.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class ResultSet
{
 public:
  using ResultSetRow = std::map<std::string, SmartMet::Spine::TimeSeries::Value>;
  using ResultSetRows = std::vector<ResultSetRow>;
  using ResultSetByTimeStep = std::map<boost::local_time::local_date_time, ResultSetRows>;
  using ResultSetById = std::map<std::string, ResultSetByTimeStep>;

  ResultSet(const std::set<std::string> &params) : itsParameterNames(params) {}
  ResultSet(const std::vector<std::string> &params)
      : itsParameterNames(params.begin(), params.end())
  {
  }

  void addRow(const std::string &id,
              const boost::local_time::local_date_time &timestep,
              const ResultSetRow &row);
  const std::set<boost::local_time::local_date_time> &timesteps() const { return itsTimeSteps; }
  const std::set<std::string> &ids() const { return itsIds; }
  const ResultSetById &resultset() const { return itsResultSet; }
  ResultSetById getResultSet(
      const SmartMet::Spine::TimeSeriesGenerator::LocalTimeList &tlist) const;

 private:
  ResultSetById itsResultSet;
  std::set<boost::local_time::local_date_time> itsTimeSteps;
  std::set<std::string> itsIds;
  std::set<std::string> itsParameterNames;
};

std::ostream &operator<<(std::ostream &os, const ResultSet &rs);
std::ostream &operator<<(std::ostream &os, const ResultSet::ResultSetById &resultset);

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

// ======================================================================
