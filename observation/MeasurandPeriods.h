#pragma once

#include "MeasurandInfo.h"
#include <cstddef>
#include <vector>

namespace SmartMet::Engine::Observation
{
class Settings;
}  // namespace SmartMet::Engine::Observation

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
// ----------------------------------------------------------------------
/*!
 * \brief Nominal data intervals of the measurands
 *
 * The nominal interval of a measurand is its aggregate period in measurands_v1,
 * for example PT1M for TA_PT1M_AVG. It is used to estimate the number of rows a
 * query reads from the database before the query is run: aggregated and subsampled
 * timeseries requests read every raw value in the period, which the requested
 * timestep alone does not reveal.
 *
 * The periods are published once the measurand info has been read from the
 * database. Until then, and for measurands with an unknown or unparseable period,
 * no estimate is made.
 */
// ----------------------------------------------------------------------

namespace MeasurandPeriods
{
// Publish the nominal periods of the measurands (measurand_id -> aggregate_period)
void set(const MeasurandInfo& info);

// Nominal interval in minutes, 0 if unknown. Sub-minute periods are rounded up to
// one minute, since the estimate must not exceed what the database can hold.
long minutes(int measurand_id);

// Estimated number of database rows: stations * sum over measurands of the values
// in the time interval. Measurands with an unknown period are not counted.
std::size_t estimate_rows(const Settings& settings,
                          const std::vector<int>& measurand_ids,
                          std::size_t nstations);

// Throw if the estimate exceeds the element limit of the request
void check_request_limits(const Settings& settings,
                          const std::vector<int>& measurand_ids,
                          std::size_t nstations);

}  // namespace MeasurandPeriods
}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
