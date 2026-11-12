#include "MeasurandPeriods.h"
#include "Settings.h"
#include <fmt/format.h>
#include <macgyver/AtomicSharedPtr.h>
#include <macgyver/DateTime.h>
#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>
#include <macgyver/TimeParser.h>
#include <timeseries/RequestLimits.h>
#include <algorithm>
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
namespace MeasurandPeriods
{
namespace
{
using PeriodMap = std::unordered_map<int, long>;  // measurand_id -> minutes

Fmi::AtomicSharedPtr<PeriodMap>& periods()
{
  static Fmi::AtomicSharedPtr<PeriodMap> ptr;
  return ptr;
}

// Parse an aggregate period such as PT1M, PT10M, PT1H, P1D, P1M or P30Y into minutes.
// Returns 0 for unknown or unparseable periods.
long parse_minutes(const std::string& period)
{
  if (period.empty() || period.front() != 'P')
    return 0;
  try
  {
    const auto duration = Fmi::TimeParser::try_parse_iso_duration(period);
    if (duration.is_not_a_date_time() || duration.is_special())
      return 0;
    const auto seconds = duration.total_seconds();
    if (seconds <= 0)
      return 0;
    // Sub-minute periods are counted as one minute
    return std::max<long>(1, static_cast<long>(seconds / 60));
  }
  catch (...)
  {
    return 0;
  }
}

std::size_t saturating_add(std::size_t a, std::size_t b)
{
  return (a > std::numeric_limits<std::size_t>::max() - b) ? std::numeric_limits<std::size_t>::max()
                                                           : a + b;
}

std::size_t saturating_mul(std::size_t a, std::size_t b)
{
  if (a == 0 || b == 0)
    return 0;
  return (a > std::numeric_limits<std::size_t>::max() / b) ? std::numeric_limits<std::size_t>::max()
                                                           : a * b;
}

}  // namespace

void set(const MeasurandInfo& info)
{
  try
  {
    auto newmap = std::make_shared<PeriodMap>();
    for (const auto& item : info)
    {
      const auto& mi = item.second;
      long id = 0;
      try
      {
        id = Fmi::stol(mi.measurand_id);
      }
      catch (...)
      {
        continue;  // QC measurands are identified by name, not by number
      }
      const auto minutes = parse_minutes(mi.aggregate_period);
      if (minutes > 0)
        (*newmap)[static_cast<int>(id)] = minutes;
    }
    periods().store(newmap);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Failed to set measurand periods");
  }
}

long minutes(int measurand_id)
{
  const auto map = periods().load();
  if (!map)
    return 0;
  auto pos = map->find(measurand_id);
  return (pos == map->end() ? 0 : pos->second);
}

std::size_t estimate_rows(const Settings& settings,
                          const std::vector<int>& measurand_ids,
                          std::size_t nstations)
{
  try
  {
    if (nstations == 0 || measurand_ids.empty() || settings.starttime.is_special() ||
        settings.endtime.is_special() || settings.endtime < settings.starttime)
      return 0;

    const auto map = periods().load();
    if (!map)
      return 0;

    const auto span = (settings.endtime - settings.starttime).total_seconds() / 60;

    std::size_t per_station = 0;
    for (const auto id : measurand_ids)
    {
      auto pos = map->find(id);
      if (pos == map->end())
        continue;
      per_station = saturating_add(per_station, static_cast<std::size_t>(span / pos->second) + 1);
    }
    return saturating_mul(per_station, nstations);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Failed to estimate the number of observations");
  }
}

void check_request_limits(const Settings& settings,
                          const std::vector<int>& measurand_ids,
                          std::size_t nstations)
{
  const auto maxelements = settings.requestLimits.maxelements;
  if (maxelements == 0)
    return;

  const auto estimate = estimate_rows(settings, measurand_ids, nstations);
  if (estimate <= maxelements)
    return;

  throw Fmi::Exception(BCP, "RequestLimitError")
      .addParameter("description",
                    fmt::format("Too many elements: the request would read about {} observations "
                                "from the database, max {} allowed!",
                                estimate,
                                maxelements))
      .addParameter("stations", Fmi::to_string(nstations))
      .addParameter("starttime", Fmi::to_iso_string(settings.starttime))
      .addParameter("endtime", Fmi::to_iso_string(settings.endtime))
      .disableLogging();
}

}  // namespace MeasurandPeriods
}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
