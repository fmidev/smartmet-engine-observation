#define CATCH_CONFIG_MAIN

#if __cplusplus >= 201402L
#include <catch2/catch.hpp>
#else
#include <catch/catch.hpp>
#endif

#include "../observation/MeasurandInfo.h"
#include "../observation/MeasurandPeriods.h"
#include "../observation/Settings.h"
#include "MeasurandPeriods.h"
#include <macgyver/TimeParser.h>
#include <timeseries/RequestLimits.h>
#include <cstddef>
#include <string>
#include <vector>

using namespace SmartMet::Engine::Observation;

namespace
{
measurand_info make_info(const std::string& id, const std::string& period)
{
  measurand_info mi;
  mi.measurand_id = id;
  mi.aggregate_period = period;
  return mi;
}

void init_periods()
{
  MeasurandInfo info;
  info["1"] = make_info("1", "PT1M");       // TA_PT1M_AVG
  info["11"] = make_info("11", "PT1H");     // TA_PT1H_AVG
  info["258"] = make_info("258", "PT10M");  // TA_PT10M_AVG
  info["24"] = make_info("24", "P1D");      // TA_P1D_AVG
  info["99"] = make_info("99", "PT1S");     // sub-minute
  info["98"] = make_info("98", "PTSM");     // garbage in the database
  info["97"] = make_info("97", "");
  info["TA"] = make_info("TA", "PT1H");  // QC measurands use names
  MeasurandPeriods::set(info);
}

Settings make_settings(const std::string& start, const std::string& end, std::size_t maxelements)
{
  Settings settings;
  settings.starttime = Fmi::TimeParser::parse(start);
  settings.endtime = Fmi::TimeParser::parse(end);
  settings.requestLimits.maxelements = maxelements;
  return settings;
}
}  // namespace

TEST_CASE("Nominal measurand periods")
{
  init_periods();
  CHECK(MeasurandPeriods::minutes(1) == 1);
  CHECK(MeasurandPeriods::minutes(11) == 60);
  CHECK(MeasurandPeriods::minutes(258) == 10);
  CHECK(MeasurandPeriods::minutes(24) == 1440);
  CHECK(MeasurandPeriods::minutes(99) == 1);
  CHECK(MeasurandPeriods::minutes(98) == 0);
  CHECK(MeasurandPeriods::minutes(97) == 0);
  CHECK(MeasurandPeriods::minutes(12345) == 0);
}

TEST_CASE("Estimated number of rows")
{
  init_periods();

  // One day: 1441 one minute values and 25 hourly values per station
  auto settings = make_settings("2025-01-01T00:00:00", "2025-01-02T00:00:00", 0);
  CHECK(MeasurandPeriods::estimate_rows(settings, {1}, 1) == 1441);
  CHECK(MeasurandPeriods::estimate_rows(settings, {11}, 1) == 25);
  CHECK(MeasurandPeriods::estimate_rows(settings, {1, 11}, 3) == 3 * (1441 + 25));
  // Unknown periods are not counted
  CHECK(MeasurandPeriods::estimate_rows(settings, {98, 12345}, 10) == 0);
  CHECK(MeasurandPeriods::estimate_rows(settings, {1}, 0) == 0);

  // Aggregation widens the period: mean_t(TA_PT1M_AVG/59m/0) for 2020-2024 hourly
  settings = make_settings("2019-12-31T23:01:00", "2025-01-01T00:00:00", 0);
  const auto estimate = MeasurandPeriods::estimate_rows(settings, {1}, 1);
  CHECK(estimate == 5 * 365 * 1440 + 2 * 1440 + 60);  // 2020 and 2024 are leap years
}

TEST_CASE("Element limit is checked before the query")
{
  init_periods();

  // Five years of one minute data is about 2.6 million rows per station
  auto settings = make_settings("2020-01-01T00:00:00", "2025-01-01T00:00:00", 1000000);
  CHECK_THROWS(MeasurandPeriods::check_request_limits(settings, {1}, 1));
  CHECK_NOTHROW(MeasurandPeriods::check_request_limits(settings, {11}, 1));

  // No limit, no check
  settings.requestLimits.maxelements = 0;
  CHECK_NOTHROW(MeasurandPeriods::check_request_limits(settings, {1}, 100));
}
