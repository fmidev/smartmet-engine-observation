#define CATCH_CONFIG_MAIN

#if __cplusplus >= 201402L
#include <catch2/catch.hpp>
#else
#include <catch/catch.hpp>
#endif

#include "EngineParameters.h"
#include "SpatiaLite.h"
#include "SpatiaLiteCacheParameters.h"
#include <spine/ConfigBase.h>
#include <memory>

using namespace SmartMet::Engine::Observation;

namespace
{
// The packaged test cache, opened read-only since it is not writable
const std::string cacheFile = "/usr/share/smartmet/test/data/sqlite/stations.sqlite.2";

SpatiaLite& database()
{
  static SmartMet::Spine::ConfigBase config("cnf/sensors.conf");
  static auto parameters = std::make_shared<EngineParameters>(config);
  static SpatiaLiteCacheParameters options(parameters);
  static SpatiaLite db(cacheFile, options);
  return db;
}

SensorOptions period()
{
  SensorOptions options;
  options.producer = "mast";
  options.fmisids = {101062};
  options.starttime = Fmi::DateTime::from_string("2013-08-05 00:00:00");
  options.endtime = Fmi::DateTime::from_string("2013-08-06 00:00:00");
  return options;
}

// The sensors of a measurand as "sensor:default" strings in order
std::vector<std::string> sensors(const std::vector<SensorInfo>& found, int measurand)
{
  std::vector<std::string> ret;
  for (const auto& s : found)
    if (s.measurand_id == measurand)
      ret.push_back(std::to_string(s.sensor_no) + (s.is_default ? ":default" : ""));
  return ret;
}
}  // namespace

TEST_CASE("Sensors of a station in the cached observations")
{
  const auto found = database().getSensors(period(), {1, 41}, {});

  // TA (1) has a default sensor, WS (41) only additional ones at this mast
  REQUIRE(sensors(found, 1) == std::vector<std::string>{"1:default", "23", "63", "106"});
  REQUIRE(sensors(found, 41) == std::vector<std::string>{"21", "23", "61", "63", "104", "106"});

  for (const auto& s : found)
  {
    REQUIRE(s.fmisid == 101062);
    REQUIRE(s.values == 2);
    REQUIRE(Fmi::to_iso_string(s.first_data) == "20130805T145000");
    REQUIRE(Fmi::to_iso_string(s.last_data) == "20130805T150000");
  }
}

TEST_CASE("Sensors outside the period of the data")
{
  auto options = period();
  options.starttime = Fmi::DateTime::from_string("2013-08-07 00:00:00");
  options.endtime = Fmi::DateTime::from_string("2013-08-08 00:00:00");
  REQUIRE(database().getSensors(options, {1}, {}).empty());
}

TEST_CASE("Sensors of an unknown station")
{
  auto options = period();
  options.fmisids = {1};
  REQUIRE(database().getSensors(options, {}, {}).empty());
}
