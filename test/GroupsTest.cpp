#define CATCH_CONFIG_MAIN

#if __cplusplus >= 201402L
#include <catch2/catch.hpp>
#else
#include <catch/catch.hpp>
#endif

#include "ProducerGroups.h"
#include "StationGroups.h"
#include "StationtypeConfig.h"
#include <macgyver/DateTime.h>

using namespace SmartMet::Engine::Observation;

namespace
{
Fmi::DateTime ymd(int year, int month, int day)
{
  return {Fmi::Date(year, month, day), Fmi::Hours(0)};
}
}  // namespace

TEST_CASE("Station groups")
{
  StationGroups groups;
  groups.addGroupPeriod(101004, "AWS", ymd(2000, 1, 1), ymd(2020, 1, 1));
  groups.addGroupPeriod(101004, "SYNOP", ymd(2010, 1, 1), ymd(2030, 1, 1));
  groups.addGroupPeriod(100971, "AWS", ymd(1990, 1, 1), ymd(1995, 1, 1));

  REQUIRE(groups.getStations() == std::set<int>{100971, 101004});
  REQUIRE(groups.getStationGroups(101004) == std::set<std::string>{"AWS", "SYNOP"});
  REQUIRE(groups.getStationGroups(1).empty());

  REQUIRE(groups.groupOK(101004, "AWS"));
  REQUIRE_FALSE(groups.groupOK(101004, "BUOY"));
  REQUIRE_FALSE(groups.groupOK(1, "AWS"));

  // Overlapping and non-overlapping periods
  REQUIRE(groups.belongsToGroup(101004, ymd(2013, 8, 5), ymd(2013, 8, 6)));
  REQUIRE(groups.belongsToGroup(100971, ymd(1994, 1, 1), ymd(2013, 1, 1)));
  REQUIRE_FALSE(groups.belongsToGroup(100971, ymd(2000, 1, 1), ymd(2013, 1, 1)));
  REQUIRE_FALSE(groups.belongsToGroup(1, ymd(2000, 1, 1), ymd(2013, 1, 1)));

  // A single instant inside a period
  REQUIRE(groups.belongsToGroup(101004, ymd(2013, 8, 5), ymd(2013, 8, 5)));
}

TEST_CASE("Producer groups")
{
  ProducerGroups groups;
  groups.addGroupPeriod("observations_fmi", 1, ymd(2000, 1, 1), ymd(2030, 1, 1));
  groups.addGroupPeriod("observations_fmi", 2, ymd(2000, 1, 1), ymd(2010, 1, 1));
  groups.addGroupPeriod("observations_fmi", 2, ymd(2015, 1, 1), ymd(2030, 1, 1));
  groups.addGroupPeriod("road", 3, ymd(2000, 1, 1), ymd(2030, 1, 1));

  REQUIRE(groups.getProducerGroups() == std::set<std::string>{"observations_fmi", "road"});

  SECTION("Only producers active during the period are returned")
  {
    REQUIRE(groups.getProducerIds("observations_fmi", ymd(2013, 8, 5), ymd(2013, 8, 6)) ==
            std::set<unsigned int>{1});
    REQUIRE(groups.getProducerIds("observations_fmi", ymd(2009, 1, 1), ymd(2016, 1, 1)) ==
            std::set<unsigned int>{1, 2});
    REQUIRE(groups.getProducerIds("observations_fmi", ymd(2020, 1, 1), ymd(2020, 1, 1)) ==
            std::set<unsigned int>{1, 2});
    REQUIRE(groups.getProducerIdsString("observations_fmi", ymd(2009, 1, 1), ymd(2016, 1, 1)) ==
            std::set<std::string>{"1", "2"});
    REQUIRE(groups.getProducerIds("nosuchgroup", ymd(2009, 1, 1), ymd(2016, 1, 1)).empty());
  }

  SECTION("Groups can be aliased")
  {
    groups.replaceProducerIds("observations_fmi", "fmi");
    REQUIRE(groups.getProducerIds("fmi", ymd(2013, 8, 5), ymd(2013, 8, 6)) ==
            std::set<unsigned int>{1});
    // Aliasing a missing group does nothing
    groups.replaceProducerIds("nosuchgroup", "other");
    REQUIRE(groups.getProducerGroups().count("other") == 0);
  }
}

TEST_CASE("Station type configuration")
{
  StationtypeConfig config;
  config.addStationtype("opendata", {"AWS", "SYNOP"});
  config.setDatabaseTableName("opendata", "observation_data");
  config.setUseCommonQueryMethod("opendata", true);
  config.setProducerIds("opendata", {1, 2});
  config.addStationtype("road", {"EXTRWS"});

  REQUIRE(config.hasGroupCodes("opendata"));
  REQUIRE_FALSE(config.hasGroupCodes("nosuchtype"));

  auto codes = config.getGroupCodeSetByStationtype("opendata");
  REQUIRE(codes);
  REQUIRE(codes->count("AWS") == 1);
  REQUIRE(codes->count("SYNOP") == 1);

  REQUIRE(config.getDatabaseTableNameByStationtype("opendata") == "observation_data");
  REQUIRE(config.getUseCommonQueryMethod("opendata"));
  REQUIRE_FALSE(config.getUseCommonQueryMethod("road"));

  REQUIRE(config.hasProducerIds("opendata"));
  REQUIRE_FALSE(config.hasProducerIds("road"));
  REQUIRE(config.getProducerIdSetByStationtype("opendata") == std::set<unsigned int>{1, 2});
}
