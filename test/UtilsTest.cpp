#define CATCH_CONFIG_MAIN

#if __cplusplus >= 201402L
#include <catch2/catch.hpp>
#else
#include <catch/catch.hpp>
#endif

#include "RoadAndForeignIds.h"
#include "Utils.h"
#include <macgyver/DateTime.h>
#include <macgyver/TimeZones.h>

using namespace SmartMet::Engine::Observation;
using namespace SmartMet::Engine::Observation::Utils;

TEST_CASE("Parameter name parsing")
{
  SECTION("removePrefix")
  {
    std::string name = "qc_TA_PT1H_AVG";
    REQUIRE(removePrefix(name, "qc_"));
    REQUIRE(name == "TA_PT1H_AVG");
    REQUIRE_FALSE(removePrefix(name, "qc_"));

    // The prefix alone is not removed
    std::string prefix = "qc_";
    REQUIRE_FALSE(removePrefix(prefix, "qc_"));
    REQUIRE(prefix == "qc_");
  }

  SECTION("trimCommasFromEnd")
  {
    REQUIRE(trimCommasFromEnd("1,2,3,,,") == "1,2,3");
    REQUIRE(trimCommasFromEnd("1,2,3") == "1,2,3");
    REQUIRE(trimCommasFromEnd(",,,") == "");
    REQUIRE(trimCommasFromEnd("") == "");
  }

  SECTION("parseParameterName strips the qc prefix and the sensor number")
  {
    REQUIRE(parseParameterName("Temperature") == "temperature");
    REQUIRE(parseParameterName("KELI_2") == "keli");
    REQUIRE(parseParameterName("qc_KELI_2") == "keli");
    // The last part is not a number, hence it is not a sensor number
    REQUIRE(parseParameterName("TA_PT1H_AVG") == "ta_pt1h_avg");
    REQUIRE(parseParameterName("TRS_10MIN_DIF") == "trs_10min_dif");
    REQUIRE(parseParameterName("TRS_10MIN_DIF_1") == "trs_10min_dif");
  }

  SECTION("parseSensorNumber defaults to 1")
  {
    REQUIRE(parseSensorNumber("KELI_2") == 2);
    REQUIRE(parseSensorNumber("KELI") == 1);
    REQUIRE(parseSensorNumber("TA_PT1H_AVG") == 1);
    REQUIRE(parseSensorNumber("TRS_10MIN_DIF_3") == 3);
  }
}

TEST_CASE("Day limits")
{
  const Fmi::DateTime t(Fmi::Date(2013, 8, 5), Fmi::Hours(13) + Fmi::Minutes(45));
  REQUIRE(day_start(t) == Fmi::DateTime(Fmi::Date(2013, 8, 5), Fmi::Hours(0)));
  REQUIRE(day_end(t) == Fmi::DateTime(Fmi::Date(2013, 8, 6), Fmi::Hours(0)));

  // Midnight is the start of its own day
  const Fmi::DateTime midnight(Fmi::Date(2013, 8, 5), Fmi::Hours(0));
  REQUIRE(day_start(midnight) == midnight);
  REQUIRE(day_end(midnight) == Fmi::DateTime(Fmi::Date(2013, 8, 6), Fmi::Hours(0)));

  // Year change
  const Fmi::DateTime newyear(Fmi::Date(2013, 12, 31), Fmi::Hours(23));
  REQUIRE(day_end(newyear) == Fmi::DateTime(Fmi::Date(2014, 1, 1), Fmi::Hours(0)));

  // Special values pass through
  REQUIRE(day_start(Fmi::DateTime()).is_not_a_date_time());
  REQUIRE(day_end(Fmi::DateTime()).is_not_a_date_time());
}

TEST_CASE("Station utilities")
{
  SECTION("removeDuplicateStations keeps the first occurrence")
  {
    SmartMet::Spine::Stations stations(4);
    stations[0].fmisid = 101004;
    stations[0].distance = "1";
    stations[1].fmisid = 100971;
    stations[2].fmisid = 101004;
    stations[2].distance = "2";
    stations[3].fmisid = 100968;

    auto result = removeDuplicateStations(stations);
    REQUIRE(result.size() == 3);
    REQUIRE(result[0].fmisid == 101004);
    REQUIRE(result[0].distance == "1");
    REQUIRE(result[1].fmisid == 100971);
    REQUIRE(result[2].fmisid == 100968);
  }

  SECTION("calculateStationDirection gives the bearing from the requested point")
  {
    SmartMet::Spine::Station station;
    station.requestedLon = 25;
    station.requestedLat = 60;

    station.longitude = 25;
    station.latitude = 61;
    calculateStationDirection(station);
    REQUIRE(station.stationDirection == Approx(0.0));

    station.longitude = 25;
    station.latitude = 59;
    calculateStationDirection(station);
    REQUIRE(station.stationDirection == Approx(180.0));

    station.longitude = 26;
    station.latitude = 60;
    calculateStationDirection(station);
    REQUIRE(station.stationDirection > 89);
    REQUIRE(station.stationDirection < 90);

    station.longitude = 24;
    station.latitude = 60;
    calculateStationDirection(station);
    REQUIRE(station.stationDirection > 270);
    REQUIRE(station.stationDirection < 271);

    // timeseries test open-several-stations: Kumpula -> Kaisaniemi is 196.2 degrees
    station.requestedLon = 24.96131;
    station.requestedLat = 60.20307;
    station.longitude = 24.94459;
    station.latitude = 60.17523;
    calculateStationDirection(station);
    REQUIRE(station.stationDirection == Approx(197.2).margin(1.0));
  }
}

TEST_CASE("FMISID values")
{
  REQUIRE(getStringValue(SmartMet::TimeSeries::Value(101004)) == "101004");
  REQUIRE(getStringValue(SmartMet::TimeSeries::Value(std::string("101004"))) == "101004");
  REQUIRE(getStringValue(SmartMet::TimeSeries::Value(101004.0)) == "101004");
  REQUIRE_THROWS(getStringValue(SmartMet::TimeSeries::Value(SmartMet::TimeSeries::None())));
}

TEST_CASE("Smartsymbol from wawa, cloudiness and temperature")
{
  Fmi::TimeZones timezones;
  auto tz = timezones.time_zone_from_string("Europe/Helsinki");
  const Fmi::LocalDateTime noon(Fmi::Date(2013, 8, 5), Fmi::Hours(12), tz);
  const Fmi::LocalDateTime midnight(Fmi::Date(2013, 12, 5), Fmi::Hours(0), tz);
  const double lat = 60.2;
  const double lon = 24.96;

  SECTION("No precipitation follows the cloudiness")
  {
    REQUIRE(calcSmartsymbolNumber(0, 0, 15, noon, lat, lon) == 1);
    REQUIRE(calcSmartsymbolNumber(0, 1, 15, noon, lat, lon) == 2);
    REQUIRE(calcSmartsymbolNumber(0, 4, 15, noon, lat, lon) == 4);
    REQUIRE(calcSmartsymbolNumber(0, 7, 15, noon, lat, lon) == 6);
    REQUIRE(calcSmartsymbolNumber(0, 8, 15, noon, lat, lon) == 7);
    // Fog group has a different overcast symbol
    REQUIRE(calcSmartsymbolNumber(30, 8, 15, noon, lat, lon) == 9);
  }

  SECTION("Precipitation depends on the temperature and cloudiness")
  {
    // Rain or snow depending on the temperature
    REQUIRE(calcSmartsymbolNumber(41, 4, 5, noon, lat, lon) == 31);
    REQUIRE(calcSmartsymbolNumber(41, 4, -5, noon, lat, lon) == 51);
    // Cloudiness levels of the three level scale
    REQUIRE(calcSmartsymbolNumber(61, 5, 5, noon, lat, lon) == 31);
    REQUIRE(calcSmartsymbolNumber(61, 7, 5, noon, lat, lon) == 34);
    REQUIRE(calcSmartsymbolNumber(61, 9, 5, noon, lat, lon) == 37);
    // Fixed symbols
    REQUIRE(calcSmartsymbolNumber(51, 8, 5, noon, lat, lon) == 11);
    REQUIRE(calcSmartsymbolNumber(77, 8, -5, noon, lat, lon) == 57);
  }

  SECTION("Night symbols are offset by 100")
  {
    REQUIRE(calcSmartsymbolNumber(0, 0, -5, midnight, lat, lon) == 101);
    REQUIRE(calcSmartsymbolNumber(61, 9, 5, midnight, lat, lon) == 137);
  }

  SECTION("Unknown codes and invalid cloudiness give no symbol")
  {
    REQUIRE_FALSE(calcSmartsymbolNumber(99, 4, 5, noon, lat, lon).has_value());
    REQUIRE_FALSE(calcSmartsymbolNumber(0, 10, 5, noon, lat, lon).has_value());
    REQUIRE_FALSE(calcSmartsymbolNumber(51, 10, 5, noon, lat, lon).has_value());
  }
}

TEST_CASE("Road and foreign parameter ids")
{
  RoadAndForeignIds ids;

  SECTION("Names map to numbers per producer")
  {
    REQUIRE(ids.stringToInteger("TA", "foreign") == 1);
    REQUIRE(ids.stringToInteger("ILMA", "road") == 1);
    REQUIRE(ids.stringToInteger("KELI", "road") == 86);
    REQUIRE(ids.stringToInteger("TA", "road") == 9999);
    REQUIRE(ids.stringToInteger("NOSUCH", "foreign") == 9999);
    REQUIRE_THROWS(ids.stringToInteger("TA", "nosuchproducer"));
  }

  SECTION("Without a producer foreign names are searched first")
  {
    REQUIRE(ids.stringToInteger("TA") == 1);
    REQUIRE(ids.stringToInteger("KELI") == 86);
    REQUIRE(ids.stringToInteger("NOSUCH") == 9999);
  }

  SECTION("Numbers map back to names")
  {
    REQUIRE(ids.integerToString(1, "foreign") == "TA");
    REQUIRE(ids.integerToString(1, "road") == "ILMA");
    REQUIRE(ids.integerToString(86, "road") == "KELI");
    REQUIRE(ids.integerToString(123456, "road") == "MISSING");
    REQUIRE_THROWS(ids.integerToString(1, "nosuchproducer"));
  }

  SECTION("Every road and foreign name round trips unless it is an alias")
  {
    for (const auto* name : {"TA", "RH", "PSEA", "WS", "WD", "VV", "WW", "SD"})
      REQUIRE(ids.integerToString(ids.stringToInteger(name, "foreign"), "foreign") == name);
    for (const auto* name : {"ILMA", "TIE", "MAAL", "KELI", "VARO", "SADE", "TSUUNT", "VIS"})
      REQUIRE(ids.integerToString(ids.stringToInteger(name, "road"), "road") == name);
  }
}
