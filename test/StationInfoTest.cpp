#define CATCH_CONFIG_MAIN

#if __cplusplus >= 201402L
#include <catch2/catch.hpp>
#else
#include <catch/catch.hpp>
#endif

#include "StationInfo.h"
#include <macgyver/StringConversion.h>
#include <macgyver/TimeZones.h>

std::string stationFile = "/usr/share/smartmet/test/data/sqlite/stations.txt";

Fmi::DateTime starttime(Fmi::DateTime::from_string("2010-01-01 00:00:00"));
Fmi::DateTime endtime(Fmi::DateTime::from_string("2010-01-02 00:00:00"));

Fmi::TimeZones timezones;

SmartMet::Engine::Observation::StationInfo stationinfo(stationFile);

TEST_CASE("Test station and data searches")
{
  SECTION("Search Stations using AWS group")
  {
    double lat = 60.17522999999999;
    double lon = 24.94459;
    int maxdistance = 50000;

    std::set<std::string> stationgroup_codes{"AWS"};
    int station_id = 100971;

    SECTION("A station is searched by id")
    {
      auto station = stationinfo.getStation(station_id, stationgroup_codes, starttime);
      REQUIRE(station.fmisid == station_id);
      REQUIRE(station.formal_name_fi == "Helsinki Kaisaniemi");
    }

    SECTION("1 station is searched by coordinates")
    {
      int numberofstations = 1;

      auto stations = stationinfo.findNearestStations(
          lon, lat, maxdistance, numberofstations, stationgroup_codes, starttime, endtime);
      REQUIRE(stations.size() == 1);
      REQUIRE(stations.back().fmisid == station_id);
      REQUIRE(stations.back().formal_name_fi == "Helsinki Kaisaniemi");
      REQUIRE(Fmi::stod(stations.back().distance) < 0.1);
    }

    SECTION("5 stations are searched by coordinates")
    {
      int numberofstations = 5;

      auto stations = stationinfo.findNearestStations(
          lon, lat, maxdistance, numberofstations, stationgroup_codes, starttime, endtime);

      REQUIRE(stations.size() == 5);
      REQUIRE(stations[0].type == "AWS");
      REQUIRE(stations[1].type == "AWS");
      REQUIRE(stations[2].type == "AWS");
      REQUIRE(stations[3].type == "AWS");
      REQUIRE(stations[4].type == "AWS");
      REQUIRE(stations[0].fmisid == 100971);
      REQUIRE(stations[1].fmisid == 101007);
      REQUIRE(stations[2].fmisid == 101004);
      REQUIRE(stations[3].fmisid == 100996);
      REQUIRE(stations[4].fmisid == 101005);
      REQUIRE(stations[2].wsi == "0-20000-0-02998");
    }

    SECTION("All AWS stations are searched")
    {
      auto stations = stationinfo.findStationsInGroup(stationgroup_codes, starttime, endtime);
      REQUIRE(stations.size() == 169);
    }

    SECTION("Old station location")
    {
      auto stations = stationinfo.findNearestStations(
          25.0,
          60.3,
          5000,
          1,
          stationgroup_codes,
          Fmi::DateTime::from_string("2020-01-01 00:00:00"),
          Fmi::DateTime::from_string("2020-02-01 00:00:00"));
      REQUIRE(stations.size() == 1);
      REQUIRE(stations.back().formal_name_fi == "Vantaa Helsinki-Vantaan lentoasema");
      REQUIRE(to_iso_extended_string(stations.back().station_end) == "2020-09-24T00:00:00");
      REQUIRE(to_iso_extended_string(stations.back().station_start) == "2008-09-01T00:00:00");
      REQUIRE(stations.back().longitude == 24.95675);
    }

    SECTION("New station location")
    {
      auto stations = stationinfo.findNearestStations(
          25.0,
          60.3,
          5000,
          1,
          stationgroup_codes,
          Fmi::DateTime::from_string("2021-01-01 00:00:00"),
          Fmi::DateTime::from_string("2021-02-01 00:00:00"));
      REQUIRE(stations.size() == 1);
      REQUIRE(stations.back().formal_name_fi == "Vantaa Helsinki-Vantaan lentoasema");
      REQUIRE(to_iso_extended_string(stations.back().station_end) == "9999-12-31T00:00:00");
      REQUIRE(to_iso_extended_string(stations.back().station_start) == "2020-09-24T00:00:00");
      REQUIRE(stations.back().longitude == 24.97274);
    }
  }

  SECTION("Using EXTRWS group")
  {
    double lat = 60.0605900;
    double lon = 24.0758100;
    int maxdistance = 50000;

    std::set<std::string> stationgroup_codes{"EXTRWS"};
    int station_id = 100013;

    SECTION("A station is searched by id")
    {
      auto station = stationinfo.getStation(station_id, stationgroup_codes, starttime);
      REQUIRE(station.fmisid == station_id);
      REQUIRE(station.formal_name_fi == "kt51_Inkoo_Innanbäck");
    }
    SECTION("1 station is searched by coordinates")
    {
      int numberofstations = 1;

      auto stations = stationinfo.findNearestStations(
          lon, lat, maxdistance, numberofstations, stationgroup_codes, starttime, endtime);
      REQUIRE(stations.size() == 1);
      REQUIRE(stations.back().fmisid == station_id);
      REQUIRE(stations.back().formal_name_fi == "kt51_Inkoo_Innanbäck");
      REQUIRE(Fmi::stod(stations.back().distance) < 0.1);
    }

    SECTION("1 station is searched by coordinates, first one alphabetically is chosen")
    {
      int numberofstations = 1;
      auto stations = stationinfo.findNearestStations(
          25.6116, 60.9783, maxdistance, numberofstations, stationgroup_codes, starttime, endtime);
      REQUIRE(stations.size() == 1);
      REQUIRE(stations.back().fmisid == 100205);
      REQUIRE(stations.back().formal_name_fi == "Lahti_Kärpäsenmäki_Opt");
      REQUIRE(Fmi::stod(stations.back().distance) < 0.3);
    }

    SECTION("5 stations are searched by coordinates")
    {
      int numberofstations = 5;

      auto stations = stationinfo.findNearestStations(
          lon, lat, maxdistance, numberofstations, stationgroup_codes, starttime, endtime);

      REQUIRE(stations.size() == 5);
      REQUIRE(stations[0].fmisid == 100013);
      REQUIRE(stations[1].fmisid == 100016);
      REQUIRE(stations[2].fmisid == 100039);
      REQUIRE(stations[3].fmisid == 100065);
      REQUIRE(stations[4].fmisid == 100015);
    }

    SECTION("All EXTRWS stations are searched")
    {
      auto stations = stationinfo.findStationsInGroup(stationgroup_codes, starttime, endtime);
      REQUIRE(stations.size() == 1508);
    }

    SECTION("Using EXTSYNOP group")
    {
      double lat = 63.65139;
      double lon = 18.55028;
      int maxdistance = 50000;
      int station_id = 114226;
      int geoid = -16011960;

      std::set<std::string> stationgroup_codes{"EXTSYNOP"};

      SECTION("1 station is searched by coordinates")
      {
        int numberofstations = 1;

        auto stations = stationinfo.findNearestStations(
            lon, lat, maxdistance, numberofstations, stationgroup_codes, starttime, endtime);
        REQUIRE(stations.size() == 1);
        REQUIRE(stations.back().fmisid == station_id);
        REQUIRE(stations.back().formal_name_fi == "Hemling");
        REQUIRE(stations.back().geoid == geoid);
        REQUIRE(Fmi::stod(stations.back().distance) <= 0.1);
      }
    }
  }
}
