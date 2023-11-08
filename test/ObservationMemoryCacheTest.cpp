#define CATCH_CONFIG_MAIN
#include "DataItem.h"
#include "ObservationMemoryCache.h"
#include "QueryMapping.h"
#include "StationInfo.h"
#include <macgyver/DateTime.h>
#include <atomic>
#include <thread>

#if __cplusplus >= 201402L
#include <catch2/catch.hpp>
#else
#include <catch/catch.hpp>
#endif

std::string stationFile = "/usr/share/smartmet/test/data/sqlite/stations.txt";
SmartMet::Engine::Observation::StationInfo stationinfo(stationFile);

TEST_CASE("Test observation memory cache in parallel (TSAN)")
{
  SECTION("Insert and find in parallel")
  {
    SmartMet::Engine::Observation::ObservationMemoryCache cache;

    // First establish stations to be used
    double lon = 25;
    double lat = 65;
    int numberofstations = 1000;
    double maxdistance = 500 * 1000;  // meters
    std::set<std::string> groups;
    Fmi::DateTime starttime = boost::posix_time::time_from_string("2020-01-01 00:00:00");
    Fmi::DateTime endtime = boost::posix_time::time_from_string("2020-02-01 00:00:00");
    auto stations = stationinfo.findNearestStations(
        lon, lat, maxdistance, numberofstations, groups, starttime, endtime);

    int measurand_count = 10;
    std::atomic_bool read_finished{false};

    bool verbose = false;

    std::thread fill_thread(
        [&]()
        {
          if (verbose)
            std::cout << "Inserting data\n";
          for (const auto& station : stations)
          {
            if (read_finished)
              break;

            auto datatime = starttime;
            auto modifiedlast = datatime;

            SmartMet::Engine::Observation::DataItems items;
            while (datatime < endtime)
            {
              for (int measurand_id = 0; measurand_id < measurand_count; ++measurand_id)
              {
                float value = measurand_id;
                SmartMet::Engine::Observation::DataItem item;
                item.data_time = datatime;
                item.modified_last = modifiedlast;
                item.data_value = value;
                item.fmisid = station.fmisid;
                item.producer_id = 1;
                items.push_back(item);
              }
              datatime += Fmi::Hours(6);
            }
            cache.fill(items);
          }
          if (verbose)
            std::cout << "Cache filled\n";
        });

    sleep(1);

    std::thread read_thread(
        [&]()
        {
          if (verbose)
            std::cout << "Reading data\n";
          SmartMet::Engine::Observation::Settings settings;
          settings.starttime = starttime;
          settings.endtime = endtime;
          settings.starttimeGiven = true;
          settings.producer_ids.insert(1);

          SmartMet::Engine::Observation::QueryMapping qmap;
          for (int i = 0; i < measurand_count; i++)
          {
            qmap.sensorNumberToMeasurandIds[i] = std::set<int>{i};
            qmap.measurandIds.push_back(i);
          }

          auto obs = cache.read_observations(stations, settings, stationinfo, groups, qmap);

          read_finished = true;
          REQUIRE(obs.size() > 1000);
          if (verbose)
            std::cout << "Cache read finished\n";
        });

    fill_thread.join();
    read_thread.join();
  }
}
