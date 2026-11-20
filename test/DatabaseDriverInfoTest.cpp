#define CATCH_CONFIG_MAIN

#if __cplusplus >= 201402L
#include <catch2/catch.hpp>
#else
#include <catch/catch.hpp>
#endif

#include "DatabaseDriverInfo.h"
#include <spine/ConfigBase.h>
#include <libconfig.h++>
#include <memory>
#include <string>

using namespace SmartMet::Engine::Observation;
using namespace SmartMet;

namespace
{
// A SpatiaLite driver whose cache is updated, as in a standalone server without PostgreSQL
const char* config_text = R"(
database_info:
{
  connect_info:
  {
    spatialite_test_cache: { spatialiteFile = "/dev/null"; };
  };
  common_info:
  {
    spatialite_test_fmi_observations:
    {
      disableAllCacheUpdates = false;
      flashCacheUpdateInterval = 30;
      finCacheUpdateInterval = 60;
      flashCacheDuration = 8800;
      flashMemoryCacheDuration = 72;
      finCacheDuration = 170;
      finMemoryCacheDuration = 25;
      extCacheDuration = 74;
      updateExtraInterval = 75;
    };
    spatialite_test_cache:
    {
      synchronous = "NORMAL";
      journal_mode = "WAL";
      auto_vacuum = "NONE";
      temp_store = "MEMORY";
      timeout = 30000;
      threads = 0;
      cache_size = 50000L;
      shared_cache = false;
      read_uncommitted = true;
      wal_autocheckpoint = 1000;
      mmap_size = 0L;
      poolSize = 5;
      maxInsertSize = 100000;
      dataInsertCacheSize = 1000;
      weatherDataQCInsertCacheSize = 1000;
      flashInsertCacheSize = 1000;
    };
  };
};

database_driver_info:
(
  {
    name = "spatialite_test_fmi_observations";
    tables = ["observation_data", "flash_data"];
    caches = ["spatialite_test_cache:observation_data,flash_data"];
    active = true;
  }
);
)";

DatabaseDriverInfo read_config()
{
  auto lc = std::make_shared<libconfig::Config>();
  lc->readString(config_text);
  Spine::ConfigBase cfg(lc, "DatabaseDriverInfoTest");
  DatabaseDriverInfo info;
  info.readConfig(cfg);
  return info;
}
}  // namespace

TEST_CASE("SpatiaLite driver reads its cache durations and update intervals")
{
  const auto info = read_config();
  const auto& driver = info.getDatabaseDriverInfo("spatialite_test_fmi_observations");

  // Before the fix these were missing, so a driver with updates enabled used zero durations:
  // the initial clean emptied the cache and the memory caches were never created.
  CHECK(driver.getIntParameterValue("disableAllCacheUpdates", 1) == 0);
  CHECK(driver.getIntParameterValue("flashCacheDuration", 0) == 8800);
  CHECK(driver.getIntParameterValue("flashMemoryCacheDuration", 0) == 72);
  CHECK(driver.getIntParameterValue("finCacheDuration", 0) == 170);
  CHECK(driver.getIntParameterValue("finMemoryCacheDuration", 0) == 25);
  CHECK(driver.getIntParameterValue("extCacheDuration", 0) == 74);
  CHECK(driver.getIntParameterValue("flashCacheUpdateInterval", 0) == 30);
  CHECK(driver.getIntParameterValue("finCacheUpdateInterval", 0) == 60);
  CHECK(driver.getIntParameterValue("updateExtraInterval", 0) == 75);

  // Settings which are not given default to zero, as for the other drivers
  CHECK(driver.getIntParameterValue("extMemoryCacheDuration", -1) == 0);
  CHECK(driver.getIntParameterValue("extCacheUpdateInterval", -1) == 0);
}
