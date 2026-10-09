#define CATCH_CONFIG_MAIN
#include "FlashMemoryCache.h"
#include <macgyver/DateTime.h>

#if __cplusplus >= 201402L
#include <catch2/catch.hpp>
#else
#include <catch/catch.hpp>
#endif

using SmartMet::Engine::Observation::FlashDataItem;
using SmartMet::Engine::Observation::FlashDataItems;
using SmartMet::Engine::Observation::FlashMemoryCache;

namespace
{
const Fmi::DateTime t0 = Fmi::DateTime::from_string("2026-09-01 12:00:00");

FlashDataItem stroke(int minutes, double lon, double lat, unsigned int id)
{
  FlashDataItem item;
  item.stroke_time = t0 + Fmi::Minutes(minutes);
  item.longitude = lon;
  item.latitude = lat;
  item.flash_id = id;
  return item;
}

// Bounding box A covers southern Finland, B is disjoint from it
struct Box
{
  double minlon, minlat, maxlon, maxlat;
};
const Box A{24, 60, 26, 61};
const Box B{10, 50, 12, 51};

std::optional<std::uint64_t> generation(const FlashMemoryCache& cache,
                                        int startminutes,
                                        int endminutes,
                                        const Box& box)
{
  return cache.latestGeneration(t0 + Fmi::Minutes(startminutes),
                                t0 + Fmi::Minutes(endminutes),
                                box.minlon,
                                box.minlat,
                                box.maxlon,
                                box.maxlat);
}
}  // namespace

TEST_CASE("Flash memory cache generations")
{
  SECTION("No answer before the cache start time is known")
  {
    FlashMemoryCache cache;
    REQUIRE(!generation(cache, 0, 10, A));
    cache.fill({stroke(1, 25, 60.5, 1)});
    REQUIRE(!generation(cache, 0, 10, A));  // filled but not cleaned yet
  }

  SECTION("Zero for a window and box which never received strokes")
  {
    FlashMemoryCache cache;
    cache.clean(t0);
    REQUIRE(generation(cache, 0, 10, A) == 0U);
    cache.fill({stroke(1, 25, 60.5, 1)});
    REQUIRE(generation(cache, 0, 10, B) == 0U);
    REQUIRE(generation(cache, 20, 30, A) == 0U);
  }

  SECTION("No answer for a window starting before the cache")
  {
    FlashMemoryCache cache;
    cache.clean(t0);
    cache.fill({stroke(1, 25, 60.5, 1)});
    REQUIRE(!generation(cache, -10, 10, A));
  }

  SECTION("Strokes in A change the generation of A only")
  {
    FlashMemoryCache cache;
    cache.clean(t0);
    cache.fill({stroke(1, 11, 50.5, 1)});
    auto a1 = generation(cache, 0, 10, A);
    auto b1 = generation(cache, 0, 10, B);
    REQUIRE(a1 == 0U);
    REQUIRE(b1 > 0U);

    cache.fill({stroke(2, 25, 60.5, 2), stroke(3, 25.5, 60.6, 3)});
    auto a2 = generation(cache, 0, 10, A);
    auto b2 = generation(cache, 0, 10, B);
    REQUIRE(a2 > a1);
    REQUIRE(b2 == b1);
  }

  SECTION("A late stroke changes only the windows it falls in")
  {
    FlashMemoryCache cache;
    cache.clean(t0);
    cache.fill({stroke(2, 25, 60.5, 1), stroke(12, 25, 60.5, 2)});
    auto early = generation(cache, 0, 5, A);
    auto late = generation(cache, 10, 15, A);

    // Stroke time older than the newest strokes in the cache
    cache.fill({stroke(3, 25.2, 60.4, 3)});
    REQUIRE(generation(cache, 0, 5, A) > early);
    REQUIRE(generation(cache, 10, 15, A) == late);
  }

  SECTION("A fill adding only duplicates changes nothing")
  {
    FlashMemoryCache cache;
    cache.clean(t0);
    cache.fill({stroke(2, 25, 60.5, 1)});
    auto g = generation(cache, 0, 10, A);
    REQUIRE(cache.fill({stroke(2, 25, 60.5, 1)}) == 0U);
    REQUIRE(generation(cache, 0, 10, A) == g);
  }

  SECTION("Bounding box edges and window ends are inclusive")
  {
    FlashMemoryCache cache;
    cache.clean(t0);
    cache.fill({stroke(10, 26, 61, 1)});  // exactly at the max corner of A and the window end
    REQUIRE(generation(cache, 0, 10, A) > 0U);
    REQUIRE(generation(cache, 10, 20, A) > 0U);
    REQUIRE(generation(cache, 0, 9, A) == 0U);
  }

  SECTION("clean() removes only generations entirely before the new start time")
  {
    FlashMemoryCache cache;
    cache.clean(t0);
    cache.fill({stroke(1, 25, 60.5, 1)});
    cache.fill({stroke(20, 25, 60.5, 2)});
    auto g = generation(cache, 15, 25, A);

    cache.clean(t0 + Fmi::Minutes(10));
    REQUIRE(!generation(cache, 0, 25, A));  // before the new start time
    REQUIRE(generation(cache, 10, 15, A) == 0U);
    REQUIRE(generation(cache, 15, 25, A) == g);
  }
}
