#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "CacheDataFilter.h"

using SmartMet::Engine::Observation::CacheDataFilter;

TEST_CASE("Test Cache filter")
{
  SECTION("valueOK")
  {
    SECTION("empty")
    {
      CacheDataFilter filter;
      REQUIRE(filter.valueOK("empty", 1));
      REQUIRE(filter.valueOK("empty", 2));
      REQUIRE(filter.valueOK("empty", 3));
    }
    SECTION("123")
    {
      CacheDataFilter filter;
      filter.setDataFilter("123", "123");
      REQUIRE(!filter.valueOK("123", 122));
      REQUIRE(filter.valueOK("123", 123));
      REQUIRE(!filter.valueOK("123", 124));
    }
    SECTION("123,124")
    {
      CacheDataFilter filter;
      filter.setDataFilter("123,124", "123,124");
      REQUIRE(!filter.valueOK("123,124", 122));
      REQUIRE(filter.valueOK("123,124", 123));
      REQUIRE(filter.valueOK("123,124", 124));
    }
    SECTION("eq 123")
    {
      CacheDataFilter filter;
      filter.setDataFilter("eq 123", "eq 123");
      REQUIRE(!filter.valueOK("eq 123", 122));
      REQUIRE(filter.valueOK("eq 123", 123));
      REQUIRE(!filter.valueOK("eq 123", 124));
    }
    SECTION("lt 123")
    {
      CacheDataFilter filter;
      filter.setDataFilter("lt 123", "lt 123");
      REQUIRE(filter.valueOK("lt 123", 122));
      REQUIRE(!filter.valueOK("lt 123", 123));
      REQUIRE(!filter.valueOK("lt 123", 124));
    }
    SECTION("le 123")
    {
      CacheDataFilter filter;
      filter.setDataFilter("le 123", "le 123");
      REQUIRE(filter.valueOK("le 123", 122));
      REQUIRE(filter.valueOK("le 123", 123));
      REQUIRE(!filter.valueOK("le 123", 124));
    }
    SECTION("gt 123")
    {
      CacheDataFilter filter;
      filter.setDataFilter("gt 123", "gt 123");
      REQUIRE(!filter.valueOK("gt 123", 122));
      REQUIRE(!filter.valueOK("gt 123", 123));
      REQUIRE(filter.valueOK("gt 123", 124));
    }
    SECTION("ge 123")
    {
      CacheDataFilter filter;
      filter.setDataFilter("ge 123", "ge 123");
      REQUIRE(!filter.valueOK("ge 123", 122));
      REQUIRE(filter.valueOK("ge 123", 123));
      REQUIRE(filter.valueOK("ge 123", 124));
    }
    SECTION("ge 1 AND lt 9")
    {
      CacheDataFilter filter;
      filter.setDataFilter("ge 1 AND lt 9", "ge 1 AND lt 9");
      REQUIRE(!filter.valueOK("ge 1 AND lt 9", 0));
      REQUIRE(filter.valueOK("ge 1 AND lt 9", 1));
      REQUIRE(filter.valueOK("ge 1 AND lt 9", 2));
      REQUIRE(filter.valueOK("ge 1 AND lt 9", 8));
      REQUIRE(!filter.valueOK("ge 1 AND lt 9", 9));
      REQUIRE(!filter.valueOK("ge 1 AND lt 9", 10));
    }
    SECTION("lt 5 OR ge 10")
    {
      CacheDataFilter filter;
      filter.setDataFilter("lt 5 OR ge 10", "lt 5 OR ge 10");
      REQUIRE(filter.valueOK("lt 5 OR ge 10", 4));
      REQUIRE(filter.valueOK("lt 5 OR ge 10", 10));
      REQUIRE(filter.valueOK("lt 5 OR ge 10", 11));
      REQUIRE(!filter.valueOK("lt 5 OR ge 10", 5));
      REQUIRE(!filter.valueOK("lt 5 OR ge 10", 6));
      REQUIRE(!filter.valueOK("lt 5 OR ge 10", 9));
    }
    SECTION("1,3,ge 5 AND lt 9,11")
    {
      CacheDataFilter filter;
      filter.setDataFilter("1,3,ge 5 AND lt 9,11", "1,3,ge 5 AND lt 9,11");
      REQUIRE(!filter.valueOK("1,3,ge 5 AND lt 9,11", 0));
      REQUIRE(filter.valueOK("1,3,ge 5 AND lt 9,11", 1));
      REQUIRE(!filter.valueOK("1,3,ge 5 AND lt 9,11", 2));
      REQUIRE(filter.valueOK("1,3,ge 5 AND lt 9,11", 3));
      REQUIRE(!filter.valueOK("1,3,ge 5 AND lt 9,11", 4));
      REQUIRE(filter.valueOK("1,3,ge 5 AND lt 9,11", 5));
      REQUIRE(filter.valueOK("1,3,ge 5 AND lt 9,11", 6));
      REQUIRE(filter.valueOK("1,3,ge 5 AND lt 9,11", 7));
      REQUIRE(filter.valueOK("1,3,ge 5 AND lt 9,11", 8));
      REQUIRE(!filter.valueOK("1,3,ge 5 AND lt 9,11", 9));
      REQUIRE(!filter.valueOK("1,3,ge 5 AND lt 9,11", 10));
      REQUIRE(filter.valueOK("1,3,ge 5 AND lt 9,11", 11));
      REQUIRE(!filter.valueOK("1,3,ge 5 AND lt 9,11", 12));
      REQUIRE(!filter.valueOK("1,3,ge 5 AND lt 9,11", 13));
    }
  }
}
