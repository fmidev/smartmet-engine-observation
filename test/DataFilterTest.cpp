#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "DataFilter.h"

using SmartMet::Engine::Observation::DataFilter;

TEST_CASE("Test Cache filter")
{
  SECTION("Cache")
  {
    SECTION("empty")
    {
      DataFilter filter;
      REQUIRE(filter.getSqlClause("name", "x").empty());
    }
    SECTION("number")
    {
      DataFilter filter;
      filter.setDataFilter("name", "123");
      REQUIRE(filter.getSqlClause("name", "x") == "(x = 123)");
    }
    SECTION("eq")
    {
      DataFilter filter;
      filter.setDataFilter("name", "eq 123");
      REQUIRE(filter.getSqlClause("name", "x") == "(x = 123)");
    }
    SECTION("lt")
    {
      DataFilter filter;
      filter.setDataFilter("name", "lt 123");
      REQUIRE(filter.getSqlClause("name", "x") == "(x < 123)");
    }
    SECTION("le")
    {
      DataFilter filter;
      filter.setDataFilter("name", "le 123");
      REQUIRE(filter.getSqlClause("name", "x") == "(x <= 123)");
    }
    SECTION("gt")
    {
      DataFilter filter;
      filter.setDataFilter("name", "gt 123");
      REQUIRE(filter.getSqlClause("name", "x") == "(x > 123)");
    }
    SECTION("ge")
    {
      DataFilter filter;
      filter.setDataFilter("name", "ge 123");
      REQUIRE(filter.getSqlClause("name", "x") == "(x >= 123)");
    }
    SECTION("and")
    {
      DataFilter filter;
      filter.setDataFilter("name", "ge 1 AND lt 9");
      REQUIRE(filter.getSqlClause("name", "x") == "(x < 9 AND x >= 1)");
    }
    SECTION("or")
    {
      DataFilter filter;
      filter.setDataFilter("name", "lt 5 OR ge 10");
      REQUIRE(filter.getSqlClause("name", "x") == "(x < 5 OR x >= 10)");
    }
  }
  SECTION("valueOK")
  {
    SECTION("empty")
    {
      DataFilter filter;
      REQUIRE(filter.valueOK("empty", 1));
      REQUIRE(filter.valueOK("empty", 2));
      REQUIRE(filter.valueOK("empty", 3));
    }
    SECTION("123")
    {
      DataFilter filter;
      filter.setDataFilter("123", "123");
      REQUIRE(!filter.valueOK("123", 122));
      REQUIRE(filter.valueOK("123", 123));
      REQUIRE(!filter.valueOK("123", 124));
    }
    SECTION("123,124")
    {
      DataFilter filter;
      filter.setDataFilter("123,124", "123,124");
      REQUIRE(!filter.valueOK("123,124", 122));
      REQUIRE(filter.valueOK("123,124", 123));
      REQUIRE(filter.valueOK("123,124", 124));
    }
    SECTION("eq 123")
    {
      DataFilter filter;
      filter.setDataFilter("eq 123", "eq 123");
      REQUIRE(!filter.valueOK("eq 123", 122));
      REQUIRE(filter.valueOK("eq 123", 123));
      REQUIRE(!filter.valueOK("eq 123", 124));
    }
    SECTION("lt 123")
    {
      DataFilter filter;
      filter.setDataFilter("lt 123", "lt 123");
      REQUIRE(filter.valueOK("lt 123", 122));
      REQUIRE(!filter.valueOK("lt 123", 123));
      REQUIRE(!filter.valueOK("lt 123", 124));
    }
    SECTION("le 123")
    {
      DataFilter filter;
      filter.setDataFilter("le 123", "le 123");
      REQUIRE(filter.valueOK("le 123", 122));
      REQUIRE(filter.valueOK("le 123", 123));
      REQUIRE(!filter.valueOK("le 123", 124));
    }
    SECTION("gt 123")
    {
      DataFilter filter;
      filter.setDataFilter("gt 123", "gt 123");
      REQUIRE(!filter.valueOK("gt 123", 122));
      REQUIRE(!filter.valueOK("gt 123", 123));
      REQUIRE(filter.valueOK("gt 123", 124));
    }
    SECTION("ge 123")
    {
      DataFilter filter;
      filter.setDataFilter("ge 123", "ge 123");
      REQUIRE(!filter.valueOK("ge 123", 122));
      REQUIRE(filter.valueOK("ge 123", 123));
      REQUIRE(filter.valueOK("ge 123", 124));
    }
    SECTION("ge 1 AND lt 9")
    {
      DataFilter filter;
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
      DataFilter filter;
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
      DataFilter filter;
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
