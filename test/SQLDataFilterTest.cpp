#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "SQLDataFilter.h"

using SmartMet::Engine::Observation::SQLDataFilter;

TEST_CASE("Test SQL filter")
{
  SECTION("SQL clauses")
  {
    SECTION("empty")
    {
      SQLDataFilter filter;
      REQUIRE(filter.getSqlClause("name", "x").empty());
    }
    SECTION("number")
    {
      SQLDataFilter filter;
      filter.setDataFilter("name", "123");
      REQUIRE(filter.getSqlClause("name", "x") == "(x = 123)");
    }
    SECTION("eq")
    {
      SQLDataFilter filter;
      filter.setDataFilter("name", "eq 123");
      REQUIRE(filter.getSqlClause("name", "x") == "(x = 123)");
    }
    SECTION("lt")
    {
      SQLDataFilter filter;
      filter.setDataFilter("name", "lt 123");
      REQUIRE(filter.getSqlClause("name", "x") == "(x < 123)");
    }
    SECTION("le")
    {
      SQLDataFilter filter;
      filter.setDataFilter("name", "le 123");
      REQUIRE(filter.getSqlClause("name", "x") == "(x <= 123)");
    }
    SECTION("gt")
    {
      SQLDataFilter filter;
      filter.setDataFilter("name", "gt 123");
      REQUIRE(filter.getSqlClause("name", "x") == "(x > 123)");
    }
    SECTION("ge")
    {
      SQLDataFilter filter;
      filter.setDataFilter("name", "ge 123");
      REQUIRE(filter.getSqlClause("name", "x") == "(x >= 123)");
    }
    SECTION("and")
    {
      SQLDataFilter filter;
      filter.setDataFilter("name", "ge 1 AND lt 9");
      REQUIRE(filter.getSqlClause("name", "x") == "(x >= 1 AND x < 9)");
    }
    SECTION("or")
    {
      SQLDataFilter filter;
      filter.setDataFilter("name", "lt 5 OR ge 10");
      REQUIRE(filter.getSqlClause("name", "x") == "(x < 5 OR x >= 10)");
    }
  }
  SECTION("valueOK")
  {
    SECTION("empty")
    {
      SQLDataFilter filter;
      REQUIRE(filter.valueOK("name", 1));
      REQUIRE(filter.valueOK("name", 2));
      REQUIRE(filter.valueOK("name", 3));
    }
    SECTION("number")
    {
      SQLDataFilter filter;
      filter.setDataFilter("name", "123");
      REQUIRE(!filter.valueOK("name", 122));
      REQUIRE(filter.valueOK("name", 123));
      REQUIRE(!filter.valueOK("name", 124));
    }
    SECTION("eq")
    {
      SQLDataFilter filter;
      filter.setDataFilter("name", "eq 123");
      REQUIRE(!filter.valueOK("name", 122));
      REQUIRE(filter.valueOK("name", 123));
      REQUIRE(!filter.valueOK("name", 124));
    }
    SECTION("lt")
    {
      SQLDataFilter filter;
      filter.setDataFilter("name", "lt 123");
      REQUIRE(filter.valueOK("name", 122));
      REQUIRE(!filter.valueOK("name", 123));
      REQUIRE(!filter.valueOK("name", 124));
    }
    SECTION("le")
    {
      SQLDataFilter filter;
      filter.setDataFilter("name", "le 123");
      REQUIRE(filter.valueOK("name", 122));
      REQUIRE(filter.valueOK("name", 123));
      REQUIRE(!filter.valueOK("name", 124));
    }
    SECTION("gt")
    {
      SQLDataFilter filter;
      filter.setDataFilter("name", "gt 123");
      REQUIRE(!filter.valueOK("name", 122));
      REQUIRE(!filter.valueOK("name", 123));
      REQUIRE(filter.valueOK("name", 124));
    }
    SECTION("ge")
    {
      SQLDataFilter filter;
      filter.setDataFilter("name", "ge 123");
      REQUIRE(!filter.valueOK("name", 122));
      REQUIRE(filter.valueOK("name", 123));
      REQUIRE(filter.valueOK("name", 124));
    }
    SECTION("and")
    {
      SQLDataFilter filter;
      filter.setDataFilter("name", "ge 1 AND lt 9");
      REQUIRE(!filter.valueOK("name", 0));
      REQUIRE(filter.valueOK("name", 1));
      REQUIRE(filter.valueOK("name", 2));
      REQUIRE(filter.valueOK("name", 8));
      REQUIRE(!filter.valueOK("name", 9));
      REQUIRE(!filter.valueOK("name", 10));
    }
    SECTION("or")
    {
      SQLDataFilter filter;
      filter.setDataFilter("name", "lt 5 OR ge 10");
      REQUIRE(filter.valueOK("name", 4));
      REQUIRE(filter.valueOK("name", 10));
      REQUIRE(filter.valueOK("name", 11));
      REQUIRE(!filter.valueOK("name", 5));
      REQUIRE(!filter.valueOK("name", 6));
      REQUIRE(!filter.valueOK("name", 9));
    }
  }
}
