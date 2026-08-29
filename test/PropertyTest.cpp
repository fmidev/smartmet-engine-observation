#define CATCH_CONFIG_MAIN

#if __cplusplus >= 201402L
#include <catch2/catch.hpp>
#else
#include <catch/catch.hpp>
#endif

#include "Property.h"
#include <any>
#include <string>
#include <vector>

using namespace SmartMet::Engine::Observation;

// Regression tests for H-11: WFS stored-query Oracle SQL injection. Request
// controlled string values are rendered as '...' into the SQL WHERE clause by
// Property::Base. Embedded single quotes must be doubled so they cannot break
// out of the string literal.

TEST_CASE("Plain string value is quoted unchanged")
{
  Property::IsEqualTo op;
  auto expr = op.get("name", std::any(std::string("Helsinki")))->getExpression("view");
  CHECK(expr == "view.name = 'Helsinki'");
}

TEST_CASE("Single quote in string value is doubled (escaped)")
{
  Property::IsEqualTo op;
  auto expr = op.get("name", std::any(std::string("O'Brien")))->getExpression("view");
  CHECK(expr == "view.name = 'O''Brien'");
}

TEST_CASE("SQL injection attempt is neutralized")
{
  Property::IsEqualTo op;
  const std::string attack = "x' OR '1'='1";
  auto expr = op.get("name", std::any(attack))->getExpression("view");
  // Every single quote in the payload must be doubled so it stays inside the
  // literal and cannot inject a boolean tautology.
  CHECK(expr == "view.name = 'x'' OR ''1''=''1'");
}

TEST_CASE("Vector of strings escapes each element")
{
  Property::IsOneOf op;
  std::vector<std::string> values{"a'b", "c"};
  auto expr = op.get("name", std::any(values))->getExpression("view");
  CHECK(expr == "view.name  IN  ('a''b', 'c')");
}

TEST_CASE("PostgreSQL rendering also escapes single quotes")
{
  Property::IsEqualTo op;
  auto expr = op.get("name", std::any(std::string("a'b")))->getExpression("view", "postgresql");
  CHECK(expr == "view.name = 'a''b'");
}
