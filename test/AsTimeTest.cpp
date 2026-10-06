#define CATCH_CONFIG_MAIN

#if __cplusplus >= 201402L
#include <catch2/catch.hpp>
#else
#include <catch/catch.hpp>
#endif

#include "AsDouble.h"
#include <macgyver/Exception.h>
#include <optional>
#include <string>

using namespace SmartMet::Engine::Observation;

namespace
{
// Mimics a pqxx field: the value as the server sent it in text form
struct FakeField
{
  std::optional<std::string> value;

  bool is_null() const { return !value; }

  template <typename T>
  T as() const
  {
    if (!value)
      throw std::runtime_error("null field");
    return pqxx::from_string<T>(*value);
  }
};
}  // namespace

TEST_CASE("as_time_t accepts EXTRACT(EPOCH) results from all PostgreSQL versions")
{
  SECTION("double precision from PostgreSQL 13 and older")
  {
    REQUIRE(as_time_t(FakeField{"1791298500"}) == 1791298500);
  }

  SECTION("numeric from PostgreSQL 14 and newer")
  {
    REQUIRE(as_time_t(FakeField{"1791298500.000000"}) == 1791298500);
  }

  SECTION("fractional seconds are truncated")
  {
    REQUIRE(as_time_t(FakeField{"1791298500.999999"}) == 1791298500);
    REQUIRE(as_time_t(FakeField{"-1.500000"}) == -2);
  }

  SECTION("times beyond 2038 do not overflow")
  {
    REQUIRE(as_time_t(FakeField{"4102444800.000000"}) == 4102444800);
  }

  SECTION("null and garbage are reported")
  {
    REQUIRE_THROWS_AS(as_time_t(FakeField{}), Fmi::Exception);
    REQUIRE_THROWS_AS(as_time_t(FakeField{"not a time"}), Fmi::Exception);
  }
}

TEST_CASE("the integer parser of libpqxx rejects numeric epochs")
{
  // Documents why as_time_t is needed instead of as<time_t>()
  REQUIRE_THROWS(FakeField{"1791298500.000000"}.as<time_t>());
}
