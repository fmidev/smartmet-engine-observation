#pragma once

#include <cmath>
#include <ctime>
#include <macgyver/StringConversion.h>
#include <pqxx/pqxx>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
template <typename FieldType>
inline double as_double(const FieldType& obj)
try
{
  return obj.template as<double>();
}
catch (...)
{
  auto err = Fmi::Exception::Trace(BCP, "Failed to convert field to double: ");
  if (obj.is_null())
    err.addDetail("Field is null");
  else
  {
    err.addDetail("Field value: '" + obj.template as<std::string>() + "'");
  }
  throw err;
}

template <typename FieldType>
inline int as_int(const FieldType& obj)
try
{
  return obj.template as<int>();
}
catch (...)
{
  auto err = Fmi::Exception::Trace(BCP, "Failed to convert field to int: ");
  if (obj.is_null())
    err.addDetail("Field is null");
  else
  {
    err.addDetail("Field value: '" + obj.template as<std::string>() + "'");
  }
  throw err;
}

// Read an epoch time in seconds, as produced by EXTRACT(EPOCH FROM ...). PostgreSQL 13 and older
// return double precision ('1791298500'), PostgreSQL 14 and newer return numeric
// ('1791298500.000000'), which the integer parsers of libpqxx reject. Fractional seconds are
// truncated towards the past.
template <typename FieldType>
inline std::time_t as_time_t(const FieldType& obj)
try
{
  return static_cast<std::time_t>(std::floor(obj.template as<double>()));
}
catch (...)
{
  auto err = Fmi::Exception::Trace(BCP, "Failed to convert field to epoch time: ");
  if (obj.is_null())
    err.addDetail("Field is null");
  else
  {
    err.addDetail("Field value: '" + obj.template as<std::string>() + "'");
  }
  throw err;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
