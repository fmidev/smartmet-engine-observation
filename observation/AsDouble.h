#pragma once

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

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
