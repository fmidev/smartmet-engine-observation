#pragma once

#include <macgyver/StringConversion.h>
#include <pqxx/pqxx>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
// PQXX version 5 uses stringstreams which take locks on the global locale with gcc implementation.
//
// The field type is templated so the same helpers accept both pqxx::field (libpqxx <= 7) and the
// pqxx::field_ref proxy returned when iterating rows in libpqxx 8.
template <typename FieldType>
inline double as_double(const FieldType& obj)
{
#if PQXX_VERSION_MAJOR > 5
  return obj.template as<double>();
#else
  return Fmi::stod(obj.template as<std::string>());
#endif
}

template <typename FieldType>
inline int as_int(const FieldType& obj)
{
#if PQXX_VERSION_MAJOR > 5
  return obj.template as<int>();
#else
  return Fmi::stoi(obj.template as<std::string>());
#endif
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
