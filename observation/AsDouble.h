#pragma once

#include <macgyver/StringConversion.h>
#include <pqxx/pqxx>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
// PQXX version 5 uses stringstreams which take locks on the global locale with gcc implementation
inline double as_double(const pqxx::field& obj)
{
#if PQXX_VERSION_MAJOR > 5
  return obj.as<double>();
#else
  return Fmi::stod(obj.as<std::string>());
#endif
}

inline int as_int(const pqxx::field& obj)
{
#if PQXX_VERSION_MAJOR > 5
  return obj.as<int>();
#else
  return Fmi::stoi(obj.as<std::string>());
#endif
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
