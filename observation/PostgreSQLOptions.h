#pragma once

#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
struct PostgreSQLOptions
{
  std::string host;
  std::string port;
  std::string database;
  std::string username;
  std::string password;
  std::string encoding;
  std::string connect_timeout;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
