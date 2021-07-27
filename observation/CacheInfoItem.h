#pragma once

#include <map>
#include <set>
#include <string>
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
struct CacheInfoItem
{
  CacheInfoItem() = default;
  CacheInfoItem(const CacheInfoItem& cii) = default;

  CacheInfoItem(const std::string& n, bool a, std::set<std::string> t)
      : name(n), active(a), tables(t)
  {
  }

  void mergeCacheInfo(const CacheInfoItem& cii_from);

  std::string name;
  bool active = false;
  std::set<std::string> tables;
  std::map<std::string, std::string> params;
  std::map<std::string, std::vector<std::string>> params_vector;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
