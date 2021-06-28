#pragma once

#include <memory>
#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class CacheDataFilter
{
 public:
  ~CacheDataFilter();
  CacheDataFilter();

  // For example name = "data_quality", value = "le 5"
  void setDataFilter(const std::string& name, const std::string& value);

  bool exist(const std::string& name) const;
  bool empty() const;
  bool valueOK(const std::string& name, int val) const;

  void print() const;

 private:
  class Impl;
  std::unique_ptr<Impl> impl;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
