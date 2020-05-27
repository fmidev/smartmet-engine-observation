#pragma once

#include <map>
#include <string>
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class SQLDataFilter
{
 public:
  using DataFilterType = std::map<std::string, std::vector<std::string>>;
  // For example name = "data_quality", value = "le 5"
  void setDataFilter(const std::string& name, const std::string& value);
  // Returns SQL clause for dbfield -> name is filter name, dbfield is table field name in database
  // For example: name = "data_quality", dbfield = "data.flag"
  std::string getSqlClause(const std::string& name, const std::string& dbfield) const;
  bool exist(const std::string& name) const;
  bool empty() const;
  void format(std::ostream& out) const;
  bool valueOK(const std::string& name, int val) const;

 private:
  DataFilterType itsDataFilter;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
