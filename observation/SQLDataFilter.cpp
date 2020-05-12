#include "SQLDataFilter.h"
#include <boost/algorithm/string.hpp>
#include <spine/Exception.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
void SQLDataFilter::setDataFilter(const std::string& name, const std::string& value)
{
  std::vector<std::string> parts;
  boost::algorithm::split(parts, value, boost::algorithm::is_any_of(","));
  itsDataFilter.insert(std::make_pair(name, parts));
}

std::string SQLDataFilter::getSqlClause(const std::string& name, const std::string& dbfield) const
{
  try
  {
    std::string ret;

    if (itsDataFilter.find(name) == itsDataFilter.end())
      return ret;

    const auto& conditions = itsDataFilter.at(name);  // OR conditions
    ret += "(";
    for (const auto& condition : conditions)
    {
      if (ret != "(")
        ret += " OR ";

      std::string cond = condition;
      if (cond.find_first_not_of("0123456789") == std::string::npos)
      {
        cond.insert(0, dbfield + " = ");
      }
      else
      {
        boost::replace_all(cond, "lt", dbfield + " <");
        boost::replace_all(cond, "gt", dbfield + " >");
        boost::replace_all(cond, "le", dbfield + " <=");
        boost::replace_all(cond, "ge", dbfield + " >=");
      }

      ret += cond;
    }
    ret += ")";
    boost::replace_all(ret, "()", "");

    return ret;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

void SQLDataFilter::format(std::ostream& out) const
{
  for (const auto& item : itsDataFilter)
  {
    out << item.first << " -> ";
    for (const auto& vector_item : item.second)
      out << vector_item;
    out << " " << std::endl;
  }
}

bool SQLDataFilter::exist(const std::string& name) const
{
  return (itsDataFilter.find(name) != itsDataFilter.end());
}

bool SQLDataFilter::empty() const
{
  return itsDataFilter.empty();
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
