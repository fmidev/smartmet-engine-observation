#include "SQLDataFilter.h"
#include <boost/algorithm/string.hpp>
#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>
#include <set>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
namespace
{
int int_value(const std::string& str, size_t pos)
{
  std::string val = str.substr(pos);
  Fmi::trim(val);
  return Fmi::stoi(val);
}
}  // namespace

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
        boost::replace_all(cond, "eq", dbfield + " =");
      }

      ret += cond;
    }
    ret += ")";
    boost::replace_all(ret, "()", "");

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
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

bool SQLDataFilter::valueOK(const std::string& name, int val) const
{
  try
  {
    if (itsDataFilter.find(name) == itsDataFilter.end())
      return true;

    const auto& conditions = itsDataFilter.at(name);

    if (conditions.empty())
      return true;

    std::vector<bool> result_vector;
    for (const auto& condition : conditions)
    {
      std::string cond = Fmi::trim_copy(condition);

      if (cond.find_first_not_of("0123456789") == std::string::npos)
      {
        // Plain number
        result_vector.push_back(val == Fmi::stoi(cond));
      }
      else
      {
        bool orCondition = (cond.find("OR") != std::string::npos);
        bool andCondition = (cond.find("AND") != std::string::npos);

        std::vector<std::string> conditions;
        if (orCondition)
        {
          conditions.push_back(cond.substr(0, cond.find("OR") - 1));
          conditions.push_back(cond.substr(cond.find("OR") + 2));
        }
        else if (andCondition)
        {
          conditions.push_back(cond.substr(0, cond.find("AND") - 1));
          conditions.push_back(cond.substr(cond.find("AND") + 3));
        }
        else
          conditions.push_back(cond);

        std::set<bool> cond_set;
        for (const auto& cnd : conditions)
        {
          bool res = true;
          if (cnd.find("lt") != std::string::npos)
          {
            res = (val < int_value(cnd, cnd.find("lt") + 2));
          }
          else if (cnd.find("gt") != std::string::npos)
          {
            res = (val > int_value(cnd, cnd.find("gt") + 2));
          }
          else if (cnd.find("le") != std::string::npos)
          {
            res = (val <= int_value(cnd, cnd.find("le") + 2));
          }
          else if (cnd.find("ge") != std::string::npos)
          {
            res = (val >= int_value(cnd, cnd.find("ge") + 2));
          }
          else if (cnd.find("eq") != std::string::npos)
          {
            res = (val == int_value(cnd, cnd.find("eq") + 2));
          }
          cond_set.insert(res);
        }

        if (andCondition)
        {
          // If no falses found then OK
          result_vector.push_back((cond_set.find(false) == cond_set.end()));
        }
        else
        {
          // orCondition or only single condition
          // If even one true found then OK
          result_vector.push_back((cond_set.find(true) != cond_set.end()));
        }
      }
    }

    for (auto i : result_vector)
      if (i == true)
      {
        return true;
      }

    return false;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Testing values in SQLDataFilter failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
