#include "CacheDataFilter.h"
#include <boost/algorithm/string.hpp>
#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>
#include <list>
#include <map>
#include <set>
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
namespace
{
enum class ComparisonType
{
  LT,
  LE,
  EQ,
  GE,
  GT
};

enum class JoinType
{
  AND,
  OR
};

std::vector<const char*> comparison_str{"lt", "le", "eq", "ge", "gt"};

struct Comparison
{
  int value;
  ComparisonType cmp;
  JoinType join;
};

// We keep AND operations at the front
using Comparisons = std::list<Comparison>;

ComparisonType parse_comparison(const std::string& str)
{
  if (str == "lt")
    return ComparisonType::LT;
  if (str == "le")
    return ComparisonType::LE;
  if (str == "eq")
    return ComparisonType::EQ;
  if (str == "ge")
    return ComparisonType::GE;
  if (str == "gt")
    return ComparisonType::GT;
  throw Fmi::Exception(BCP, "Invalid data comparison operator '" + str + "'");
}

JoinType parse_join(const std::string& str)
{
  if (str == "OR")
    return JoinType::OR;
  if (str == "AND")
    return JoinType::AND;
  throw Fmi::Exception(BCP, "Invalid logical expression '" + str + "'");
}
}  // namespace

class CacheDataFilter::Impl
{
 public:
  void addDataFilter(const std::string& name, const std::string& filter_str)
  {
    try
    {
      std::vector<std::string> parts;
      boost::algorithm::split(parts, filter_str, boost::algorithm::is_any_of(" "));

      if (parts.size() == 1)
        filtermap[name].push_back(
            Comparison{Fmi::stoi(parts[0]), ComparisonType::EQ, JoinType::OR});
      else if (parts.size() == 2)
        filtermap[name].push_back(
            Comparison{Fmi::stoi(parts[1]), parse_comparison(parts[0]), JoinType::OR});
      else if (parts.size() == 5)
      {
        // "lt X OR gt X" etc
        auto cmp1 = parse_comparison(parts[0]);
        auto val1 = Fmi::stoi(parts[1]);
        auto join = parse_join(parts[2]);
        auto cmp2 = parse_comparison(parts[3]);
        auto val2 = Fmi::stoi(parts[4]);
        // keep AND conditions at the front so that the loop in valueOK works correctly
        if (join == JoinType::AND)
        {
          filtermap[name].push_front(Comparison{val1, cmp1, join});
          filtermap[name].push_front(Comparison{val2, cmp2, join});
        }
        else
        {
          filtermap[name].push_back(Comparison{val1, cmp1, join});
          filtermap[name].push_back(Comparison{val2, cmp2, join});
        }
      }
      else
        throw Fmi::Exception(BCP, "Incorrect number of elements in data comparison expression")
            .addParameter("size", Fmi::to_string(parts.size()));
    }
    catch (...)
    {
      throw Fmi::Exception::Trace(BCP, "Invalid data comparison expression '" + filter_str + "'");
    }
  }

  bool exist(const std::string& name) const { return filtermap.find(name) != filtermap.end(); }

  bool empty() const { return filtermap.empty(); }

  bool valueOK(const std::string& name, int val) const
  {
    const auto pos = filtermap.find(name);

    // Value is ok if there is no filter for the parameter
    if (pos == filtermap.end())
      return true;

    // Initial value for joins is true, if the first comparison is AND. Otherwise start with FALSE
    // for OR's
    bool result = (pos->second.front().join == JoinType::AND);

    for (const auto& comp : pos->second)
    {
      bool flag = true;

      if (comp.cmp == ComparisonType::LT)
        flag = (val < comp.value);
      else if (comp.cmp == ComparisonType::LE)
        flag = (val <= comp.value);
      else if (comp.cmp == ComparisonType::EQ)
        flag = (val == comp.value);
      else if (comp.cmp == ComparisonType::GE)
        flag = (val >= comp.value);
      else
        flag = (val > comp.value);

      if (comp.join == JoinType::AND)
        result &= flag;
      else
        result |= flag;
    }

    return result;
  }

  void print() const
  {
    for (const auto& name_filters : filtermap)
    {
      printf("NAME = %s\n", name_filters.first.c_str());
      for (const auto& filter : name_filters.second)
      {
        printf("\t%s %d (%d)\n",
               comparison_str[static_cast<int>(filter.cmp)],
               filter.value,
               filter.join);
      }
    }
  }

 private:
  using FilterMap = std::map<std::string, Comparisons>;
  FilterMap filtermap;
};

CacheDataFilter::~CacheDataFilter() = default;

CacheDataFilter::CacheDataFilter() : impl(new Impl()) {}

void CacheDataFilter::setDataFilter(const std::string& name, const std::string& value)
{
  std::vector<std::string> parts;
  boost::algorithm::split(parts, value, boost::algorithm::is_any_of(","));

  for (const auto& filter : parts)
    impl->addDataFilter(name, filter);
}

bool CacheDataFilter::exist(const std::string& name) const
{
  return impl->exist(name);
}

bool CacheDataFilter::empty() const
{
  return impl->empty();
}

bool CacheDataFilter::valueOK(const std::string& name, int val) const
{
  return impl->valueOK(name, val);
}

void CacheDataFilter::print() const
{
  impl->print();
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
