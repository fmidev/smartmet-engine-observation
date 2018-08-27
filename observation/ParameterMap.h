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
class ParameterMap
{
 public:
  ParameterMap() {}
  ParameterMap(const ParameterMap&) = delete;

  using StationParameters = std::map<std::string, std::string>;
  using NameToStationParameterMap = std::map<std::string, StationParameters>;

  std::string getParameter(const std::string& name, const std::string& stationtype) const
  {
    if (params.find(name) != params.end())
    {
      const StationParameters& stationparams = params.at(name);
      if (stationparams.find(stationtype) != stationparams.end())
        return stationparams.at(stationtype);
    }
    return std::string();
  }
  void addStationParameterMap(const std::string& name,
                              std::map<std::string, std::string> stationparams)
  {
    params.insert(make_pair(name, stationparams));
  }
  const StationParameters& at(const std::string& name) const
  {
    if (params.find(name) != params.end())
      return params.at(name);

    return emptymap;
  }
  NameToStationParameterMap::const_iterator find(const std::string& name) const
  {
    return params.find(name);
  }
  NameToStationParameterMap::const_iterator begin() const { return params.begin(); }
  NameToStationParameterMap::const_iterator end() const { return params.end(); }

 private:
  NameToStationParameterMap params;
  StationParameters emptymap;
};

using ParameterMapPtr = boost::shared_ptr<const ParameterMap>;

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
