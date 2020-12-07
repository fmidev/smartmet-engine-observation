#include "QueryObservableProperty.h"
#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
QueryObservableProperty::~QueryObservableProperty() = default;

void QueryObservableProperty::solveMeasurandIds(
    const QueryObservableProperty::ParameterVectorType &parameters,
    const ParameterMapPtr &parameterMap,
    const QueryObservableProperty::StationTypeType &stationType,
    QueryObservableProperty::ParameterIdMapType &parameterIDs) const
{
  try
  {
    // Empty list means we want all parameters
    const bool findOnlyGiven = (not parameters.empty());

    for (auto params = parameterMap->begin(); params != parameterMap->end(); ++params)
    {
      if (findOnlyGiven &&
          find(parameters.begin(), parameters.end(), params->first) == parameters.end())
        continue;

      auto gid = params->second.find(stationType);
      if (gid == params->second.end())
      {
        if (params->first == "pap_pt1s_avg")
          parameterIDs.emplace(650, params->first);
        continue;
      }

      try
      {
        int id = std::stoi(gid->second);
        parameterIDs.emplace(id, params->first);
      }
      catch (std::exception &)
      {
        // gid is either too large or not convertible (ie. something is wrong)
        continue;
      }
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
