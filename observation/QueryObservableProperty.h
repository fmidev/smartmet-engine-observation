#pragma once

#include "ObservableProperty.h"
#include "ParameterMap.h"
#include "QueryBase.h"
#include <map>
#include <string>
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class QueryObservableProperty : public QueryBase
{
 public:
  QueryObservableProperty() = default;

  ~QueryObservableProperty() override;

 protected:
  using ParameterIdMapType = std::multimap<int, std::string>;
  using ParameterVectorType = std::vector<std::string>;
  using StationTypeType = std::string;

  void solveMeasurandIds(const ParameterVectorType &parameters,
                         const ParameterMapPtr &parameterMap,
                         const StationTypeType &stationType,
                         ParameterIdMapType &parameterIds) const;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
