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

  QueryObservableProperty(const QueryObservableProperty& other) = default;
  QueryObservableProperty(QueryObservableProperty&& other) = default;
  QueryObservableProperty& operator=(const QueryObservableProperty& other) = default;
  QueryObservableProperty& operator=(QueryObservableProperty&& other) = default;

 protected:
  using ParameterIdMapType = std::multimap<int, std::string>;
  using ParameterVectorType = std::vector<std::string>;
  using StationTypeType = std::string;

  static void solveMeasurandIds(const ParameterVectorType& parameters,
                                const ParameterMapPtr& parameterMap,
                                const StationTypeType& stationType,
                                ParameterIdMapType& parameterIds);
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
